import json
import re
from datetime import datetime
from typing import (
    Any,
    Iterable,
    List,
    Optional,
    Type,
)

from pyspark.sql import types as spark_types
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.hdfs.utils import infer_partition, parse_config
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DomainsClass,
    MapTypeClass,
)
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column

HDFS_PREFIXES = ["hdfs://"]
_field_type_mapping = {
    NullType: NullTypeClass,
    StringType: StringTypeClass,
    BinaryType: BytesTypeClass,
    BooleanType: BooleanTypeClass,
    DateType: DateTypeClass,
    TimestampType: TimeTypeClass,
    DecimalType: NumberTypeClass,
    DoubleType: NumberTypeClass,
    FloatType: NumberTypeClass,
    ByteType: BytesTypeClass,
    IntegerType: NumberTypeClass,
    LongType: NumberTypeClass,
    ShortType: NumberTypeClass,
    ArrayType: NullTypeClass,
    MapType: MapTypeClass,
    StructField: RecordTypeClass,
    StructType: RecordTypeClass,
}

_spark_str_to_spark_type = {
    **spark_types._all_atomic_types,
    **spark_types._all_complex_types,
}


def is_hdfs_uri(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in HDFS_PREFIXES)


def parse_config(env):
    try:
        file = open(f"/spark-configuration/spark-defaults-{env}.conf")
        conf = {}

        while True:
            line = file.readline()
            if not line:
                break
            if line.startswith("spark"):
                key, value = [item for item in line.split(" ") if item != ""]
                conf[key] = value.replace("\n", "")
        return conf
    except Exception as e:
        raise ValueError(f"File path not found for env {env}, {e}")


def infer_partition(path: str) -> str:
    """Generify hdfs path by predefined formats

    Args:
        path (str): path to infer

    Returns:
        str: partition_by path
    """
    # Case when date string matches both %Y%m & %y%m%d
    # 220401 -> %Y%m -> Year: 2204, month: 01
    # 220401 -> %y%m%d -> Year: 2022, month: 04, day: 01
    # Case when date string matches one and wrong on the other
    # 202201 -> %Y%m -> Year: 2022, month: 01
    # 202201 -> %y%m%d -> Exception, there's no 22nd month
    # -> %Y%m has higher chance of matching date -> lower priority
    fmts = (
        "%Y",
        "%m",
        "%d",
        "%y%m%d",
        "%Y%m",
        "%Y%m%d",
        "%m%d",
        "%Y%d",
        "%Y-%m-%d",
        "%Y-%m",
    )
    tracked_fmt = []
    output = []
    for item in path.split("/"):
        for fmt in fmts:
            try:
                extracted_date_str = re.sub(r"[a-zA-Z=_]", "", item)
                if extracted_date_str:
                    dt = datetime.strptime(extracted_date_str, fmt)
                    fuzzy_tokens = item.replace(extracted_date_str, "")
                    pattern = fuzzy_tokens + fmt
                    pattern_parsed = dt.strftime(pattern)
                    if fmt not in tracked_fmt and pattern_parsed == item:
                        output.append(pattern)
                        tracked_fmt.append(fmt)
                        break
            except Exception:
                pass
    return "/".join(output)


def is_invalid_path(path: str) -> bool:
    """Check path contain specific keyword
    Args:
        path (str): path to check

    Returns:
        bool: whether path is invalid
    """
    kws = ["__HIVE_DEFAULT_PARTITION__", "_SUCCESS"]
    return any([kw in path for kw in kws])


def generate_properties(partition_by: Optional[str], directory: str):
    properties = {}
    if partition_by:
        properties = {
            "location": directory,
            "is_partition": "True",
            "partition_by": partition_by,
            "is_time_range_required": "True",
            "is_metadata_embedded": "True",
        }
    else:
        properties = {
            "location": directory,
            "is_partition": "False",
            "is_time_range_required": "False",
            "is_metadata_embedded": "True",
        }
    return properties


def add_domain_to_entity_wu(
    entity_type: str, entity_urn: str, domain_urn: str
) -> Iterable[MetadataWorkUnit]:
    mcp = MetadataChangeProposalWrapper(
        entityType=entity_type,
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=f"{entity_urn}",
        aspectName="domains",
        aspect=DomainsClass(domains=[domain_urn]),
    )
    wu = MetadataWorkUnit(id=f"{domain_urn}-to-{entity_urn}", mcp=mcp)
    yield wu


_COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")


def get_column_type(column_type: Any) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """
    TypeClass: Optional[Type] = None
    for sql_type in _field_type_mapping.keys():
        if (
            column_type in _spark_str_to_spark_type
            and _spark_str_to_spark_type[column_type] == sql_type
        ):
            TypeClass = _field_type_mapping[sql_type]
            break

    if TypeClass is None:
        # sql_report.report_warning(
        #     dataset_name, f"unable to map type {column_type!r} to metadata schema"
        # )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_fields_for_column(column: dict) -> List[SchemaField]:
    field = SchemaField(
        fieldPath=column["name"],
        type=get_column_type(column["type"]),
        nativeDataType=column.get("full_type", column["type"]),
        description=column.get("comment", None),
        nullable=column["nullable"],
        recursive=False,
    )
    return [field]


def check_complex(fields: List[SchemaField]):
    if _COMPLEX_TYPE.match(fields[0].nativeDataType) and isinstance(
        fields[0].type.type, NullTypeClass
    ):
        assert len(fields) == 1

        field = fields[0]
        # Get avro schema for subfields along with parent complex field
        avro_schema = get_avro_schema_for_hive_column(
            field.fieldPath, field.nativeDataType
        )

        new_fields = schema_util.avro_schema_to_mce_fields(
            json.dumps(avro_schema), default_nullable=True
        )

        # First field is the parent complex field
        new_fields[0].nullable = field.nullable
        new_fields[0].description = field.description
        new_fields[0].isPartOfKey = field.isPartOfKey
        return new_fields

    return fields
