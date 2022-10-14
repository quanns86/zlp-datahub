import itertools
import json
import logging
import os
import re
import socket
from datetime import datetime
from enum import Enum
from typing import Any, Iterable, List, Optional, Type, Union, cast

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
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
from pyspark.sql import types as spark_types

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    PlatformKey,
    SchemaKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hdfs.config import HDFSSourceConfig
from datahub.ingestion.source.hdfs.report import HDFSSourceReport
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DomainsClass,
    MapTypeClass,
    OtherSchemaClass,
    JobStatusClass
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    MapTypeClass
)

from sqlalchemy.sql import sqltypes as types
from sqlalchemy import create_engine, dialects, inspect
from typing import (
    Any,
    Iterable,
    List,
    Optional,
    Type,
    Union,
    cast,
)

from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column
import re

from datahub.ingestion.extractor import schema_util

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)

# for a list of all types, see https://spark.apache.org/docs/3.0.3/api/python/_modules/pyspark/sql/types.html
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

_spark_str_to_spark_type = {**spark_types._all_atomic_types, **spark_types._all_complex_types}


def get_column_type(
    report: SourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:
    """
    Maps known Spark types to datahub types
    """
    TypeClass: Any = None

    for field_type, type_class in _field_type_mapping.items():
        if _spark_str_to_spark_type.get(column_type) == field_type:
            TypeClass = type_class
            break

    # if still not found, report the warning
    if TypeClass is None:

        report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


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


def get_column_type(
    column_type: Any
) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """
    TypeClass: Optional[Type] = None
    for sql_type in _field_type_mapping.keys():
        if column_type in _spark_str_to_spark_type and _spark_str_to_spark_type[column_type] == sql_type:
            TypeClass = _field_type_mapping[sql_type]
            break

    if TypeClass is None:
        # sql_report.report_warning(
        #     dataset_name, f"unable to map type {column_type!r} to metadata schema"
        # )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_fields_for_column(
    column: dict
) -> List[SchemaField]:
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


class HDFSContainerSubTypes(str, Enum):
    DATABASE = "Database"
    SCHEMA = "Schema"


class HDFSSource(StatefulIngestionSourceBase):
    source_config: HDFSSourceConfig
    hdfs_report = HDFSSourceReport()
    platform = "hdfs"

    def __init__(self, config: HDFSSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.init_spark()

    def init_spark(self):
        conf = SparkConf()
        conf.set(
            "spark.app.name",
            f"datahub.hdfs_scheduled_ingest[{self.source_config.location if type(self.source_config.location) == str else self.source_config.location[:2]}]",
        )
        # Extract configuration file
        options = parse_config(self.source_config.env)
        for option, value in options.items():
            conf.set(option, value)
        # Bind address
        try:
            driver_host = socket.gethostbyname("actions")
            if driver_host:
                conf.set("spark.driver.bindAddress", driver_host)
        except Exception:
            self.hdfs_report.report_warning(
                "spark.driver.bindAddress", "Host not found, try using localhost"
            )
            conf.set("spark.driver.bindAddress", "0.0.0.0")

        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext
        self.Path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path
        URI = self.sc._gateway.jvm.java.net.URI
        self.hadoop_host = f"hdfs://{self.source_config.hadoop_host}"
        self.fs = self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
            URI(self.hadoop_host),
            self.sc._gateway.jvm.org.apache.hadoop.conf.Configuration(),
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = HDFSSourceConfig.parse_obj(config_dict)

        return cls(config, ctx)

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that sql_common provides.
        Subclasses can override as needed.
        """
        return JobId("common_ingest_from_sql_source")

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id == self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.source_config.stateful_ingestion
            and self.source_config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def gen_database_key(self) -> PlatformKey:
        return DatabaseKey(
            database=self.source_config.database,
            platform=self.platform,
            instance=self.source_config.env,
        )

    def gen_schema_key(self, schema: str) -> PlatformKey:
        return SchemaKey(
            database=self.source_config.database,
            schema=schema,
            platform=self.platform,
            instance=self.source_config.env,
        )

    def gen_database_containers(self) -> Iterable[MetadataWorkUnit]:
        database = self.source_config.database
        domain_urn = self._gen_domain_urn(database)

        database_container_key = self.gen_database_key()
        container_workunits = gen_containers(
            container_key=database_container_key,
            name=database,
            sub_types=[HDFSContainerSubTypes.DATABASE],
            domain_urn=domain_urn,
        )

        for wu in container_workunits:
            self.hdfs_report.report_workunit(wu)
            yield wu

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(domain)

        return domain_urn

    def gen_schema_containers(
        self, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        db_name = self.source_config.database
        schema_container_key = self.gen_schema_key(schema)

        database_container_key: Optional[PlatformKey] = None
        if db_name is not None:
            database_container_key = self.gen_database_key()

        container_workunits = gen_containers(
            schema_container_key,
            schema,
            [HDFSContainerSubTypes.SCHEMA],
            database_container_key,
        )

        for wu in container_workunits:
            self.hdfs_report.report_workunit(wu)
            yield wu

    def add_table_to_schema_container(
        self, dataset_urn: str, schema: str
    ) -> Iterable[Union[MetadataWorkUnit, MetadataWorkUnit]]:
        schema_container_key = self.gen_schema_key(schema)
        container_workunits = add_dataset_to_container(
            container_key=schema_container_key,
            dataset_urn=dataset_urn,
        )
        for wu in container_workunits:
            self.hdfs_report.report_workunit(wu)
            yield wu

    def _gen_domain_urn(self, dataset_name: str) -> Optional[str]:
        domain_urn: Optional[str] = None

        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(domain)

        return domain_urn

    def _get_domain_wu(
        self,
        dataset_name: str,
        entity_urn: str,
        entity_type: str,
    ) -> Iterable[MetadataWorkUnit]:

        domain_urn = self._gen_domain_urn(dataset_name)
        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type=entity_type,
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.hdfs_report.report_workunit(wu)
                yield wu

    def read_file(self, files_to_infer: List[str]) -> Optional[DataFrame]:

        extension = self.source_config.format

        if extension == "parquet":
            df = self.spark.read.option("mergeSchema", "true").parquet(*files_to_infer)
        elif extension == "csv":
            # see https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe
            df = self.spark.read.option("mergeSchema", "true").csv(
                *files_to_infer,
                header="True",
                inferSchema="True",
                sep=",",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif extension == "tsv":
            df = self.spark.read.option("mergeSchema", "true").csv(
                *files_to_infer,
                header="True",
                inferSchema="True",
                sep="\t",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif extension == "json":
            df = self.spark.read.option("mergeSchema", "true").json(*files_to_infer)
        elif extension == "arvo":
            try:
                df = (
                    self.spark.read.option("mergeSchema", "true")
                    .format("avro")
                    .load(*files_to_infer)
                )
            except Exception:
                self.hdfs_report.report_warning(
                    extension,
                    "To ingest avro files, please install the spark-avro package: https://mvnrepository.com/artifact/org.apache.spark/spark-avro_2.12/3.0.3",
                )
                return None

        # TODO: add support for more file types
        # elif file.endswith(".orc"):
        # df = self.spark.read.orc(file)
        else:
            self.hdfs_report.report_warning(
                extension, f"extension {extension} is unsupported"
            )
            return None

        # replace periods in names because they break PyDeequ
        # see https://mungingdata.com/pyspark/avoid-dots-periods-column-names/
        return df.toDF(*(c.replace(".", "_") for c in df.columns))

    def get_table_schema(
        self, schema: str, dataframe: DataFrame, **kwargs
    ) -> Iterable[MetadataWorkUnit]:
        table_name = kwargs.get("table_name")
        dataset_urn = make_dataset_urn(
            self.platform, table_name, self.source_config.env
        )

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )

        dataset_properties = DatasetPropertiesClass(
            description="",
            customProperties={
                **{
                    "database": self.source_config.database,
                    "format": self.source_config.format,
                },
                **kwargs.get("properties"),
            },
        )

        dataset_snapshot.aspects.append(dataset_properties)

        column_fields = [check_complex(get_schema_fields_for_column(col)) for col in [{"name": f_name, "type": f_type, "nullable": False} for f_name, f_type in dataframe.dtypes]]
        column_fields = list(itertools.chain(*column_fields))

        schema_metadata = SchemaMetadata(
            schemaName=table_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            fields=column_fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )

        dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=f"ingest-hdfs-{table_name}", mce=mce)
        self.hdfs_report.report_workunit(wu)
        yield wu

        yield from self.add_table_to_schema_container(dataset_urn=dataset_urn, schema=schema)

        yield from self._get_domain_wu(
            dataset_name=table_name,
            entity_urn=dataset_urn,
            entity_type="dataset"
        )

        # Add dataset-urn to current checkpoint
        if self.is_stateful_ingestion_configured():
            cur_checkpoint = self.get_current_checkpoint(
                self.get_default_ingestion_job_id()
            )
            if cur_checkpoint is not None:
                checkpoint_state = cast(
                    BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                )
                checkpoint_state.add_table_urn(dataset_urn)

    def ingest_table(
        self, schema, files_to_infer: List[str], **kwargs
    ) -> Iterable[MetadataWorkUnit]:
        table_name = kwargs.get("table_name")
        table = self.read_file(files_to_infer)

        # if table is not readable, skip
        if table is None:
            return

        # yield the table schema first
        logger.debug(
            f"Ingesting {table_name}: making table schemas {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        )
        yield from self.get_table_schema(
            schema=schema, dataframe=table, table_name=table_name, properties=kwargs.get("properties", {})
        )

    def get_relative_path_from_hadoop_host(self, path: str) -> str:
        return os.path.relpath(path, self.hadoop_host)

    def generate_table_name(self, directory):
        return ".".join([item for item in directory.split("/") if item])

    def get_files_to_infer(self, subfolders, subfiles):
        files_to_infer = []
        # Get the first file a
        # Check for actual file and format
        # ex: '/a/b/c.parquet' -> ['a/b/c', 'parquet']
        # if self.source_config.format in file_path.rsplit('/', 1)[-1]:nd append to files_to_infer list
        while subfiles.hasNext():
            file_path = str(subfiles.next().getPath())
            if not is_invalid_path(file_path):
                file_name = file_path.rsplit("/", 1)[-1]
                file_extension = file_path.rsplit("/", 1)[-1].rsplit(".", 1)[-1] if '.' in file_name else ''
                if file_extension in self.source_config.extension:
                    if self.source_config.infer_latest:
                        # Check if going through latest partition
                        # If yes, only get the latest parquet file
                        # If no, append first parquet file
                        if subfolders.hasNext() or (
                            not subfolders.hasNext() and not subfiles.hasNext()
                        ):
                            files_to_infer.append(file_path)
                            break
                    else:
                        files_to_infer.append(file_path)
                        break
        return files_to_infer

    def _gen_directories_recursive(self, to_check_directory: Union[str, List[str]]) -> Optional[List[str]]:
        folder_to_track = []
        folder_paths = []
        files = self.fs.listLocatedStatus(self.Path(to_check_directory))
        # Get parent directories
        while files.hasNext():
            path = str(files.next().getPath())
            if not is_invalid_path(path):
                folder_to_track.append(path)
        # Iterate, using stack to check for actual path
        while len(folder_to_track) > 0:
            has_subdirectories = False
            to_check = folder_to_track.pop()
            rel_path = self.get_relative_path_from_hadoop_host(to_check)
            files = self.fs.listLocatedStatus(self.Path("/" + rel_path))
            while files.hasNext():
                file = files.next()
                path = str(file.getPath())
                path_rel = self.get_relative_path_from_hadoop_host(path)
                # If nested folder doesn't belong to partition type folder and doesn't contain invalid keyword -> push to stack to check later
                if (
                    file.isDirectory()
                    and not infer_partition(path_rel)
                    and not is_invalid_path(path)
                ):
                    folder_to_track.append(path)
                    has_subdirectories = True
            # If current checking folder doesn't have sub-directories that have partition or that folder only contains format file -> actual folder
                if (
                    not has_subdirectories
                    and not is_invalid_path(rel_path)
                    and rel_path not in folder_paths
                ):
                    folder_paths.append(rel_path)
        return folder_paths

    def get_directories_to_check(self) -> List[str]:
        raw_input = self.source_config.location
        accumulated_directory_paths = []
        if self.source_config.recursive:
            for dir in raw_input:
                accumulated_directory_paths.extend(self._gen_directories_recursive(dir))
        else:
            accumulated_directory_paths = raw_input.copy()
        self.hdfs_report.report_warning("ready-to-be-ingested", accumulated_directory_paths)
        return accumulated_directory_paths

    def loop_partitions(self, directory):
        files_to_infer = []
        if not directory.startswith("/"):
            directory = "/" + directory
        if not directory.endswith("/"):
            directory += "/"

            # For each child folder, get the first parquet file and append file path to files_to_infer list
        subfolders = self.fs.listLocatedStatus(self.Path(directory))
        while subfolders.hasNext():
            sub_folder_path_rel = self.get_relative_path_from_hadoop_host(
                str(subfolders.next().getPath())
            )
            if not is_invalid_path(sub_folder_path_rel):
                if not sub_folder_path_rel.startswith("/"):
                    sub_folder_path_rel = "/" + sub_folder_path_rel
                subfiles = self.fs.listFiles(
                    self.Path(sub_folder_path_rel), True
                )
                files_to_infer.extend(self.get_files_to_infer(subfolders, subfiles))
        return files_to_infer

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        config_dict = self.source_config.dict()
        host_port = config_dict.get("hadoop_host", "no_host_port")
        database = config_dict.get("database", "no_database")
        return f"{self.platform}_{host_port}_{database}"

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create the custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.source_config,
                state=BaseSQLAlchemyCheckpointState(),
            )
        return None

    def update_default_job_run_summary(self) -> None:
        summary = self.get_job_run_summary(self.get_default_ingestion_job_id())
        if summary is not None:
            # For now just add the config and the report.
            summary.config = self.config.json()
            summary.custom_summary = self.hdfs_report.as_string()
            summary.runStatus = (
                JobStatusClass.FAILED
                if self.get_report().failures
                else JobStatusClass.COMPLETED
            )

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseSQLAlchemyCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if (
            self.source_config.stateful_ingestion
            and self.source_config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            def soft_delete_item(urn: str, type: str) -> Iterable[MetadataWorkUnit]:
                entity_type: str = "dataset"

                if type == "container":
                    entity_type = "container"

                logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
                mcp = MetadataChangeProposalWrapper(
                    entityType=entity_type,
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=StatusClass(removed=True),
                )
                wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
                self.hdfs_report.report_workunit(wu)
                self.hdfs_report.report_stale_entity_soft_deleted(urn)
                yield wu

            last_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, last_checkpoint.state
            )
            cur_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, cur_checkpoint.state
            )

            for table_urn in last_checkpoint_state.get_table_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(table_urn, "table")

            for view_urn in last_checkpoint_state.get_view_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(view_urn, "view")

            for container_urn in last_checkpoint_state.get_container_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(container_urn, "container")

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        tracked_schema = []
        yield from self.gen_database_containers()
        for directory in self.get_directories_to_check():
            if self.source_config.schema_pattern.allowed(directory):
                try:
                    files_to_infer = self.loop_partitions(directory)
                    try:
                        schema = [item for item in directory.split("/") if item][0]
                        if schema and schema not in tracked_schema:
                            yield from self.gen_schema_containers(schema)
                            tracked_schema.append(schema)
                    except:
                        self.hdfs_report.report_warning("schema-creation-failure", f"cannot create schema for directory {directory}")
                    while True:
                        if len(files_to_infer) > 0:
                            if not self.source_config.merge_schema:
                                files_to_infer = [files_to_infer[-1]] if self.source_config.infer_latest else [files_to_infer[0]]
                            parent_dir, filename = os.path.split(files_to_infer[0])
                            parent_dir_rel = self.get_relative_path_from_hadoop_host(parent_dir)
                            partition_by = infer_partition(parent_dir_rel)
                            try:

                                yield from self.ingest_table(
                                    schema=schema,
                                    files_to_infer=files_to_infer,
                                    table_name=self.generate_table_name(directory),
                                    properties=generate_properties(partition_by, directory),
                                )
                            except Exception as e:
                                # Handle conflict schema merging
                                if 'Failed merging schema' in str(e):
                                    files_to_infer.pop(0)
                                    continue
                                self.hdfs_report.report_failure("hdfs-ingestion", f"{directory}, {str(e)[:150]}")
                        else:
                            self.hdfs_report.report_warning(
                                "hdfs-ingestion", f"no files to ingest in {directory}"
                            )
                        break
                except Exception as e:
                    self.hdfs_report.report_failure("hdfs-ingestion", f"{directory}, {str(e)[:100]}")
            else:
                self.hdfs_report.report_file_dropped(directory)
        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    def get_report(self):
        return self.hdfs_report

    def close(self):
        self.spark.stop()
        self.update_default_job_run_summary()
        self.prepare_for_commit()
