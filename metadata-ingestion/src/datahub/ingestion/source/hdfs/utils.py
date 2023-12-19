import json
import logging
import re
from datetime import datetime
from functools import lru_cache
from itertools import chain
from typing import Any, List, Optional, Type

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
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

from datahub.ingestion.extractor import schema_util
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
    MapTypeClass,
)
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column

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

DELTA_PATTERN = "/_delta_log"
_COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


class FolderToScan:
    path: str
    owner: str
    partition_path: str
    is_delta: bool = False
    files: List[str] = []
    partitions: List[str] = []
    folder_to_profile = str

    def __init__(
        self,
        path: str,
        owner: str,
        partition_path: str,
        is_delta: bool,
        files: List[str],
        partitions: List[str],
        folder_to_profile: str,
    ) -> None:
        self.path = path
        self.owner = owner
        self.partition_path = partition_path
        self.is_delta = is_delta
        self.files = files
        self.partitions = partitions
        self.folder_to_profile = folder_to_profile

    def __repr__(self) -> str:
        return f"FolderToScan(path={self.path}, owner={self.owner}, partition_path={self.partition_path}, is_delta={self.is_delta}, files={self.files}), folder_to_profile={self.folder_to_profile}"


class HdfsFileSystemUtils:
    spark: SparkSession
    hadoop_host: str
    format: str

    def __init__(self, spark, hadoop_host: str, format: str):
        self.spark = spark
        self.sc = self.spark.sparkContext
        self.Path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path
        java_import(self.sc._gateway.jvm, "java.net.URI")
        self.hadoop_host = hadoop_host
        self.format = format

    @property
    @lru_cache(maxsize=None)
    def fs(self):
        return self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
            self.sc._gateway.jvm.java.net.URI(self.hadoop_host),
            self.sc._gateway.jvm.org.apache.hadoop.conf.Configuration(),
        )

    def is_valid_path(self, path: str, additional_patterns: List[str] = []) -> bool:
        """Check path contain specific keyword
        Args:
            path (str): path to check

        Returns:
            bool: whether path is invalid
        """
        kws = [
            "__HIVE_DEFAULT_PARTITION__",
            "_SUCCESS",
            "_temporary",
            ".spark-staging",
            "commits",
            "_spark_metadata",
            "checkpoints",
            *additional_patterns,
        ]

        return all(kw not in path for kw in kws)

    def infer_partition(self, path: str) -> str:
        """Generify hdfs path by predefined formats

        Args:
            path (str): path to infer

        Returns:
            str: partition_by path
        """
        date_formats = (
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
        tracked_formats = []
        non_partitions = []
        partitions = []
        has_partition_pattern = re.compile("^[^0-9]*[0-9_-]+$")
        for item in path.split("/"):
            if has_partition_pattern.search(item):
                if "hour=" in item:
                    continue
                for date_format in date_formats:
                    try:
                        extracted_date_str = re.sub(r"[a-zA-Z=_]", "", item)
                        if extracted_date_str:
                            date = datetime.strptime(extracted_date_str, date_format)
                            fuzzy_tokens = item.replace(extracted_date_str, "")
                            pattern = fuzzy_tokens + date_format
                            pattern_parsed = date.strftime(pattern)
                            if (
                                date_format not in tracked_formats
                                and pattern_parsed == item
                            ):
                                partitions.append(pattern)
                                tracked_formats.append(date_format)
                                break
                    except ValueError:
                        pass
            else:
                non_partitions.append(item)
        return "/".join(non_partitions), "/".join(partitions)

    def check_exists(self, path: str, folders_to_scan):
        return any(p.path == path for p in folders_to_scan)

    def generate_relative_path(self, file):
        return file.getPath().toUri().getPath()

    def generate_children_folders(
        self, location: str, ignore_patterns: List[str] = [], recursive: bool = False
    ):
        try:
            files_iterator = self.fs.listLocatedStatus(self.Path(location))
            folders = []
            while files_iterator.hasNext():
                file = files_iterator.next()
                path = self.generate_relative_path(file)
                if self.is_valid_path(path, additional_patterns=ignore_patterns):
                    if file.isDirectory():
                        if recursive:
                            folders.extend(
                                self.generate_children_folders(
                                    path, ignore_patterns, recursive
                                )
                            )
                        else:
                            folders.append(path)
                    else:
                        folders.append(location)
                        return folders

            return folders
        except Exception as e:
            logger.debug(e)
            return []

    def generate_children_files(self, location: str, recursive: bool = False):
        files_iterator = self.fs.listFiles(self.Path(location), recursive)
        files = []
        while files_iterator.hasNext():
            file = files_iterator.next()
            path = file.getPath().toString()
            if file.isFile() and self.format in path and self.is_valid_path(path):
                files.append(file.getPath().toString())
                break
        return files

    def truncate_root_path(self, root_path: str, partitions: List[str]):
        return [x.replace(root_path, "").strip("/") for x in partitions]

    def vote_partition_patterns(self, partitions: List[str]):
        votes = {}
        for partition in partitions:
            _, partition_pattern = self.infer_partition(partition)
            votes[partition_pattern] = votes.get(partition_pattern, 0) + 1
        return max(votes, key=votes.get)

    def generate_directories(
        self, location: str, folders_to_scan: List[FolderToScan]
    ) -> List[dict]:
        files_iterator = self.fs.listLocatedStatus(self.Path(location))
        folders = []
        while files_iterator.hasNext():
            f = files_iterator.next()
            path = self.generate_relative_path(f)
            if self.is_valid_path(path, [DELTA_PATTERN]):
                if f.isFile():
                    folder_only = "/".join(path.split("/")[:-1])
                    base_path, partition_pattern = self.infer_partition(folder_only)
                    if not self.check_exists(base_path, folders_to_scan):
                        if partition_pattern:
                            partitions = self.generate_children_folders(
                                base_path, [DELTA_PATTERN], True
                            )
                            if len(partitions) > 0:
                                files = list(
                                    chain(
                                        *(
                                            self.generate_children_files(folder)
                                            for folder in partitions
                                        )
                                    )
                                )
                                logger.info(f"Ready to ingest {base_path}")
                                folders_to_scan.append(
                                    FolderToScan(
                                        path=base_path,
                                        owner=f.getOwner(),
                                        partition_path=self.vote_partition_patterns(
                                            partitions
                                        ),
                                        is_delta=False,
                                        files=files,
                                        partitions=self.truncate_root_path(
                                            base_path, partitions
                                        ),
                                        folder_to_profile=f"{self.hadoop_host}/{partitions[-1]}",
                                    )
                                )
                        else:
                            logger.info(f"Ready to ingest {base_path}")
                            folders_to_scan.append(
                                FolderToScan(
                                    path=base_path,
                                    owner=f.getOwner(),
                                    partition_path=partition_pattern,
                                    is_delta=False,
                                    files=self.generate_children_files(base_path),
                                    partitions=[],
                                    folder_to_profile=f"{self.hadoop_host}/{base_path}",
                                )
                            )
                else:
                    self.generate_directories(path, folders_to_scan)
        return folders

    def mark_delta(self, folders_to_scan: List[FolderToScan]):
        for folder in folders_to_scan:
            children_folders = self.generate_children_folders(folder.path)
            if any([DELTA_PATTERN in f for f in children_folders]):
                folder.is_delta = True


class HdfsUtils:
    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    @staticmethod
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

    @staticmethod
    def get_schema_fields_for_column(column: dict) -> List[SchemaField]:
        field = SchemaField(
            fieldPath=column["name"],
            type=HdfsUtils.get_column_type(column["type"]),
            nativeDataType=column.get("full_type", column["type"]),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
        )
        return [field]

    @staticmethod
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
