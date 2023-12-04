import itertools
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator

from datahub.ingestion.source.hdfs.config import HDFSSourceConfig
from datahub.ingestion.source.hdfs.report import HDFSSourceReport
from datahub.ingestion.source.hdfs.utils import (
    FolderToScan,
    HdfsFileSystemUtils,
    HdfsUtils,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OtherSchemaClass,
    _Aspect,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.registries.domain_registry import DomainRegistry
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from datahub.telemetry import stats, telemetry

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


@platform_name("HDFS", id="hdfs")
@config_class(HDFSSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class HDFSSource(StatefulIngestionSourceBase):
    source_config: HDFSSourceConfig
    hdfs_report = HDFSSourceReport()
    container_WU_creator: ContainerWUCreator
    hdfs_fs_utils: HdfsFileSystemUtils
    hdfs_utils: HdfsUtils

    @classmethod
    def create(cls, config_dict, ctx):
        config = HDFSSourceConfig.parse_obj(config_dict)

        return cls(config, ctx)

    def __init__(self, config: HDFSSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.spark = self.init_spark()
        self.hdfs_fs_utils = HdfsFileSystemUtils(
            self.spark, self.source_config.hadoop_host, self.source_config.format
        )
        self.profiling_times_taken = []
        if self.source_config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.source_config.domain],
                graph=self.ctx.graph,
            )

    def init_spark(self):
        conf = SparkConf()
        conf.set(
            "spark.app.name",
            f"datahub.ingestion/{self.source_config.location if type(self.source_config.location) == str else self.source_config.location[:2]}",
        )
        for option, value in self.source_config.spark_config.items():
            conf.set(option, value)
        return SparkSession.builder.config(conf=conf).getOrCreate()

    def get_fields(self, paths: List[str]):
        try:
            extension = self.source_config.format
            if extension == "parquet":
                df = self.spark.read.parquet(*paths)
            elif extension == "csv":
                # see https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe
                df = self.spark.read.csv(
                    *paths,
                    header="True",
                    inferSchema="True",
                    sep=",",
                    ignoreLeadingWhiteSpace=True,
                    ignoreTrailingWhiteSpace=True,
                )
            elif extension == "tsv":
                df = self.spark.read.csv(
                    *paths,
                    header="True",
                    inferSchema="True",
                    sep="\t",
                    ignoreLeadingWhiteSpace=True,
                    ignoreTrailingWhiteSpace=True,
                )
            elif extension == "json":
                df = self.spark.read.json(*paths)
            elif extension == "arvo":
                df = self.spark.read.format("avro").load(*paths)
            else:
                self.report.report_warning(
                    extension, f"Unsupported file extension {extension}"
                )
        except Exception as e:
            self.report.report_warning(
                paths, f"Failed to get schema, the error was {e}"
            )
        column_fields = [
            HdfsUtils.check_complex(HdfsUtils.get_schema_fields_for_column(col))
            for col in [
                {"name": f_name, "type": f_type, "nullable": False}
                for f_name, f_type in df.dtypes
            ]
        ]
        column_fields = list(itertools.chain(*column_fields))
        column_fields = sorted(column_fields, key=lambda f: f.fieldPath)

        return column_fields

    def get_table_profile(
        self, folder: FolderToScan, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Importing here to avoid Deequ dependency for non profiling use cases
        # Deequ fails if Spark is not available which is not needed for non profiling use cases
        from datahub.ingestion.source.s3.profiling import _SingleTableProfiler
        from pydeequ.analyzers import AnalyzerContext

        # read in the whole table with Spark for profiling
        table = None
        try:
            table = self.spark.read.format(self.source_config.format).load(
                folder.folder_to_profile
            )
        except Exception as e:
            logger.error(e)

        # if table is not readable, skip
        if table is None:
            self.report.report_warning(
                folder.path,
                f"unable to read from file {folder.path}",
            )
            return

        if (
            self.source_config.profiling.enabled
            and self.source_config.profile_patterns.allowed(folder.path)
        ):
            with PerfTimer() as timer:
                # init PySpark analysåis object
                logger.debug(
                    f"Profiling {folder.path}: reading file and computing nulls+uniqueness {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                )
                table_profiler = _SingleTableProfiler(
                    table,
                    self.spark,
                    self.source_config.profiling,
                    self.report,
                    folder.path,
                )

                logger.debug(
                    f"Profiling {folder.path}: preparing profilers to run {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                )
                # instead of computing each profile individually, we run them all in a single analyzer.run() call
                # we use a single call because the analyzer optimizes the number of calls to the underlying profiler
                # since multiple profiles reuse computations, this saves a lot of time
                table_profiler.prepare_table_profiles()

                # compute the profiles
                logger.debug(
                    f"Profiling {folder.path}: computing profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                )
                analysis_result = table_profiler.analyzer.run()
                analysis_metrics = AnalyzerContext.successMetricsAsDataFrame(
                    self.spark, analysis_result
                )

                logger.debug(
                    f"Profiling {folder.path}: extracting profiles {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
                )
                table_profiler.extract_table_profiles(analysis_metrics)

                time_taken = timer.elapsed_seconds()

                logger.info(
                    f"Finished profiling {folder.path}; took {time_taken:.3f} seconds"
                )

                self.profiling_times_taken.append(time_taken)

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=table_profiler.profile,
            ).as_workunit()

    def ingest_table(self, folder: FolderToScan) -> Iterable[MetadataWorkUnit]:
        aspects: List[Optional[_Aspect]] = []
        logger.info("Ingesting table %s", folder.path)
        browse_path = folder.path.strip("/")
        data_name = ".".join(browse_path.split("/"))
        data_platform_urn = make_data_platform_urn(self.source_config.platform)
        logger.info("Creating dataset urn with name: %s", browse_path)
        platform = self.source_config.platform if not folder.is_delta else "delta-lake"

        self.container_WU_creator.platform = platform
        dataset_urn = make_dataset_urn(
            platform,
            data_name,
            self.source_config.env,
        )
        if self.source_config.platform_instance:
            data_platform_instance = DataPlatformInstanceClass(
                platform=data_platform_urn,
                instance=make_dataplatform_instance_urn(
                    self.source_config.platform, self.source_config.platform_instance
                ),
            )
            aspects.append(data_platform_instance)

        is_partition = "True" if folder.partition_path != "" else "False"
        customProperties = {
            "database": self.source_config.database,
            "db_host": self.source_config.db_host,
            "format": self.source_config.format,
            "location": folder.path,
            "is_partition": is_partition,
            "partition_by": folder.partition_path,
            "is_time_range_required": is_partition,
            "is_metadata_embedded": "True",
            "partitions": " ".join(folder.partitions),
        }
        dataset_properties = DatasetPropertiesClass(
            description="",
            name=data_name,
            customProperties=customProperties,
        )
        aspects.append(dataset_properties)
        if len(folder.files) > 0:
            try:
                fields = self.get_fields(folder.files)
                schema_metadata = SchemaMetadata(
                    schemaName=data_name,
                    platform=data_platform_urn,
                    version=0,
                    hash="",
                    fields=fields,
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                aspects.append(schema_metadata)
            except Exception as e:
                logger.error(
                    "Failed to get schema for %s, the error was %s", folder.path, e
                )
                return

            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=aspects,
            ):
                yield mcp.as_workunit()

            yield from self.container_WU_creator.create_container_hierarchy(
                folder.path, dataset_urn
            )

            domain_urn: Optional[str] = None
            for domain, pattern in self.source_config.domain.items():
                if pattern.allowed(data_name):
                    domain_urn = make_domain_urn(
                        self.domain_registry.get_domain_urn(domain)
                    )

            if domain_urn:
                yield from add_domain_to_entity_wu(
                    entity_urn=dataset_urn,
                    domain_urn=domain_urn,
                )

            if self.source_config.profile_patterns.allowed(folder.path):
                yield from self.get_table_profile(folder, dataset_urn)
        else:
            self.report.report_warning(folder.path, f"No files to infer")

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.source_config.platform,
            self.source_config.platform_instance,
            self.source_config.env,
        )
        with PerfTimer() as timer:
            assert self.source_config.location
            folders_tracked = []
            for location in self.source_config.location:
                for folder in self.hdfs_fs_utils.generate_directories(
                    location, folders_tracked
                ):
                    if self.source_config.schema_pattern.allowed(folder["path"]):
                        self.hdfs_fs_utils.generate_directories(folder, folders_tracked)
            self.hdfs_fs_utils.mark_delta(folders_tracked)

            for folder in folders_tracked:
                yield from self.ingest_table(folder)

            total_time_taken = timer.elapsed_seconds()

            logger.info(
                "Extracted %s tables in %s",
                len(folders_tracked),
                total_time_taken,
            )

            time_percentiles: Dict[str, float] = {}

            if len(self.profiling_times_taken) > 0:
                percentiles = [50, 75, 95, 99]
                percentile_values = stats.calculate_percentiles(
                    self.profiling_times_taken, percentiles
                )

                time_percentiles = {
                    f"table_time_taken_p{percentile}": stats.discretize(
                        percentile_values[percentile]
                    )
                    for percentile in percentiles
                }

            telemetry.telemetry_instance.ping(
                "data_lake_profiling_summary",
                # bucket by taking floor of log of time taken
                {
                    "total_time_taken": stats.discretize(total_time_taken),
                    "count": stats.discretize(len(self.profiling_times_taken)),
                    "platform": self.source_config.platform,
                    **time_percentiles,
                },
            )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self):
        return self.report