from typing import Dict, List, Optional

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

from typing import Dict, List, Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from pydantic.fields import Field
import os



class HDFSSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )
    platform = Field(
        default="hdfs",
        description="The platform that this source connects to (either 'hdfs'). ",
    )
    database = Field(
        default="",
        description="The platform that this source connects to (either 'hdfs'). ",
    )
    db_host = Field(
        default="",
        description="",
    )
    profile_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to profile ",
    )
    profiling: DataLakeProfilerConfig = Field(
        default=DataLakeProfilerConfig(), description="Data profiling configuration"
    )
    spark_config = Field(
        default={
            "spark.master": f'{os.environ.get("SPARK_MASTER")}',
            "spark.driver.host": os.environ.get("SPARK_DRIVER"),
            "spark.submit.deployMode": "client",
            "spark.executor.memory": "5g",
            "spark.driver.memory": "4g",
            "spark.executor.cores": 2,
            "spark.cores.max": 8,
        },
        description="Spark config",
    )
    hadoop_host: str = Field(
        default="localhost",
        description="The host of the hadoop cluster.",
    )
    location: List[str] = Field(
        default=[],
        description="The location of the dataset.",
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="The regex pattern to match the schema.",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default={},
        description="The domain of the dataset.",
    )
    format: str = Field(
        default="parquet",
        description="The format of the dataset.",
    )