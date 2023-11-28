import logging
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

import logging
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

# hide annoying debug errors from py4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


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
    profiling: DataLakeProfilerConfig = Field(
        default=DataLakeProfilerConfig(), description="Data profiling configuration"
    )
    spark_config = Field(default={})
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