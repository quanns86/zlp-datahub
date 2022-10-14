
from typing import Any, Dict, List, Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
)


class HDFSStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


class HDFSSourceConfig(StatefulIngestionConfigBase):

    env: str = DEFAULT_ENV
    hadoop_host: str = '127.0.0.1'
    database: str = 'data_platform'
    merge_schema: bool = True
    infer_latest: bool = False
    recursive: bool = True
    location: List[str]
    format: str
    extension: Optional[List[str]]
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    domain: Dict[str, AllowDenyPattern] = dict()
    stateful_ingestion: Optional[HDFSStatefulIngestionConfig] = None

    @pydantic.root_validator()
    def ensure_extension_is_passed_to_config(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        if values.get('extension') is None or values.get('extension') == []:
            values['extension'] = [values.get('format')]
        return values
