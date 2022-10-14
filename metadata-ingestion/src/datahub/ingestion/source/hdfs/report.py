import dataclasses
from dataclasses import field as dataclass_field
from typing import List

from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
)


@dataclasses.dataclass
class HDFSSourceReport(StatefulIngestionReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)
    soft_deleted_stale_entities: List[str] = dataclasses.field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)
