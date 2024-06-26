# This import verifies that the dependencies are available.
import pymysql  # noqa: F401

from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig


class TiDBConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:3306"
    scheme = "mysql+pymysql"


class TiDBSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "tidb"

    @classmethod
    def create(cls, config_dict, ctx):
        config = TiDBConfig.parse_obj(config_dict)
        return cls(config, ctx)
