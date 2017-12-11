from carry.dialects.base import GenericSqlHelper
from carry.dialects.mssql import MSSqlHelper
from carry.dialects.mysql import MySqlHelper
from carry.dialects.oracle import OracleHelper


class SqlHelperFactory(object):
    @classmethod
    def create(cls, engine):
        registered_helpers = {
            'mysql': MySqlHelper,
            'oracle': OracleHelper,
            'mssql': MSSqlHelper,
        }
        helper_cls = registered_helpers.get(engine.name, GenericSqlHelper)
        return helper_cls(engine)
