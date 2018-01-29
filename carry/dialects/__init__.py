from __future__ import unicode_literals

from carry.dialects.base import GenericSqlHelper
from carry.dialects.mssql import MSSqlHelper
from carry.dialects.mysql import MySqlHelper
from carry.dialects.oracle import OracleHelper
from carry.dialects.sqlite import SqliteHelper


class SqlHelperFactory(object):
    @classmethod
    def create(cls, engine):
        registered_helpers = {
            'mysql': MySqlHelper,
            'oracle': OracleHelper,
            'mssql': MSSqlHelper,
            'sqlite': SqliteHelper,
        }
        helper_cls = registered_helpers.get(engine.name, GenericSqlHelper)
        return helper_cls(engine)
