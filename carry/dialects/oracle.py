from sqlalchemy import text

from carry.dialects.base import GenericSqlHelper


class OracleHelper(GenericSqlHelper):
    def relations(self, schema):
        raise NotImplementedError

    def create_view(self, name, sql):
        sql = u"""
CREATE OR REPLACE VIEW {name}
AS {sql} WITH READ ONLY""".format(name=name, sql=sql)
        self.engine.execute(text(sql))
