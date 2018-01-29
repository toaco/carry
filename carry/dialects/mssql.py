from __future__ import unicode_literals

from sqlalchemy import text

from carry.dialects.base import GenericSqlHelper


class MSSqlHelper(GenericSqlHelper):
    def truncate(self, names):
        pass

    def relations(self, schema):
        raise NotImplementedError

    def create_view(self, name, sql):
        sql_ = """
        IF exists(SELECT *
            FROM sysobjects
                WHERE name = '{name}')
              BEGIN
                DROP VIEW {name}
              END
        """.format(name=name)
        # because of sqlalchemy can't detect this is not a simple query, so autocommit must be True
        self.engine.execute(text(sql_).execution_options(autocommit=True))
        sql_ = """
        CREATE VIEW {name} AS
        ({sql})
        """.format(name=name, sql=sql)
        self.engine.execute(sql_)
