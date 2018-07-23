from sqlalchemy import text

from carry.dialects import GenericSqlHelper


class PostgresHelper(GenericSqlHelper):
    def relations(self, schema):
        raise NotImplementedError

    def create_view(self, name, sql):
        sql = """
        CREATE OR REPLACE VIEW {name}
        AS {sql}""".format(name=name, sql=sql)
        self.engine.execute(text(sql))

    def truncate(self, names):
        sql = """
        TRUNCATE TABLE ONLY {}
        RESTART IDENTITY CASCADE
        """.format(','.join(names))
        self.engine.execute(text(sql))
