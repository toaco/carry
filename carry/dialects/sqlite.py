from sqlalchemy import text

from carry.dialects.base import GenericSqlHelper


class SqliteHelper(GenericSqlHelper):
    def relations(self, schema):
        raise NotImplementedError

    def create_view(self, name, sql):
        self.engine.execute(text(u'DROP VIEW IF EXISTS {name};'.format(name=name)))
        sql = u"""
        CREATE VIEW IF NOT EXISTS {name}
        AS {sql}""".format(name=name, sql=sql)
        self.engine.execute(text(sql))

    def truncate(self, names):
        sqls = ('DELETE FROM {}'.format(name) for name in names)
        for sql in sqls:
            self.engine.execute(text(sql))
