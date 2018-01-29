from __future__ import unicode_literals

import sqlalchemy


class SqlHelper(object):
    def __init__(self, engine):
        self.engine = engine

    def relations(self, schema):
        raise NotImplementedError

    def get_sorted_tables(self, schema):
        raise NotImplementedError

    def create_view(self, name, sql):
        raise NotImplementedError

    def truncate(self, names):
        raise NotImplementedError

    def drop_view(self, name):
        raise NotImplementedError

    def dependency(self, name):
        raise NotImplementedError


class GenericSqlHelper(SqlHelper):
    """
    reference:http://docs.sqlalchemy.org/en/latest/core/reflection.html
    """

    def __init__(self, engine):
        super(GenericSqlHelper, self).__init__(engine)
        self.inspector = sqlalchemy.inspect(self.engine)

    def relations(self, schema):
        raise NotImplementedError

    def get_sorted_tables(self, schema=None):
        if schema is None:
            schema = self.inspector.default_schema_name
        result = []
        rows = self.inspector.get_sorted_table_and_fkc_names(schema)
        for row in rows:
            name = row[0]
            if name:
                result.append(name)
            else:
                break
        return result

    def create_view(self, name, sql):
        raise NotImplementedError

    def truncate(self, names):
        raise NotImplementedError

    def drop_view(self, name):
        self.engine.execute('DROP VIEW {}'.format(name))

    def dependency(self, name):
        result = []
        for item in self.inspector.get_foreign_keys(name):
            result.append(item['referred_table'])
        return result
