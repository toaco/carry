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
