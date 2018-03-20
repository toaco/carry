from __future__ import unicode_literals

import os

import pandas
import sqlalchemy

from carry.dataframe import DFIteratorAdapter
from carry.dialects import SqlHelperFactory
from carry.logger import logger


class StoreFactory(object):
    @classmethod
    def create_all(cls, store_configs):
        stores = []
        for store_config in store_configs:
            if 'url' in store_config:
                store = RDB(**store_config)
                stores.append(store)
            else:
                store = CSV(**store_config)
                stores.append(store)
        return StoreCollection(stores)


class StoreCollection(object):
    def __init__(self, stores):
        self.stores = stores

    def find_by_store_name(self, name):
        for store in self.stores:
            if store.name == name:
                return store

    def find_by_table_name(self, name, store_name_limits=None):
        store_name_limits = set(store_name_limits or [])
        for store in self.stores:
            if name in store and store.name in store_name_limits:
                return store

    def drop_created_views(self):
        for store in self.stores:
            store.drop_created_views()


def convert_table_name(func):
    def _wrapper(self, name, *args, **kw):
        """convert the table name of "name" in store. 
        if table name is "Table" and store is case insensitive, _raw_name("table") will return 
        "Table" instead of raising an exception.
        """
        if self._case_sensitive:
            if name not in self._tables:
                raise ValueError('Table {} not in {}'.format(name, self.name))
        else:
            try:
                name = self._case_insensitive_names[name.lower()]
            except KeyError:
                raise ValueError('Table {} not in {}'.format(name, self.name))
        return func(self, name, *args, **kw)

    return _wrapper


class Store(object):
    def __init__(self, name, tables, case_sensitive=False):
        """
        when `case_sensitive` is False, `convert_table_name` will convert input table name to inner
        table name. this should only be used for method which expected the table already existed in 
        Store.
        
        :param tables: 
        :param case_sensitive: if False, all API of this class must not care the case of table name
        """
        self.name = name
        if tables:
            self._tables = set(tables)
        else:
            self._tables = set()

        if not case_sensitive:
            self._case_insensitive_names = {}
            self._update_case_insensitive_names()
        self._case_sensitive = case_sensitive

    def _update_case_insensitive_names(self):
        self._case_insensitive_names = {}
        for table in self._tables:
            self._case_insensitive_names[table.lower()] = table

    def __contains__(self, table):
        try:
            self._convert_table_name(table)
        except ValueError:
            return False
        return True

    def _convert_table_name(self, name):
        return convert_table_name(lambda this, table: table)(self, name)

    @property
    def ordered_tables(self):
        raise NotImplementedError

    @convert_table_name
    def count(self, name):
        raise NotImplementedError

    @convert_table_name
    def get(self, name, **config):
        raise NotImplementedError

    @convert_table_name
    def put(self, name, data, **config):
        raise NotImplementedError

    def truncate(self, names):
        raise NotImplementedError

    def drop_created_views(self):
        raise NotImplementedError


def rename_chunk_size(config):
    if 'chunk_size' in config:
        config['chunksize'] = config['chunk_size']
        del config['chunk_size']
    return config


class RDB(Store):
    def __init__(self, name, url, create_view=False, view_prefix='', tables=None, echo=False):
        self.url = url
        try:
            self.engine = sqlalchemy.create_engine(url, echo=echo, server_side_cursors=True)
        except TypeError:
            self.engine = sqlalchemy.create_engine(url, echo=echo)

        self.create_view = create_view
        self.view_prefix = view_prefix
        self.name_and_sql_paths = self._find_name_and_sql_paths(name)
        self.sql_helper = SqlHelperFactory.create(self.engine)
        self.created_views = []

        if not tables:
            inspector = sqlalchemy.inspect(self.engine)
            tables = inspector.get_table_names()
            self.materialized_tables = tables[:]
            # treat .sql file as a table
            tables.extend(self.name_and_sql_paths.keys())
        super(RDB, self).__init__(name, tables)
        self._dependency = {}

    @property
    def ordered_tables(self):
        tables = self.sql_helper.get_sorted_tables()
        assert len(self._tables) == len(tables)
        return tables

    @staticmethod
    def _find_name_and_sql_paths(name):
        name_and_sql_paths = {}
        for basedir, dirs, filenames in os.walk(name):
            for filename in filenames:
                root, ext = os.path.splitext(filename)
                if ext == '.sql':
                    name_and_sql_paths[root] = os.path.join(basedir, filename)
        return name_and_sql_paths

    @convert_table_name
    def count(self, name):
        # support counting `.sql` result
        if name in self.name_and_sql_paths:
            sql = self._get_sql(name)
            return self.engine.scalar(sqlalchemy.text("select count(*) from ({}) T".format(sql)))
        return self.engine.scalar(sqlalchemy.text("select count(*) from {}".format(name)))

    @convert_table_name
    def get(self, name, **config):
        """extract table from rdb"""
        sql = self._get_sql(name)
        if sql is None:
            sql = "select * from {}".format(name)
        elif sql.strip() == '':
            raise ValueError('{}.sql is empty!'.format(name))
        else:
            if self.create_view:
                view_name = self.view_prefix + '_' + name if self.view_prefix else name
                self.sql_helper.create_view(view_name, sql)
                self.created_views.append(view_name)
        data = self._read_sql(sql, **config)
        if config.get('chunk_size'):
            return DFIteratorAdapter(data)
        else:
            return DFIteratorAdapter([data])

    def put(self, name, data, **config):
        try:
            name = self._convert_table_name(name)
        except ValueError:
            # for not existed table
            name = name
        self._to_sql(name, data, **config)
        self._tables.add(name)
        self._update_case_insensitive_names()

    def _get_sql(self, name):
        if name.endswith('.sql'):
            name = name[:-4]

        if name in self.name_and_sql_paths:
            with open(self.name_and_sql_paths[name], 'rb') as fo:
                return fo.read().decode('utf-8')

    def _read_sql(self, sql, **config):
        config = rename_chunk_size(config)
        if config.get('chunksize', None):
            return pandas.read_sql(sql, self.engine, **config)
        return [pandas.read_sql(sql, self.engine, **config), ]

    def _to_sql(self, name, data, **config):
        config = rename_chunk_size(config)
        data.to_sql(name=name, con=self.engine, **config)

    def execute(self, name):
        sql = self._get_sql(name)
        if sql:
            self._execute_sql(sql)
        else:
            raise ValueError

    def _execute_sql(self, sql):
        return self.engine.execute(sqlalchemy.text(sql))

    def truncate(self, names):
        converted_names = []
        for name in names:
            try:
                name = self._convert_table_name(name)
            except ValueError:
                # the table not exist in store
                pass
            else:
                converted_names.append(name)
        names = list(filter(lambda name: name in self.materialized_tables, converted_names))
        if names:
            logger.info('Truncate table in {}: {}'.format(self.name, ', '.join(names)))
            self.sql_helper.truncate(names)

    def drop_created_views(self):
        for name in self.created_views:
            self.sql_helper.drop_view(name)

    @convert_table_name
    def load(self, name, path, **config):
        sql = """
        LOAD DATA LOCAL INFILE '{path}'
        INTO TABLE {name}
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\r\n';
        """
        self._execute_sql(sql.format(path=path, name=name))

    @convert_table_name
    def dependency(self, name):
        return self.sql_helper.dependency(name)


class CSV(Store):
    def __init__(self, name, folder=None, tables=None):
        if folder is None:
            if not os.path.exists(name):
                os.mkdir(name)
            folder = './' + name
        self.folder = folder

        if tables is None:
            files = os.listdir(folder)
            tables = []
            for file_ in files:
                filename, ext = os.path.splitext(file_)
                if ext == '.csv':
                    tables.append(filename)

        super(CSV, self).__init__(name, tables)

    @property
    def ordered_tables(self):
        return self._tables

    @convert_table_name
    def count(self, name):
        raise NotImplementedError

    @convert_table_name
    def get(self, name, **config):
        data = self._read_csv(name, config)
        if config.get('chunksize'):
            return DFIteratorAdapter(data)
        else:
            return DFIteratorAdapter([data])

    def put(self, name, data, **config):
        path = self.get_path(name)

        data.to_csv(path, **config)

        self._tables.add(name)
        self._update_case_insensitive_names()

    def truncate(self, names):
        logger.info('Truncate table in {}: {}'.format(self.name, ', '.join(names)))
        for name in names:
            try:
                path = self.get_path(name)
            except ValueError:
                continue
            with open(path, 'w'):
                pass

    def get_path(self, name):
        try:
            name = self._convert_table_name(name)
        except ValueError:
            # for not existed table
            name = name
        return os.path.join(self.folder, name + '.csv')

    def _read_csv(self, name, config):
        config = rename_chunk_size(config)
        path = self.get_path(name)
        return pandas.read_csv(path, **config)

    def drop_created_views(self):
        pass
