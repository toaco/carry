from tqdm import tqdm

from carry import logger
from carry.default import RDBGetConfig, RDBPutConfig, CSVPutConfig, CSVGetConfig, RDBLoadConfig
from carry.store import RDB, CSV
from carry.transform import Dest, Cursor, NoResultFound


class TaskClassifier(object):
    def __init__(self, tasks):
        self.tasks = tasks

    def effected_tables(self):
        tables = []
        for task in self.tasks:
            if isinstance(task, (list, tuple)):
                task, _ = task
                if '.' not in task:
                    tables.append(task)
                elif '.sql' in task:
                    ets = _
                    if not ets:
                        pass
                    elif isinstance(ets, (str, unicode)):
                        tables.append(task.split('.sql')[1])
                    elif isinstance(ets, (list, tuple)):
                        tables.extend(ets)
                    else:
                        raise NotImplementedError
                else:
                    raise NotImplementedError
            else:
                if '.' not in task:
                    tables.append(task)
                elif '.sql' in task:
                    tables.append(task.split('.sql')[0])
                else:
                    raise NotImplementedError
        return tables


class TaskFactory(object):
    @classmethod
    def create(cls, stores, sources, dest, subtask):
        if isinstance(subtask, (list, tuple)):
            subtask, callback = subtask
        else:
            callback = None
        if '.' not in subtask:
            return cls._create_table_task(stores, sources, dest, subtask, callback)
        elif '.*' in subtask:
            store_name, _ = subtask.split('.*')
            source_store = stores.find_by_store_name(store_name)

            tc = TaskCollection()
            for source in sources:
                if source_store.name == source['name']:
                    sources = [source]
                    break
            for table in source_store.ordered_tables:
                tc.add(cls._create_table_task(stores, sources, dest, table))
            return tc
        elif '.sql' in subtask:
            name, _ = subtask.split('.sql')
            dest_store = stores.find_by_store_name(dest['name'])
            return SQLTask(dest_store, name)
        else:
            raise NotImplementedError

    @classmethod
    def _create_table_task(cls, stores, sources, dest, subtask, callback=None):
        source_store = stores.find_by_table_name(
            subtask, store_name_limits=[source['name'] for source in sources])
        if not source_store:
            raise ValueError

        for source in sources:
            if source_store.name == source['name']:
                source = source.copy()
                source.pop('name')
                get_config = source
                break
        else:
            raise ValueError('Unknown table name {}'.format(subtask))

        dest = dest.copy()
        dest_store = stores.find_by_store_name(dest.pop('name'))
        put_config = dest

        if isinstance(source_store, RDB) and isinstance(dest_store, RDB):
            get_config = RDBGetConfig(get_config)
            put_config = RDBPutConfig(put_config)
            return RDBToRDBTask(source_store, dest_store, subtask, get_config, put_config, callback)
        elif isinstance(source_store, RDB) and isinstance(dest_store, CSV):
            get_config = RDBGetConfig(get_config)
            put_config = CSVPutConfig(put_config)
            return RDBToCSVTask(source_store, dest_store, subtask, get_config, put_config, callback)
        elif isinstance(source_store, CSV) and isinstance(dest_store, RDB):
            get_config = CSVGetConfig(get_config)
            put_config = RDBLoadConfig(put_config)
            return CSVToRDBTask(source_store, dest_store, subtask, get_config, put_config)
        else:
            raise NotImplementedError


class Task(object):
    def execute(self):
        raise NotImplementedError


class TaskCollection(Task):
    def __init__(self, tasks=None):
        self.tasks = tasks or []

    def execute(self):
        for task in self.tasks:
            task.execute()

    def add(self, task):
        self.tasks.append(task)


def transfer_log(func):
    def _wrapper(self, *args, **kw):
        logger.info('Transfer table: {from_}.{name} -> {into}.{name}'.format(
            from_=self.source.name, into=self.dest.name, name=self.table)
        )
        return func(self, *args, **kw)

    return _wrapper


class RDBToRDBTask(Task):
    def __init__(self, source, dest, table, get_config, put_config, callback=None):
        self.source = source
        self.dest = dest
        self.get_config = get_config
        self.put_config = put_config
        self.table = table
        self._callback = callback

    @transfer_log
    def execute(self):
        data = self.source.get(self.table, **self.get_config)
        count = self.source.count(self.table)
        with tqdm(total=count, unit='rows') as bar:
            if self._callback:
                cursor = Cursor(data, fetch_callback=bar.update)
                chunk_size = self.get_config.get('chunk_size')
                dest = Dest(self.dest, self.table, chunk_size, put_config=self.put_config)
                try:
                    self._callback(cursor, dest)
                except NoResultFound:
                    pass
                finally:
                    dest.commit()
            else:
                for i, data_ in enumerate(data):
                    self.dest.put(self.table, data_, **self.put_config)
                    bar.update(len(data_))


class RDBToCSVTask(RDBToRDBTask):
    pass


class CSVToRDBTask(Task):
    def __init__(self, source, dest, table, get_config, put_config):
        self.source = source
        self.dest = dest
        self.get_config = get_config
        self.put_config = put_config
        self.table = table

    @transfer_log
    def execute(self):
        path = self.source.get_path(self.table)
        self.dest.load(self.table, path, **self.put_config)


class SQLTask(Task):
    def __init__(self, store, name):
        self.store = store
        self.name = name

    def execute(self):
        logger.info('Execute SQL script in {store}: {name}.sql'.format(
            store=self.store.name, name=self.name)
        )
        self.store.execute(self.name)


class PythonTask(Task):
    def __init__(self, callable_):
        """python callable object"""
        self.callable = callable_

    def execute(self):
        self.callable()
