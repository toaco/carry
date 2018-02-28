from __future__ import unicode_literals

import time
from threading import Condition, Lock

import six
from tqdm import tqdm

from carry import exc
from carry.bar import MockProgressbar
from carry.default import RDBGetConfig, RDBPutConfig, CSVPutConfig, CSVGetConfig
from carry.exc import NoSuchTableError
from carry.exc import ProducerError, ConsumerError, CarryError
from carry.logger import logger
from carry.store import RDB, CSV
from carry.transform import Dest, Cursor, NoResultFound


class TableTaskConfig(object):
    def __init__(self, name, transformer=None, header=None, get_config=None, put_config=None,
                 mode=None, dependency=None, source_name=None, effects=None):
        self.name = name
        self.transformer = transformer
        self.header = header
        self.get_config = get_config
        self.put_config = put_config
        self.mode = mode
        self.dependency = dependency
        self.source_name = source_name or name
        self.effects = effects


class SQLTaskConfig(object):
    def __init__(self, name, dependency=None, effects=None):
        self.name = name
        self.dependency = dependency
        self.effects = effects


class PythonTaskConfig(object):
    def __init__(self, callable_, dependency=None, effects=None):
        self.callable_ = callable_
        self.dependency = dependency
        self.effects = effects


class TaskClassifier(object):
    def __init__(self, tasks):
        self.tasks = tasks

    def effected_tables(self,sourceName,stors):
        tables = []
        for task_config in self.tasks:
            if isinstance(task_config, (TableTaskConfig, SQLTaskConfig)):
                tables.append(task_config.name)
                if task_config.effects is not None:
                    tables.extend(task_config.effects)

            elif isinstance(task_config, (list, tuple)):
                task_config, _ = task_config
                tables.append(task_config)
            elif isinstance(task_config, six.string_types):
                if '.' not in task_config:
                    tables.append(task_config)
                elif '.sql' in task_config:
                    tables.append(task_config.split('.sql')[0])
                # TODO
                elif '.*' in task_config:
                    for stor in stors:
                        if sourceName == stor.name:
                            tbs = stor.materialized_tables
                            tables.append(tbs)
                else:
                    raise NotImplementedError
            elif isinstance(task_config, TableTaskConfig):
                tables.append(task_config.name)
            elif isinstance(task_config, PythonTaskConfig):
                if task_config.effects is not None:
                    tables.extend(task_config.effects)
            elif callable(task_config):
                pass
            else:
                raise NotImplementedError
        return tables


class TaskFactory(object):
    def __init__(self, stores, task_config):
        self.stores = stores
        self.task_config = task_config

        self.task_dependency = {}
        self.tasks = {}

    def create_all(self):
        task_config = self.task_config
        orders = task_config.pop('orders')

        for subtask_config in orders:
            subtask = self.create(self.stores, task_config, subtask_config)
            if isinstance(subtask, list):
                for st in subtask:
                    self.tasks[st.name] = st
                    self.task_dependency[st.name] = st.dependency
            else:
                self.tasks[subtask.name] = subtask
                self.task_dependency[subtask.name] = subtask.dependency

        source_tables = dict((task.source_table_name, task.name) for task in self.tasks.values()
                             if isinstance(task, RDBToCSVTask))
        for name, dependency in self.task_dependency.items():
            if dependency:
                continue

            task = self.tasks[name]
            if isinstance(task, RDBToCSVTask):
                try:
                    source_table_dependency = task.source.dependency(task.source_table_name)
                except ValueError:
                    # table not in the source database
                    continue
                else:
                    for d in source_table_dependency:
                        if d in source_tables:
                            dependency.append(source_tables[d])
            elif isinstance(task, (RDBToRDBTask, CSVToRDBTask)):
                try:
                    target_table_dependency = task.dest.dependency(name)
                except ValueError:
                    # table not in the dest database
                    continue
                except NoSuchTableError:
                    continue
                else:
                    for d in target_table_dependency:
                        if d in self.tasks:
                            dependency.append(d)

    @classmethod
    def create(cls, stores, task_config, subtask_config):
        sources = task_config.get('from')
        dest = task_config.get('to')

        # table task
        if isinstance(subtask_config, TableTaskConfig):
            return cls._create_table_task(
                stores, sources, dest, subtask_config.name, subtask_config.transformer, header=subtask_config.header,
                get_config=subtask_config.get_config, put_config=subtask_config.put_config, mode=subtask_config.mode,
                dependency=subtask_config.dependency, source_name=subtask_config.source_name
            )

        # table task: list or tuple
        if isinstance(subtask_config, (list, tuple)):
            subtask_config, transformer = subtask_config
        else:
            transformer = None

        # sql task
        if isinstance(subtask_config, SQLTaskConfig):
            dest_store = stores.find_by_store_name(dest['name'])
            return SQLTask(dest_store, subtask_config.name, subtask_config.dependency)

        # python task
        if callable(subtask_config):
            return PythonTask(subtask_config)
        if isinstance(subtask_config, PythonTaskConfig):
            return PythonTask(subtask_config.callable_, subtask_config.dependency)

        # string
        if '.' not in subtask_config:
            # table task
            return cls._create_table_task(stores, sources, dest, subtask_config, transformer)
        elif '.*' in subtask_config:
            # multiple table tasks
            store_name, _ = subtask_config.split('.*')
            source_store = stores.find_by_store_name(store_name)

            tasks = []
            for source in sources:
                if source_store.name == source['name']:
                    sources = [source]
                    break
            for table in source_store.ordered_tables:
                tasks.append(cls._create_table_task(stores, sources, dest, table))
            return tasks
        elif '.sql' in subtask_config:
            # sql task
            name, _ = subtask_config.split('.sql')
            dest_store = stores.find_by_store_name(dest['name'])
            return SQLTask(dest_store, name)
        else:
            raise NotImplementedError

    @classmethod
    def _create_table_task(cls, stores, sources, dest, table_name, transformer=None, header=None,
                           get_config=None, put_config=None, mode=None, dependency=None, source_name=None):

        source_name = source_name or table_name

        # find source store
        source_store = stores.find_by_table_name(
            source_name, store_name_limits=[source['name'] for source in sources])
        if not source_store:
            raise exc.NoSuchTableError(source_name)

        # get `get_config`
        if get_config is None:
            get_config = {}
        for source in sources:
            if source_store.name == source['name']:
                source = source.copy()
                source.pop('name')
                get_config.update(source)
                break
        else:
            raise exc.NoSuchTableError(source_name)

        # find dest_store
        dest = dest.copy()
        dest_store = stores.find_by_store_name(dest.pop('name'))

        # get `put_config`
        if put_config is None:
            put_config = {}
        put_config.update(dest)

        # create task_config
        if isinstance(source_store, RDB) and isinstance(dest_store, RDB):
            get_config = RDBGetConfig(get_config)
            put_config = RDBPutConfig(put_config)
            return RDBToRDBTask(source_store, dest_store, table_name, get_config, put_config,
                                transformer, header, dependency, source_name)
        elif isinstance(source_store, RDB) and isinstance(dest_store, CSV):
            get_config = RDBGetConfig(get_config)
            put_config = CSVPutConfig(put_config)
            return RDBToCSVTask(source_store, dest_store, table_name, get_config, put_config,
                                transformer, header, dependency, source_name)
        elif isinstance(source_store, CSV) and isinstance(dest_store, RDB):
            get_config = CSVGetConfig(get_config)
            put_config = RDBPutConfig(put_config)
            return CSVToRDBTask(source_store, dest_store, table_name, get_config, put_config,
                                transformer, header, dependency, source_name)
        else:
            raise NotImplementedError


class Task(object):
    progress_bar_lock = Lock()
    bar_id = 1  # if zero,tqdm may create an emtpy line after first bar

    def __init__(self, name, dependency=None):
        self.name = name
        self.dependency = dependency or []

    def execute(self, pool=None, watcher=None, consumers_num=3):
        raise NotImplementedError


class RDBToRDBTask(Task):
    def __init__(self, source, dest, table, get_config, put_config, transformer=None, header=None, dependency=None,
                 source_table_name=None):
        super(RDBToRDBTask, self).__init__(table, dependency)
        self.source = source
        self.dest = dest
        self.get_config = get_config
        self.put_config = put_config
        self.table = table
        self.source_table_name = source_table_name
        self.transformer = transformer
        self.header = header

        self.shared = {
            'queue': [],
            'max_queue_size': 10,
            'condition': Condition(),
            'task_done': False
        }
        self.task_done = False

        self._consumers_num = None
        self._finished_consumers_num = 0
        self._consumer_died = False
        self._producer_died = False
        self._finished_lock = Lock()

    def execute(self, pool=None, watcher=None, consumers_num=3):
        self._consumers_num = consumers_num

        def logger(func_name, thread_id):
            def _logger(msg):
                # print '\n{}-{}-{}: {}\n'.format(self.table, func_name, thread_id, msg),
                pass

            return _logger

        pool.add_job(self._get_data, True, logger('get_data', ''))
        for i in range(consumers_num):
            pool.add_job(self._put_data, watcher, logger('put_data', i + 1))

    def _get_data(self, display_bar=True, logger=None):
        try:
            try:
                data = self.source.get(self.source_table_name, **self.get_config)
            except Exception:
                print('Error occurred when get data from {}!'.format(self.source_table_name))
                raise
            if display_bar:
                count = self.source.count(self.source_table_name)
                with self.progress_bar_lock:
                    bar = tqdm(total=count, unit='rows', position=Task.bar_id,
                               desc='Transfer table {name}'.format(name=self.table))
                    Task.bar_id += 1
            else:
                bar = MockProgressbar(desc='Transfer table {name}'.format(name=self.table))

            if self.transformer:
                self._transform(bar, data)
            else:
                self._put_into_buffer_directly(bar, data, logger)
        except Exception as e:
            condition = self.shared['condition']
            condition.acquire()
            self.task_done = True
            self._producer_died = True
            condition.notify()
            condition.release()
            if isinstance(e, CarryError):
                raise e
            else:
                raise ProducerError(e)

    def _transform(self, bar, data):
        cursor = Cursor(data, fetch_callback=bar.update, header=self.header)
        dest = Dest(self.put_config.get('chunk_size', 0), self.shared)
        try:
            self.transformer(cursor, dest)
        except NoResultFound:
            pass
        finally:
            dest.commit()
            self.task_done = True

    def _put_into_buffer_directly(self, bar, data, logger):

        condition = self.shared['condition']

        for i, d in enumerate(data):
            if isinstance(self.header, dict):
                d.filter_fields(self.header.keys())
                d.rename_fields(self.header)
            elif isinstance(self.header, (tuple, list)):
                d.filter_fields(self.header)

            queue = self.shared['queue']
            max_queue_size = self.shared['max_queue_size']

            logger('before acquire')
            condition.acquire()
            logger('after acquire')

            if self._consumer_died:
                return

            if len(queue) == max_queue_size:
                logger('reach the size limit')
                condition.wait()
                logger('reacquire the lock')

                if self._consumer_died:
                    return

            queue.append(d)

            logger('before notify and release')
            condition.notify()
            condition.release()
            logger('after notify and release')
            time.sleep(0.001)

            bar.update(len(d))
        else:
            logger('set task_done to True ')
            self.task_done = True
            condition.acquire()
            condition.notify()
            condition.release()

    def _put_data(self, watcher, logger):
        try:
            queue = self.shared['queue']
            condition = self.shared['condition']

            while 1:
                logger('before acquire')
                condition.acquire()
                logger('after acquire')

                if self._consumer_died:
                    return

                flag = 0
                while 1:
                    if not queue:
                        if self.task_done:
                            logger('before notify and release')
                            condition.notify()
                            condition.release()
                            logger('after notify and release')
                            flag = 1
                            break
                        else:
                            logger('queue is empty')
                            condition.wait()
                            logger('reacquire the lock')

                            if self._consumer_died:
                                return

                            flag = 2
                    else:
                        break
                if flag == 1:
                    break
                elif flag == 2 and self.task_done and not queue:
                    logger('before notify and release')
                    condition.notify()
                    condition.release()
                    logger('after notify and release')
                    break

                data = queue.pop(0)

                logger('before notify and release')
                condition.notify()
                condition.release()
                logger('after notify and release')

                time.sleep(0.001)

                error_count = 0
                while 1:
                    try:
                        self.dest.put(self.table, data, **self.put_config)
                        break
                    except Exception as e:
                        if error_count == 10:
                            raise e
                        else:
                            error_count += 1
                            logger(e)
                            continue
        except Exception as e:
            self._consumer_died = True
            raise ConsumerError(e)
        finally:
            self._finished(watcher)

    def _finished(self, watcher):
        with self._finished_lock:
            self._finished_consumers_num += 1
        if self._finished_consumers_num == self._consumers_num:
            watcher(self.name, self.task_done and not self._consumer_died and not self._producer_died)


class RDBToCSVTask(RDBToRDBTask):
    pass


class CSVToRDBTask(RDBToRDBTask):
    def _get_data(self, display_bar=False, logger=None):
        super(CSVToRDBTask, self)._get_data(display_bar=False, logger=logger)


class SQLTask(Task):
    def __init__(self, store, name, dependency=None):
        super(SQLTask, self).__init__(name, dependency)
        self.store = store

    def execute(self, pool=None, watcher=None, consumers_num=3):
        logger.info('Execute SQL script in {store}: {name}'.format(
            store=self.store.name, name=self.name)
        )
        self.store.execute(self.name)
        watcher(self.name)


class PythonTask(Task):
    def __init__(self, callable_, dependency=None):
        """python callable object"""
        super(PythonTask, self).__init__(callable_, dependency)
        self.callable = callable_

    def execute(self, pool=None, watcher=None, consumers_num=3):
        self.callable()
