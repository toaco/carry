from __future__ import unicode_literals

import copy
import imp

import six

from carry import exc
from carry.dispatcher import TaskDispatcher
from carry.logger import logger
from carry.store import StoreFactory
from carry.task import TaskFactory, TaskClassifier, TableTaskConfig, SQLTaskConfig, PythonTaskConfig
from carry.transform import NoResultFound
from carry.version import __version__

table = TableTaskConfig
sql = SQLTaskConfig
py = PythonTaskConfig


def run(config, task_ids=None):
    try:
        if isinstance(config, six.string_types):
            config = imp.load_source('carry.config', config)
            etl = Carry(config.STORES)
            etl.execute(config.TASKS, task_ids)
        elif isinstance(config, dict):
            config = copy.deepcopy(config)
            etl = Carry(config['STORES'])
            etl.execute(config['TASKS'], task_ids)
    except Exception as e:
        exc.exceptions.add(e)
        raise


class Carry(object):
    def __init__(self, store_configs):
        self.stores = StoreFactory.create_all(store_configs)

    def execute(self, tasks, task_ids):
        task_ids = set(task_ids or ())
        for i, task in enumerate(tasks):
            if task_ids and i not in task_ids:
                continue

            self._execute_task(i, task)
            logger.info('Finish task  {}'.format(i))

        self.stores.drop_created_views()

    def _execute_task(self, num, task):
        sources = task.get('from')
        dest = task.get('to')
        orders = task.get('orders')
        sourceName =''
        for source in sources:
            sourceName=source['name']
        logger.info('Start task {}: Transfer tables from {} to {}'.format(
            num, [source['name'] for source in sources], dest['name'])
        )

        # truncate
        tc = TaskClassifier(orders)
        effected_tables = tc.effected_tables(sourceName,self.stores.stores)
        dest_store = self.stores.find_by_store_name(dest['name'])
        dest_store.truncate(effected_tables)

        task_dispatcher = TaskDispatcher(self.stores, task)
        task_dispatcher.dispatch()
