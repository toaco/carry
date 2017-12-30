import imp

from carry.logger import logger
from carry.store import StoreFactory
from carry.task import TaskFactory, TaskClassifier
from carry.transform import NoResultFound

__version__ = '0.1'


def run(config, task_ids=None):
    if isinstance(config, str):
        config = imp.load_source('carry.config', config)

    etl = Carry(config.STORES)
    etl.execute(config.TASKS, task_ids)


class Carry(object):
    def __init__(self, store_configs):
        self.stores = StoreFactory.create_all(store_configs)

    def execute(self, tasks, task_ids):
        task_ids = set(task_ids or ())
        for i, task in enumerate(tasks):
            if task_ids and i not in task_ids:
                continue
            sources = task.get('from')
            dest = task.get('to')
            orders = task.pop('orders')

            logger.info('Start task {}: Transfer tables from {} to {}'.format(
                i, [source['name'] for source in sources], dest['name'])
            )
            self._execute_task(sources, dest, orders)
            logger.info('Finish task {}'.format(
                i, [source['name'] for source in sources], dest['name'])
            )

    def _execute_task(self, sources, dest, orders):
        # truncate
        tc = TaskClassifier(orders)
        effected_tables = tc.effected_tables()
        dest_store = self.stores.find_by_store_name(dest['name'])
        dest_store.truncate(effected_tables)

        # task
        for subtask in orders:
            subtask = TaskFactory.create(self.stores, sources, dest, subtask)
            subtask.execute()
