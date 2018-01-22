import os
from Queue import Queue
from threading import Thread, Lock

from carry.logger import logger
from carry.task import TaskFactory
from carry.utils import topological_find


class ThreadPoolManger(object):
    def __init__(self, thread_num):
        self.work_queue = Queue()
        self.thread_num = thread_num
        self.__init_threading_pool(self.thread_num)

    def __init_threading_pool(self, thread_num):
        for i in range(thread_num):
            thread = ThreadManger(self.work_queue)
            thread.start()

    def add_job(self, func, *args):
        self.work_queue.put((func, args))


class ThreadManger(Thread):
    def __init__(self, work_queue):
        Thread.__init__(self)
        self.work_queue = work_queue
        self.daemon = True

    def run(self):
        while True:
            target, args = self.work_queue.get()
            try:
                target(*args)
            finally:
                self.work_queue.task_done()


class TaskDispatcher(object):
    def __init__(self, stores, task_config):
        task_factory = TaskFactory(stores, task_config)
        task_factory.create_all()
        self._tasks = task_factory.tasks
        self._dependency = task_factory.task_dependency
        self._published_tasks = set()

        et_num = len(self._executable_tasks())
        self.consumers_num = task_config.pop('consumers', 3)
        threads = task_config.pop('threads', et_num) * (self.consumers_num + 1)
        self._threads_pool = ThreadPoolManger(threads)
        self._lock = Lock()

    def dispatch(self):
        logger.info('Start transfer(PID: {})'.format(os.getpid()))
        self._publish()
        self._threads_pool.work_queue.join()
        print '\n' * (len(self._tasks) + 1)  # prevent progress bars

    def _publish(self):
        tasks = self._executable_tasks() - set(self._published_tasks)
        self._published_tasks = self._published_tasks.union(tasks)
        for task in tasks:
            task = self._tasks.get(task)
            task.execute(pool=self._threads_pool, watcher=self.notify,
                         consumers_num=self.consumers_num)

    def notify(self, task_id):
        with self._lock:
            self._finish(task_id)
            self._publish()

    def _executable_tasks(self):
        tasks = topological_find(self._dependency)
        return tasks

    def _finish(self, task_name):
        if task_name in self._dependency:
            del self._dependency[task_name]
        for value in self._dependency.values():
            if value and task_name in value:
                value.remove(task_name)
