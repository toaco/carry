from threading import RLock


class CarryError(Exception):
    pass


class NoSuchTableError(CarryError):
    def __init__(self, table_name):
        self.message = 'Can not find the table: ' + table_name


class ProducerError(CarryError):
    pass


class ConsumerError(CarryError):
    pass


class NoSuchColumnsError(CarryError):
    def __init__(self, columns):
        self.message = 'Can not find those columns: ' + ', '.join(columns)


class ExceptionHistory(object):
    def __init__(self):
        self.exceptions = []
        self.lock = RLock()

    @property
    def size(self):
        return len(self.exceptions)

    def add(self, exc):
        with self.lock:
            self.exceptions.append(exc)

    def clear(self):
        self.exceptions = []

    def __getitem__(self, item):
        return self.exceptions[item]


exceptions = ExceptionHistory()
