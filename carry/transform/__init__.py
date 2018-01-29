from __future__ import unicode_literals

import time


class NoResultFound(Exception):
    pass


class Cursor(object):
    def __init__(self, data, fetch_callback=None, header=None):
        self._data = data
        self._iterator = iter(self)
        self._fetch_callback = fetch_callback
        self._header = header

    def fetch(self):
        """return the next row of table,if don't have next row,
        raise `NoResultFoundError` """
        try:
            return self._iterator.next()
        except StopIteration:
            raise NoResultFound

    def __iter__(self):
        for data_ in self._data:
            if isinstance(self._header, dict):
                data_.filter_fields(self._header.keys())
                data_.rename_fields(self._header)
            elif isinstance(self._header, (tuple, list)):
                data_.filter_fields(self._header)
            num = 0
            for row in data_:
                num += 1
                yield row
            if self._fetch_callback:
                self._fetch_callback(num)


class Dest(object):
    def __init__(self, chunk_size, shared):
        self._chunk_size = chunk_size
        self._data = []

        self.shared = shared

    def insert(self, *row):
        if not row:
            raise ValueError
        for r in row:
            self._data.append(r)
            if len(self._data) == self._chunk_size:
                self.commit()

    def commit(self):
        shared = self.shared
        queue = shared['queue']
        condition = shared['condition']
        max_queue_size = shared['max_queue_size']
        if self._data:
            data = type(self._data[0]).concat(self._data)

            condition.acquire()
            if len(queue) == max_queue_size:
                condition.wait()
            queue.append(data)

            condition.notify()
            condition.release()
            time.sleep(0.01)

            self._data = []


if __name__ == '__main__':
    cursor = Cursor([[1, 2], [3, 4], [5]])
    assert cursor.fetch() == 1
    assert cursor.fetch() == 2
    assert cursor.fetch() == 3
    assert cursor.fetch() == 4
    assert cursor.fetch() == 5
    try:
        cursor.fetch()
    except NoResultFound:
        pass
    else:
        assert False
