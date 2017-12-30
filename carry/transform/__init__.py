class NoResultFound(Exception):
    pass


class Cursor(object):
    def __init__(self, data, fetch_callback=None):
        self._data = data
        self._iterator = iter(self)
        self._fetch_callback = fetch_callback

    def fetch(self):
        """return the next row of table,if don't have next row,
        raise `NoResultFoundError` """
        try:
            return self._iterator.next()
        except StopIteration:
            raise NoResultFound

    def __iter__(self):
        for chunk in self._data:
            num = 0
            for row in chunk:
                num += 1
                yield row
            self._fetch_callback(num)


class Dest(object):
    def __init__(self, dest_store, table, chunk_size, put_config):
        self._dest_store = dest_store
        self._table = table
        self._chunk_size = chunk_size
        self._put_config = put_config
        self._data = []

    def insert(self, row):
        if not row:
            raise ValueError
        self._data.append(row)
        if len(self._data) == self._chunk_size:
            self.commit()

    def commit(self):
        if self._data:
            data = type(self._data[0]).concat(self._data)
            self._dest_store.put(self._table, data, **self._put_config)
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
    except LookupError:
        pass
    else:
        assert False
