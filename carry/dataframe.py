import pandas


class DFIteratorAdapter(object):
    def __init__(self, data):
        self._data = data

    def __iter__(self):
        for chunk in self._data:
            yield DFAdapter(chunk)


class DFAdapter(object):
    """pandas DataFrame adapter"""

    def __init__(self, df):
        self.df = df

    def __iter__(self):
        # returned row was a copy of row in source
        for row in self.df.iterrows():
            yield DFRowAdapter(row[1])

    def __len__(self):
        return self.df.shape[0]

    def to_sql(self, *args, **kwargs):
        return self.df.to_sql(*args, **kwargs)


class DFRowAdapter(object):
    """adapter for row in pandas DataFrame"""

    def __init__(self, row):
        self.__dict__['_row'] = row

    @classmethod
    def concat(cls, rows):
        # https://stackoverflow.com/questions/21004993/pandas-concat-series-to-df-as-rows
        return pandas.concat(list(map(lambda x: x['_row'], rows)), axis=1).T

    def __getitem__(self, key):
        if key == '_row':
            return self.__dict__['_row']
        return self.__dict__['_row'][key]

    def __setitem__(self, key, value):
        self.__dict__['_row'][key] = value

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        return self[key]

    def copy(self):
        return DFRowAdapter(self.__dict__['_row'].copy(deep=True))
