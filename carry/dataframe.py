from __future__ import unicode_literals

import pandas

from carry import exc


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

    def to_csv(self, *args, **kwargs):
        return self.df.to_csv(*args, **kwargs)

    def _check_columns(self, columns):
        df_columns = set(self.df.columns.values)
        s = set(columns) - df_columns
        if s:
            raise exc.NoSuchColumnsError(s)

    def filter_fields(self, header):
        self._check_columns(header)
        df_columns = set(self.df.columns.values)
        self.df = self.df.drop(columns=df_columns - set(header))

    def rename_fields(self, mapper):
        self._check_columns(mapper.keys())
        self.df = self.df.rename(index=str, columns=mapper)


class DFRowAdapter(object):
    """adapter for row in pandas DataFrame"""
    case_sensitive = False

    def __init__(self, row):
        self.__dict__['_row'] = row
        self.__dict__['_case_insensitive_names'] = {}
        for field in row.index.values.tolist():
            self.__dict__['_case_insensitive_names'][field.lower()] = field

    @classmethod
    def concat(cls, rows):
        # https://stackoverflow.com/questions/21004993/pandas-concat-series-to-df-as-rows
        return pandas.concat(list(map(lambda x: x['_row'], rows)), axis=1).T

    def __getitem__(self, key):
        if key == '_row':
            return self.__dict__['_row']
        if not self.case_sensitive:
            key = self.__dict__['_case_insensitive_names'][key.lower()]
        return self.__dict__['_row'][key]

    def __setitem__(self, key, value):
        if not self.case_sensitive:
            key = self.__dict__['_case_insensitive_names'][key.lower()]
        self.__dict__['_row'][key] = value

    def __delitem__(self, key):
        if not self.case_sensitive:
            key = self.__dict__['_case_insensitive_names'][key.lower()]
        self.__dict__['_row'] = self.__dict__['_row'].drop([key])

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, key):
        return self[key]

    def __delattr__(self, key):
        del self[key]

    def copy(self):
        return DFRowAdapter(self.__dict__['_row'].copy(deep=True))
