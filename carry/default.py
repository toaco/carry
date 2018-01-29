import csv

from carry.utils import DefaultDict


class RDBGetConfig(DefaultDict):
    default = {
        'chunk_size': 10000,
        'coerce_float': False,
    }


class RDBPutConfig(DefaultDict):
    default = {
        'if_exists': 'append',
        'index': False,
        'chunk_size': 10000
    }


class RDBLoadConfig(DefaultDict):
    pass


class CSVGetConfig(DefaultDict):
    default = {
        'encoding': 'utf-8',
        'index_col': None,
        'header': 0,
        'chunksize': 10000,
        'dtype': str,
        'float_precision': 'round_trip',

        'na_values': r'NULL',
        'sep': ',',
        'lineterminator': '\n',
        'escapechar': None,
        'quoting': csv.QUOTE_MINIMAL
    }


class CSVPutConfig(DefaultDict):
    default = {
        'encoding': 'utf-8',
        'index': False,
        'header': True,
        'mode': 'a',
        'chunksize': 10000,

        'na_rep': r'NULL',
        'sep': ',',
        'escapechar': '',
        'quoting': csv.QUOTE_MINIMAL
    }


if __name__ == '__main__':
    RDBPutConfig({12: 2})
    RDBPutConfig({'index': 3})
    RDBPutConfig({})
    RDBPutConfig()
