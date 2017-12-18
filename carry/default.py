import csv

from carry.utils import DefaultDict


class RDBGetConfig(DefaultDict):
    default = {
        'chunk_size': 10000
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
    pass


class CSVPutConfig(DefaultDict):
    default = {
        'encoding': 'utf-8',
        'index': False,
        'header': False,
        'mode': 'a',

        'na_rep': r'NULL',
        'sep': ',',
        'line_terminator': '\r\n',
        'escapechar': '',
        'quoting': csv.QUOTE_MINIMAL
    }


if __name__ == '__main__':
    RDBPutConfig({12: 2})
    RDBPutConfig({'index': 3})
    RDBPutConfig({})
    RDBPutConfig()
