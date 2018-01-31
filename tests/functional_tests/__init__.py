import os
import sys

import six

from tests import Databases


def join_path(*path):
    file_system_encoding = sys.getfilesystemencoding()
    joined_path = os.path.join(*path)
    if isinstance(joined_path, six.text_type):
        return joined_path
    elif isinstance(joined_path, six.binary_type):
        return joined_path.decode(file_system_encoding)


databases_dir = join_path(os.path.dirname(__file__), 'res')

databases = Databases({
    'db1': 'sqlite:///' + join_path(databases_dir, 'db1'),
    'db2': 'sqlite:///' + join_path(databases_dir, 'db2'),
    'db3': 'sqlite:///' + join_path(databases_dir, 'db3'),
})

db1 = databases.db1
db2 = databases.db2
db3 = databases.db3
