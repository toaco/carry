import os

from tests import Databases

databases_dir = os.path.join(os.path.dirname(__file__), 'res')

databases = Databases({
    'db1': 'sqlite:///' + os.path.join(databases_dir, 'db1'),
    'db2': 'sqlite:///' + os.path.join(databases_dir, 'db2'),
    'db3': 'sqlite:///' + os.path.join(databases_dir, 'db3'),
})

db1 = databases.db1
db2 = databases.db2
db3 = databases.db3
