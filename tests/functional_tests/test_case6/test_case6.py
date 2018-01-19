# -*- coding: utf-8 -*-
import datetime
import os

import carry
from tests import users
from tests.functional_tests import db1
from tests.utlis import chdir


def test():
    # Arrange
    users.create(db1.engine)
    db1.engine.execute(
        users.insert(),
        [
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()}
        ]
    )

    # Act
    config = {
        'STORES': [
            {
                'name': 'db1',
                'url': db1.url,
            },
            {
                'name': 'db2',
            }
        ],
        'TASKS': [
            {
                'from': [{
                    'name': 'db1'
                }],
                'to': {
                    'name': 'db2',
                },
                'orders': [
                    'users'
                ],
            }
        ]
    }
    with chdir(__file__):
        carry.run(config)

    # Assert
    with chdir(__file__):
        csv_dir = os.path.join('.', 'db2')
        csv_file = os.path.join(csv_dir, 'users.csv')
        with open(csv_file, 'rb') as fo:
            assert fo.read().decode('utf-8').count('\n') == 4
        os.remove(csv_file)
        os.rmdir(csv_dir)
