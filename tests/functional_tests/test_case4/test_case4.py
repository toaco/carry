# -*- coding: utf-8 -*-
import datetime

from sqlalchemy.engine import reflection

import carry
from tests import users
from tests.functional_tests import db1, db2
from tests.utlis import chdir


def test():
    # Arrange
    users.create(db1.engine)
    users.create(db2.engine)
    db1.engine.execute(
        users.insert(),
        [
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()}
        ]
    )

    # Act
    def Assert():
        inspector = reflection.Inspector.from_engine(db1.engine)
        assert 'users_view' in inspector.get_view_names()

    config = {
        'STORES': [
            {
                'name': 'db1',
                'url': db1.url,
                'create_view': True,
            },
            {
                'name': 'db2',
                'url': db2.url,
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
                    'users_view',
                    carry.py(Assert, dependency=['users_view'])
                ],
            }
        ]
    }
    with chdir(__file__):
        carry.run(config)

    # Teardown
    db2.engine.execute('DROP TABLE users_view')
