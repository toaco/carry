# -*- coding: utf-8 -*-
import datetime

from sqlalchemy.engine import reflection

import carry
from tests import users, truncate_TableTest
from tests.functional_tests import db1, db2
from tests.utlis import chdir


def test():
    # Arrange
    truncate_TableTest.create(db1.engine)
    truncate_TableTest.create(db2.engine)
    db1.engine.execute(
        truncate_TableTest.insert(),
        [
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()}
        ]
    )
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
        assert 'truncate_TableTest' in inspector.get_view_names()

    config = {
        'STORES': [
            {
                'name': 'db1',
                'url': db1.url,
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
                    'truncate_TableTest',
                    carry.py(Assert, dependency=['truncate_TableTest'])
                ],
            }
        ]
    }
    with chdir(__file__):
        carry.run(config)

    # Teardown
    db2.engine.execute('DROP TABLE truncate_TableTest')
