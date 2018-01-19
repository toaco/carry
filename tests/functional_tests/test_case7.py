# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import func, select

import carry
from tests import users
from tests.functional_tests import db1, db2


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
                    'UsErS'
                ],
            }
        ]
    }
    carry.run(config)

    # Assert
    count = db2.engine.execute(select([func.count()]).select_from(users)).scalar()
    assert count == 3
