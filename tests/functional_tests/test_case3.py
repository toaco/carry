# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import func, select

import carry
from tests import addresses, users
from tests.functional_tests import db1, db2, db3


def test(drop123):
    # Arrange
    users.create(db1.engine)
    addresses.create(db2.engine)
    db1.engine.execute(
        users.insert(),
        [
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()},
            {'name': 'wendy', 'fullname': 'Wendy Williams', 'reg_time': datetime.datetime.now()}
        ]
    )
    db2.engine.execute(
        addresses.insert(),
        [
            {'user_id': 1, 'email_address': '123@mail.com'},
            {'user_id': 2, 'email_address': '234@mail.com'}
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
            },
            {
                'name': 'db3',
                'url': db3.url,
            }
        ],
        'TASKS': [
            {
                'from': [
                    {
                        'name': 'db1'
                    },
                    {
                        'name': 'db2'
                    },
                ],
                'to': {
                    'name': 'db3',
                },
                'orders': [
                    'users',
                    'addresses',
                ],
            }
        ]
    }
    carry.run(config)

    # Assert
    count = db3.engine.execute(select([func.count()]).select_from(users)).scalar()
    assert count == 3

    count = db3.engine.execute(select([func.count()]).select_from(addresses)).scalar()
    assert count == 2
