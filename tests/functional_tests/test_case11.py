# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import func, select

import carry
from carry import exc
from tests import users, users2
from tests.functional_tests import db1, db2


def test():
    # Arrange
    users.create(db1.engine)
    users2.create(db2.engine)
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
                    carry.table('users2', header={
                        'name': 'name2',
                        'id': 'id',
                        'fullname': 'fullname2',
                        'reg_time': 'reg_time2',
                        'unknown': 'unknown',
                    }, source_name='users')
                ],
            }
        ]
    }
    carry.run(config)

    # Assert
    count = db2.engine.execute(select([func.count()]).select_from(users2)).scalar()
    assert count == 0

    assert exc.exceptions.size == 1
    assert isinstance(exc.exceptions[0], exc.NoSuchColumnError)
