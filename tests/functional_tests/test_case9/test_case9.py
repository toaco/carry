# -*- coding: utf-8 -*-

from sqlalchemy import select, func

import carry
from carry import exc
from tests import users
from tests.functional_tests import db2
from tests.utlis import chdir


def test():
    # Arrange
    users.create(db2.engine)

    # Act
    config = {
        'STORES': [
            {
                'name': 'db1',
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
                    'users'
                ],
            }
        ]
    }
    with chdir(__file__):
        carry.run(config)

    # Assert
    count = db2.engine.execute(select([func.count()]).select_from(users)).scalar()
    assert count == 0

    assert exc.exceptions.size == 1
    assert isinstance(exc.exceptions[0], exc.ConsumerError)
