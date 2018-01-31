# -*- coding: utf-8 -*-
import pytest
from sqlalchemy import func, select

import carry
from carry import exc
from tests import users
from tests.functional_tests import db1, db2


def test():
    # Arrange
    users.create(db2.engine)

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
                    'users'
                ],
            }
        ]
    }
    with pytest.raises(exc.NoSuchTableError):
        carry.run(config)

    # Assert
    count = db2.engine.execute(select([func.count()]).select_from(users)).scalar()
    assert count == 0

    assert exc.exceptions.size == 1
    assert isinstance(exc.exceptions[0], exc.NoSuchTableError)
