import os

import pytest

from tests import metadata
from tests.functional_tests import db1, db2, db3
from tests.utlis import mkdir


@pytest.fixture(scope='session', autouse=True)
def create_res_dir():
    with mkdir(os.path.join(os.path.dirname(__file__), 'res')):
        yield


@pytest.fixture(autouse=True)
def drop12():
    metadata.drop_all(db1.engine)
    metadata.drop_all(db2.engine)
    yield
    metadata.drop_all(db1.engine)
    metadata.drop_all(db2.engine)


@pytest.fixture
def drop123(drop12):
    metadata.drop_all(db3.engine)
    yield
    metadata.drop_all(db3.engine)
