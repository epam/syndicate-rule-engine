import os

import mongomock
import pytest
from moto import mock_aws

from helpers.constants import CAASEnv


# here I mock the global MongoClient and also add a global fixture that is
# executed before each test and clears the database. Not sure if such
# approach is ok, but it works. The same with minio and vault.
# This "pytest_confiture" function must be executed BEFORE any imports that
# can initialize MongoClient or other clients. Be careful with imports
# in other conftest files

def pytest_configure(config):
    mongo_patcher = mongomock.patch(servers=('testing',))  # todo from test.env
    cl = mongo_patcher.start()
    config.mongo_client = cl(host='testing')
    config._mongo_patcher = mongo_patcher

    aws_patcher = mock_aws()
    aws_patcher.start()
    config._aws_patcher = aws_patcher


def pytest_unconfigure(config):
    patcher = getattr(config, '_mongo_patcher', None)
    if patcher:
        patcher.stop()
    patcher = getattr(config, '_aws_patcher', None)
    if patcher:
        patcher.stop()


@pytest.fixture(autouse=True)
def clear_envs():
    # todo think about
    os.environ.pop(CAASEnv.INVOCATION_REQUEST_ID.value, None)
    os.environ.pop(CAASEnv.API_GATEWAY_STAGE.value, None)
    os.environ.pop(CAASEnv.API_GATEWAY_HOST.value, None)
