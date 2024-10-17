import os

import mongomock
import pytest
from moto import mock_aws

from unittest.mock import patch
from helpers.constants import CAASEnv
from .commons import InMemoryHvacClient


# here I mock the global MongoClient and also add a global fixture that is
# executed before each test and clears the database. Not sure if such
# approach is ok, but it works. The same with minio and vault.
# This "pytest_configure" function must be executed BEFORE any imports that
# can initialize MongoClient or other clients. Be careful with imports
# in other conftest files

def pytest_configure(config):
    os.environ['MOTO_S3_CUSTOM_ENDPOINTS'] = CAASEnv.MINIO_ENDPOINT.get('')

    mongo_patcher = mongomock.patch(servers=CAASEnv.MONGO_URI.get('localhost'))
    cl = mongo_patcher.start()
    config.mongo_client = cl(host=CAASEnv.MONGO_URI.get('localhost'))
    config._mongo_patcher = mongo_patcher

    aws_patcher = mock_aws()
    aws_patcher.start()
    config._aws_patcher = aws_patcher

    vault_patcher = patch('hvac.Client', InMemoryHvacClient)
    vault_patcher.start()
    config._vault_patcher = vault_patcher


def pytest_unconfigure(config):
    if patcher := getattr(config, '_mongo_patcher', None):
        patcher.stop()
    if patcher := getattr(config, '_aws_patcher', None):
        patcher.stop()
    if patcher := getattr(config, '_vault_patcher', None):
        patcher.stop()


@pytest.fixture(autouse=True)
def clear_envs():
    """
    These are envs that can be set dynamically for internal purposes. So
    we clear it after each test. Maybe should redesign this thing
    """
    os.environ.pop(CAASEnv.INVOCATION_REQUEST_ID.value, None)
    os.environ.pop(CAASEnv.API_GATEWAY_STAGE.value, None)
    os.environ.pop(CAASEnv.API_GATEWAY_HOST.value, None)
