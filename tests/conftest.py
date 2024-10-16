import json
import os

import mongomock
import pytest
from moto import mock_aws
from webtest import TestApp

from .commons import SOURCE


# here I mock the global MongoClient and also add a global fixture that is
# executed before each test and clears the database. Not sure if such
# approach is ok, but it works. The same with minio and vault.
# This "pytest_confiture" function must be executed BEFORE any imports that
# can initialize MongoClient or other clients. Be careful with imports
# in other conftest files

def pytest_configure(config):
    mongo_patcher = mongomock.patch(servers=('testing', ))  # todo from test.env
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
def mocked_mongo_client(request):
    client = request.config.mongo_client
    for db in client.list_database_names():
        client.drop_database(db)
    return client


@pytest.fixture(scope='session')
def deployment_resources():
    name = 'deployment_resources.json'
    with open(SOURCE / name, 'r') as f:
        data1 = json.load(f).get('custodian-as-a-service-api') or {}
    with open(SOURCE / 'validators' / name, 'r') as f:
        data2 = json.load(f).get('custodian-as-a-service-api') or {}
    data1['models'] = data2.get('models') or {}
    return data1


@pytest.fixture(autouse=True)
def clear_envs():
    # todo think about
    os.environ.pop('_INVOCATION_REQUEST_ID', None)
    os.environ.pop('_CAAS_API_GATEWAY_STAGE', None)
    os.environ.pop('_CAAS_API_GATEWAY_HOST', None)


@pytest.fixture(
    scope='session')  # todo think about scope and look at the performance
def wsgi_app(deployment_resources):
    from onprem.api.app import OnPremApiBuilder
    from onprem.api.deployment_resources_parser import \
        DeploymentResourcesApiGatewayWrapper
    dr_wrapper = DeploymentResourcesApiGatewayWrapper(deployment_resources)
    builder = OnPremApiBuilder(dr_wrapper)
    return builder.build()


@pytest.fixture
def wsgi_test_app(wsgi_app) -> TestApp:
    return TestApp(wsgi_app)
