import json

import pytest
from webtest import TestApp

from ..commons import SOURCE


# assuming that only this package will use mongo so that we need to clear
# it after each invocation. It's here for optimization purposes

@pytest.fixture(autouse=True)
def mocked_mongo_client(request):
    client = request.config.mongo_client
    yield client
    for db in client.list_database_names():
        client.drop_database(db)


@pytest.fixture(scope='session')
def deployment_resources():
    name = 'deployment_resources.json'
    with open(SOURCE / name, 'r') as f:
        data1 = json.load(f).get('custodian-as-a-service-api') or {}
    with open(SOURCE / 'validators' / name, 'r') as f:
        data2 = json.load(f).get('custodian-as-a-service-api') or {}
    data1['models'] = data2.get('models') or {}
    return data1


@pytest.fixture(scope='session')  # todo think about scope and look at the performance
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
