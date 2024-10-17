import json

import boto3
import pytest
from moto.backends import get_backend
from webtest import TestApp

from helpers.constants import Permission, CAASEnv, PolicyEffect
from services import SP  # probably the only safe import we can use in conftest
from ..commons import SOURCE, InMemoryHvacClient, SREClient


# assuming that only this package will use mongo so that we need to clear
# it after each invocation. It's here for optimization purposes

# data sources fixtures
@pytest.fixture(autouse=True)
def mocked_mongo_client(request):
    client = request.config.mongo_client
    yield client
    for db in client.list_database_names():
        client.drop_database(db)


@pytest.fixture(autouse=True)
def mocked_s3_client(request):
    yield boto3.client('s3')
    get_backend('s3').reset()


@pytest.fixture(autouse=True)
def mocked_hvac_client(request):
    yield InMemoryHvacClient()
    InMemoryHvacClient.reset()


# logic fixtures

@pytest.fixture(autouse=True)
def vault_token(mocked_hvac_client) -> None:
    # just test key
    mocked_hvac_client.secrets.kv.v2.create_or_update_secret(
        path='rule-engine-private-key',
        secret={
            'data': 'LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSHVBZ0VBTUJBR0J5cUdTTTQ5QWdFR0JTdUJCQUFqQklIV01JSFRBZ0VCQkVJQXRGcnczSW43QzZuK01hSHEKK3BJQnBiejVjUzI5V202RVRidUpCbmJUeUhYZ0V2cjBNcXFLT25qemY1VCtoSGZodVhYSSs5VE5VR1dCekl0Rgo4THRhVGltaGdZa0RnWVlBQkFBbUs4U25HYUkyVHNJRXAzMDZIRWgzZXNTNHNLUXZ4QXNmY0R4ZUVEVW1GUGxhCkhKejUyM2MzMVJXRGNLaXR1cXFpUXlOYjZvdEM3MXZMdjNXNCswSW9Bd0NGc0RUWVVqMHl1SmU4TjBWblNYSHMKOWpFOWR2L2dPSUYreG1YMjI1bnIzZnU4UHhiWDZXdlFhQjcwUTZ2WUlsOGtCNStSaTA2WkxESms2cHBCdHM0SApHUT09Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K'},
        mount_point='kv'
    )


@pytest.fixture()
def system_user(mocked_mongo_client, vault_token) -> tuple[str, str]:
    """
    Creates system policy, role and user
    """
    SP.policy_service.create(
        customer=CAASEnv.SYSTEM_CUSTOMER_NAME.get(),
        name='system',
        description='system policy',
        permissions=[p.value for p in Permission],
        tenants=['*'],
        effect=PolicyEffect.ALLOW
    ).save()
    SP.role_service.create(
        customer=CAASEnv.SYSTEM_CUSTOMER_NAME.get(),
        name='system',
        expiration=None,
        policies=['system'],
        description='system role',
    )
    SP.users_client.signup_user(
        username='system',
        password='system',
        customer=CAASEnv.SYSTEM_CUSTOMER_NAME.get(),
        role='system'
    )
    return 'system', 'system'


@pytest.fixture()
def system_user_token(system_user) -> str:
    return SP.users_client.authenticate_user(
        username=system_user[0],
        password=system_user[1]
    )['id_token']


@pytest.fixture(scope='session')
def deployment_resources():
    name = 'deployment_resources.json'
    with open(SOURCE / name, 'r') as f:
        data1 = json.load(f).get('custodian-as-a-service-api') or {}
    with open(SOURCE / 'validators' / name, 'r') as f:
        data2 = json.load(f).get('custodian-as-a-service-api') or {}
    data1['models'] = data2.get('models') or {}
    return data1


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


@pytest.fixture
def sre_client(wsgi_test_app) -> SREClient:
    return SREClient(wsgi_test_app)
