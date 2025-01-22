import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Callable
from unittest.mock import patch

import mongomock
import msgspec
import pytest
from modular_sdk.commons.constants import ParentScope, ParentType
from moto import mock_aws

from helpers.constants import CAASEnv
from helpers.time_helper import utc_datetime, utc_iso

from .commons import (
    AWS_ACCOUNT_ID,
    AZURE_ACCOUNT_ID,
    DATA,
    GOOGLE_ACCOUNT_ID,
    InMemoryHvacClient,
)

if TYPE_CHECKING:
    from modular_sdk.models.customer import Customer
    from modular_sdk.models.tenant import Tenant

    from services.metadata import Metadata
    from services.platform_service import Platform

TEST_ENVS = {
    'AWS_REGION': 'us-east-1',
    'AWS_ACCESS_KEY_ID': 'testing',
    'AWS_SECRET_ACCESS_KEY': 'testing',
    'AWS_SECURITY_TOKEN': 'testing',
    'AWS_SESSION_TOKEN': 'testing',
    'AWS_DEFAULT_REGION': 'us-east-1',
    'SYSTEM_CUSTOMER_NAME': 'TEST_SYSTEM_CUSTOMER',
    'CAAS_INNER_CACHE_TTL_SECONDS': '0',
    'CAAS_SERVICE_MODE': 'docker',
    'CAAS_MONGO_URI': 'mongodb://testing',
    'CAAS_MONGO_DATABASE': 'custodian-as-a-service-testing',
    'CAAS_MINIO_ENDPOINT': 'http://testing',
    'CAAS_MINIO_ACCESS_KEY_ID': 'testing',
    'CAAS_MINIO_SECRET_ACCESS_KEY': 'testing',
    'CAAS_VAULT_ENDPOINT': 'http://testing',
    'CAAS_VAULT_TOKEN': 'testing',
    'modular_service_mode': 'docker',
    'modular_mongo_db_name': 'custodian-as-a-service-testing',
    'modular_mongo_user': 'testing',
    'modular_mongo_password': 'testing',
    'modular_mongo_url': 'testing',
    'AWS_ACCOUNT_ID': '123456789012',
    'AZURE_SUBSCRIPTION_ID': '3d615fa8-05c6-47ea-990d-9d162testing',
    'CLOUDSDK_CORE_PROJECT': 'testing-project-123',
    'application_name': 'syndicate-rule-engine'
}

# This "pytest_configure" function must be executed BEFORE any imports that
# can initialize MongoClient or other clients. Be careful with imports
# in other conftest files


def pytest_configure(config):
    os.environ.update(TEST_ENVS)
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


@pytest.fixture(scope='session')
def aws_scan_result() -> Path:
    return DATA / 'cloud_custodian' / 'aws'


@pytest.fixture(scope='session')
def azure_scan_result() -> Path:
    return DATA / 'cloud_custodian' / 'azure'


@pytest.fixture(scope='session')
def google_scan_result() -> Path:
    return DATA / 'cloud_custodian' / 'google'


@pytest.fixture(scope='session')
def k8s_scan_result() -> Path:
    return DATA / 'cloud_custodian' / 'kubernetes'


@pytest.fixture(scope='session')
def aws_shards_path() -> Path:
    return DATA / 'shards' / 'aws'


@pytest.fixture()
def utcnow() -> datetime:
    return utc_datetime()


@pytest.fixture(scope='session')
def load_expected():
    _cache = {}
    _decoder = msgspec.json.Decoder()

    def inner(filename: str):
        if not filename.endswith('.json'):
            filename = f'{filename}.json'
        if filename not in _cache:
            fn = DATA / 'expected' / filename
            assert fn.exists() and fn.is_file(), f'{fn} must exist for test'
            with open(fn, 'rb') as fp:
                _cache[filename] = _decoder.decode(fp.read())
        return _cache[filename]

    return inner


@pytest.fixture()
def main_customer() -> 'Customer':
    from modular_sdk.models.customer import Customer

    return Customer(
        name='TEST_CUSTOMER',
        display_name='test customer',
        admins=[],
        is_active=True,
    )


@pytest.fixture
def aws_tenant(main_customer: 'Customer') -> 'Tenant':
    from modular_sdk.models.region import RegionAttr
    from modular_sdk.models.tenant import Tenant

    return Tenant(
        name='AWS-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='AWS',
        project=AWS_ACCOUNT_ID,
        contacts={},
        activation_date=utc_iso(utc_datetime() - timedelta(days=30)),
        regions=[
            RegionAttr(
                maestro_name='EU_WEST_1',
                native_name='eu-west-1',
                cloud='AWS',
                region_id='1'
            ),
            RegionAttr(
                maestro_name='EU_CENTRAL_1',
                native_name='eu-central-1',
                cloud='AWS',
                region_id='2'
            ),
            RegionAttr(
                maestro_name='EU_NORTH_1',
                native_name='eu-north-1',
                cloud='AWS',
                region_id='3'
            ),
            RegionAttr(
                maestro_name='EU_WEST_3',
                native_name='eu-west-3',
                cloud='AWS',
                region_id='4'
            ),
        ],
    )


@pytest.fixture
def azure_tenant(main_customer: 'Customer') -> 'Tenant':
    from modular_sdk.models.tenant import Tenant

    return Tenant(
        name='AZURE-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='AZURE',
        project=AZURE_ACCOUNT_ID,
        contacts={},
        activation_date=utc_iso(utc_datetime() - timedelta(days=30)),
    )


@pytest.fixture
def google_tenant(main_customer: 'Customer') -> 'Tenant':
    from modular_sdk.models.tenant import Tenant

    return Tenant(
        name='GOOGLE-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='GOOGLE',
        project=GOOGLE_ACCOUNT_ID,
        contacts={},
        activation_date=utc_iso(utc_datetime() - timedelta(days=30)),
    )


@pytest.fixture
def k8s_platform(
    main_customer: 'Customer', aws_tenant: 'Tenant'
) -> 'Platform':
    from modular_sdk.models.parent import Parent

    from services.platform_service import Platform

    return Platform(
        parent=Parent(
            parent_id='platform_id',
            application_id='application_id',
            customer_id=main_customer.name,
            type=ParentType.PLATFORM_K8S.value,
            description='Test platform',
            meta={'name': 'test', 'region': 'eu-west-1', 'type': 'EKS'},
            is_deleted=False,
            type_scope=f'{ParentType.PLATFORM_K8S.value}#{ParentScope.SPECIFIC.value}#{aws_tenant.name}',
        )
    )


@pytest.fixture(scope='session')
def empty_metadata() -> 'Metadata':
    from services.metadata import Metadata

    return Metadata.empty()


@pytest.fixture(scope='session')
def load_metadata() -> Callable:
    def _inner(name: str, load_as=dict) -> dict:
        if not name.endswith('.json'):
            name = f'{name}.json'
        path = DATA / 'metadata' / name
        assert path.exists(), f'{path} must exist'
        with open(path, 'rb') as fp:
            return msgspec.json.decode(fp.read(), type=load_as)
    return _inner
