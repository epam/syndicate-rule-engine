import os
import msgspec
from datetime import timedelta
from typing import TYPE_CHECKING

from pathlib import Path
import mongomock
import pytest
from moto import mock_aws

from unittest.mock import patch
from helpers.constants import CAASEnv
from helpers.time_helper import utc_iso, utc_datetime
from .commons import InMemoryHvacClient, DATA, AWS_ACCOUNT_ID, AZURE_ACCOUNT_ID, GOOGLE_ACCOUNT_ID
from modular_sdk.commons.constants import ParentType, ParentScope
if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant
    from modular_sdk.models.customer import Customer
    from services.platform_service import Platform


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


@pytest.fixture
def aws_scan_result() -> Path:
    return DATA / "cloud_custodian" / "aws"


@pytest.fixture
def azure_scan_result() -> Path:
    return DATA / "cloud_custodian" / "azure"


@pytest.fixture
def google_scan_result() -> Path:
    return DATA / "cloud_custodian" / "google"


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
        is_active=True
    )


@pytest.fixture
def aws_tenant(main_customer: 'Customer') -> 'Tenant':
    from modular_sdk.models.tenant import Tenant
    from modular_sdk.models.region import RegionAttr
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
            RegionAttr(native_name='eu-west-1'),
            RegionAttr(native_name='eu-central-1'),
            RegionAttr(native_name='eu-north-1'),
            RegionAttr(native_name='eu-west-3')
        ]
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
def k8s_platform(main_customer: 'Customer',
                 aws_tenant: 'Tenant') -> 'Platform':
    from services.platform_service import Platform
    from modular_sdk.models.parent import Parent
    return Platform(
        parent=Parent(
            parent_id='platform_id',
            customer_id=main_customer.name,
            type=ParentType.PLATFORM_K8S.value,
            description='Test platform',
            meta={
                'name': 'test',
                'region': 'eu-west-1',
                'type': 'EKS'
            },
            is_deleted=False,
            type_scope=f'{ParentType.PLATFORM_K8S.value}#{ParentScope.SPECIFIC.value}#{aws_tenant.name}'
        )
    )
