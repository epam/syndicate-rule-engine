from types import SimpleNamespace

import pytest

from helpers.constants import GLOBAL_REGION, ResourcesCollectorType
from models.resource import Resource
from services.resources_service import ResourcesService

CUSTOMER = 'TEST_CUSTOMER'
TENANT = 'TEST_TENANT'


@pytest.fixture()
def service() -> ResourcesService:
    svc = ResourcesService()
    col = Resource.mongo_adapter().get_collection(Resource)
    col.delete_many({})
    yield svc
    col.delete_many({})


@pytest.fixture()
def tenant():
    return SimpleNamespace(name=TENANT, customer_name=CUSTOMER)


def _save(
    svc: ResourcesService,
    resource_type: str,
    location: str,
    id_: str,
    tenant_name: str = TENANT,
    customer_name: str = CUSTOMER,
) -> None:
    svc.save(
        svc.create(
            account_id='123456789012',
            location=location,
            resource_type=resource_type,
            id=id_,
            name=id_,
            arn=None,
            data={},
            sync_date=0.0,
            collector_type=ResourcesCollectorType.CUSTODIAN,
            tenant_name=tenant_name,
            customer_name=customer_name,
        )
    )


def test_counts_grouped_by_type_and_location(service, tenant):
    _save(service, 'aws.ec2', 'eu-west-1', 'i-1')
    _save(service, 'aws.ec2', 'eu-west-1', 'i-2')
    _save(service, 'aws.ec2', 'eu-central-1', 'i-3')
    _save(service, 'aws.s3', GLOBAL_REGION, 'bucket-1')

    res = service.count_type_resources_for_tenant(
        tenant,
        {
            'rule-1': {'resource': 'aws.ec2'},
            'rule-2': {'resource': 'aws.s3'},
        },
    )
    assert res == {
        'aws.ec2': {'eu-west-1': 2, 'eu-central-1': 1},
        'aws.s3': {GLOBAL_REGION: 1},
    }


def test_other_tenants_and_types_are_not_counted(service, tenant):
    _save(service, 'aws.ec2', 'eu-west-1', 'i-1')
    _save(service, 'aws.ec2', 'eu-west-1', 'i-2', tenant_name='OTHER_TENANT')
    _save(service, 'aws.rds', 'eu-west-1', 'db-1')

    res = service.count_type_resources_for_tenant(
        tenant, {'rule-1': {'resource': 'aws.ec2'}}
    )
    assert res == {'aws.ec2': {'eu-west-1': 1}}


def test_counts_are_not_capped(service, tenant):
    """The previous implementation silently capped counts at 50."""
    for i in range(120):
        _save(service, 'aws.ec2', 'eu-west-1', f'i-{i}')

    res = service.count_type_resources_for_tenant(
        tenant, {'rule-1': {'resource': 'aws.ec2'}}
    )
    assert res == {'aws.ec2': {'eu-west-1': 120}}


def test_empty_metadata_does_not_query(service, tenant):
    _save(service, 'aws.ec2', 'eu-west-1', 'i-1')
    assert service.count_type_resources_for_tenant(tenant, {}) == {}


@pytest.mark.parametrize(
    ('region', 'expected'),
    (
        ('eu-west-1', 3),  # 2 regional + 1 global
        ('eu-central-1', 1),  # only the global one
        (GLOBAL_REGION, 1),  # global must not be counted twice
    ),
)
def test_resources_scanned_for_region(region, expected):
    type_resources = {'aws.ec2': {'eu-west-1': 2, GLOBAL_REGION: 1}}
    assert (
        ResourcesService.resources_scanned_for_region(
            type_resources, 'aws.ec2', region
        )
        == expected
    )


def test_resources_scanned_for_unknown_type():
    assert (
        ResourcesService.resources_scanned_for_region(
            {}, 'aws.ec2', 'eu-west-1'
        )
        == 0
    )
