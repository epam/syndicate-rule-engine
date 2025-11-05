import time
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
from uuid import uuid4

import pytest

from helpers.constants import Cloud, Severity
from models.resource_exception import ResourceException
from services.resource_exception_service import (
    ResourceExceptionsCollection,
    ResourceExceptionsService,
    ConflictError,
)
from services.resources import AWSResource, AZUREResource
from services.sharding import (
    ShardsCollection,
    ShardPart,
    SingleShardDistributor,
)
from services.metadata import Metadata


@pytest.fixture
def service() -> ResourceExceptionsService:
    """Fixture for ResourceExceptionsService"""
    return ResourceExceptionsService()


@pytest.fixture
def expire_at() -> float:
    """Fixture for expire_at timestamp"""
    return (datetime.now(timezone.utc) + timedelta(days=30)).timestamp()


def test_filter_no_exceptions():
    """Test filter_exception_resources with no exception filters"""
    collection = ResourceExceptionsCollection([])

    distributor = SingleShardDistributor()
    shards_collection = ShardsCollection(distributor)

    aws_resource_data = {
        'id': 'i-123456789',
        'name': 'test-instance',
        'region': 'us-east-1',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [{'Key': 'Environment', 'Value': 'Production'}],
        'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-123456789',
        'date': '2024-01-01T00:00:00Z',
    }

    azure_resource_data = {
        'id': 'azure-vm-123',
        'name': 'test-vm',
        'location': 'eastus',
        'resource_type': 'azure.vm',
        'sync_date': time.time(),
        'tags': {'Environment': 'Production'},
    }

    resources_raw = [aws_resource_data, azure_resource_data]
    shard_part = ShardPart(
        policy='test-policy', location='us-east-1', resources=resources_raw
    )

    shards_collection.meta = {
        'test-policy': {
            'resource': 'aws.ec2',
            'description': 'test policy',
            'comment': 'test comment',
        }
    }

    shards_collection.put_part(shard_part)

    mock_metadata = Mock(spec=Metadata)
    mock_rule_metadata = Mock()
    mock_rule_metadata.severity.value = Severity.HIGH.value
    mock_rule_metadata.iter_mitre_attacks.return_value = []
    mock_metadata.rule.return_value = mock_rule_metadata

    with pytest.MonkeyPatch().context() as m:

        def mock_shard_to_resources(cloud, shard, rt, metadata, account_id):
            yield AWSResource(
                id=aws_resource_data['id'],
                name=aws_resource_data['name'],
                region=aws_resource_data['region'],
                resource_type=aws_resource_data['resource_type'],
                sync_date=aws_resource_data['sync_date'],
                data=aws_resource_data,
                arn=aws_resource_data['arn'],
                date=aws_resource_data['date']
            )
            yield AZUREResource(
                id=azure_resource_data['id'],
                name=azure_resource_data['name'],
                location=azure_resource_data['location'],
                resource_type=azure_resource_data['resource_type'],
                sync_date=azure_resource_data['sync_date'],
                data=azure_resource_data
            )

        m.setattr(
            'services.resource_exception_service._shard_to_resources',
            mock_shard_to_resources,
        )
        m.setattr(
            'services.resource_exception_service.prepare_resource_type',
            lambda rt, cloud: rt,
        )

        exception_data, non_exception_collection = (
            collection.filter_exception_resources(
                shards_collection, Cloud.AWS, mock_metadata, 'account-123'
            )
        )

    # Test exception data (should be empty since no exceptions defined)
    assert exception_data == []

    # Test non-exception collection (should contain all resources)
    assert len(non_exception_collection.shards) == 1

    non_exception_parts = list(non_exception_collection.iter_parts())
    assert len(non_exception_parts) == 1
    assert len(non_exception_parts[0].resources) == 2

    part = non_exception_parts[0]
    assert part.policy == 'test-policy'
    assert part.location == 'us-east-1'
    assert len(part.resources) == 2

    resource_ids = [resource['id'] for resource in part.resources]
    assert 'i-123456789' in resource_ids
    assert 'azure-vm-123' in resource_ids


def test_filter_one_exception():
    """Test filter_exception_resources with filter that will find one exception"""
    exception = ResourceException(
        id=str(uuid4()),
        customer_name='test-customer',
        tenant_name='test-tenant',
        resource_id='i-123456789',
        location='us-east-1',
        resource_type='aws.ec2',
        created_at=time.time(),
        updated_at=time.time(),
        expire_at=datetime.now(timezone.utc) + timedelta(days=1),
    )

    collection = ResourceExceptionsCollection([exception])

    distributor = SingleShardDistributor()
    shards_collection = ShardsCollection(distributor)

    aws_resource_match = {
        'id': 'i-123456789',
        'name': 'test-instance-match',
        'region': 'us-east-1',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [{'Key': 'Environment', 'Value': 'Production'}],
        'InstanceId': 'i-123456789',
        'ImageId': 'ami-12345678',
        'State': {'Name': 'running'},
        'InstanceType': 't2.micro',
        'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-123456789',
        'date': '2024-01-01T00:00:00Z',
    }

    aws_resource_no_match = {
        'id': 'i-987654321',
        'name': 'test-instance-no-match',
        'region': 'us-east-1',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [{'Key': 'Environment', 'Value': 'Development'}],
        'InstanceId': 'i-987654321',
        'ImageId': 'ami-87654321',
        'State': {'Name': 'running'},
        'InstanceType': 't2.micro',
        'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-987654321',
        'date': '2024-01-01T00:00:00Z',
    }

    resources_raw = [aws_resource_match, aws_resource_no_match]
    shard_part = ShardPart(
        policy='test-policy', location='us-east-1', resources=resources_raw
    )

    shards_collection.meta = {
        'test-policy': {
            'resource': 'aws.ec2',
            'description': 'test policy',
            'comment': 'test comment',
        }
    }

    shards_collection.put_part(shard_part)

    mock_metadata = Mock(spec=Metadata)
    mock_rule_metadata = Mock()
    mock_rule_metadata.severity.value = Severity.HIGH.value
    mock_rule_metadata.iter_mitre_attacks.return_value = ['T1005']  # Sample MITRE technique
    mock_metadata.rule.return_value = mock_rule_metadata

    with pytest.MonkeyPatch().context() as m:

        def mock_shard_to_resources(cloud, shard, rt, metadata, account_id):
            yield AWSResource(
                id=aws_resource_match['id'],
                name=aws_resource_match['name'],
                region=aws_resource_match['region'],
                resource_type=aws_resource_match['resource_type'],
                sync_date=aws_resource_match['sync_date'],
                data=aws_resource_match,
                arn=aws_resource_match['arn'],
                date=aws_resource_match['date']
            )
            yield AWSResource(
                id=aws_resource_no_match['id'],
                name=aws_resource_no_match['name'],
                region=aws_resource_no_match['region'],
                resource_type=aws_resource_no_match['resource_type'],
                sync_date=aws_resource_no_match['sync_date'],
                data=aws_resource_no_match,
                arn=aws_resource_no_match['arn'],
                date=aws_resource_no_match['date']
            )

        m.setattr(
            'services.resource_exception_service._shard_to_resources',
            mock_shard_to_resources,
        )
        m.setattr(
            'services.resource_exception_service.prepare_resource_type',
            lambda rt, cloud: rt,
        )

        exception_data, non_exception_collection = (
            collection.filter_exception_resources(
                shards_collection, Cloud.AWS, mock_metadata, 'account-123'
            )
        )

    assert len(exception_data) == 1
    
    exception_info = exception_data[0]
    assert 'exception' in exception_info
    assert 'type' in exception_info
    assert 'added_date' in exception_info
    assert 'expiration_data' in exception_info
    assert 'summary' in exception_info
    
    summary = exception_info['summary']
    assert 'resources_data' in summary
    assert 'violations_data' in summary
    assert 'attacks_data' in summary
    
    assert isinstance(summary['resources_data'], dict)
    assert isinstance(summary['violations_data'], dict)
    assert isinstance(summary['attacks_data'], dict)

    assert len(non_exception_collection.shards) == 1

    non_exception_parts = list(non_exception_collection.iter_parts())
    assert len(non_exception_parts) == 1
    assert len(non_exception_parts[0].resources) == 1

    non_exception_resource = non_exception_parts[0].resources[0]
    assert non_exception_resource['id'] == 'i-987654321'


def test_filter_all_exceptions():
    """Test filter_exception_resources with filter that will make all the resources an exception"""
    exception = ResourceException(
        id=str(uuid4()),
        customer_name='test-customer',
        tenant_name='test-tenant',
        tags_filters=['Environment=Production'],
        created_at=time.time(),
        updated_at=time.time(),
        expire_at=datetime.now(timezone.utc) + timedelta(days=1),
    )

    collection = ResourceExceptionsCollection([exception])

    distributor = SingleShardDistributor()
    shards_collection = ShardsCollection(distributor)

    aws_resource1 = {
        'id': 'i-123456789',
        'name': 'test-instance-1',
        'region': 'us-east-1',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [{'Key': 'Environment', 'Value': 'Production'}],
        'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-123456789',
        'date': '2024-01-01T00:00:00Z',
    }

    aws_resource2 = {
        'id': 'i-987654321',
        'name': 'test-instance-2',
        'region': 'us-east-1',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Team', 'Value': 'Backend'},
        ],
        'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-987654321',
        'date': '2024-01-01T00:00:00Z',
    }

    aws_resource3 = {
        'id': 'i-555666777',
        'name': 'test-instance-3',
        'region': 'us-west-2',
        'resource_type': 'aws.ec2',
        'sync_date': time.time(),
        'Tags': [
            {'Key': 'Environment', 'Value': 'Production'},
            {'Key': 'Owner', 'Value': 'Alice'},
        ],
        'arn': 'arn:aws:ec2:us-west-2:123456789:instance/i-555666777',
        'date': '2024-01-01T00:00:00Z',
    }

    resources_raw = [aws_resource1, aws_resource2, aws_resource3]
    shard_part = ShardPart(
        policy='test-policy', location='us-east-1', resources=resources_raw
    )

    shards_collection.meta = {
        'test-policy': {
            'resource': 'aws.ec2',
            'description': 'test policy',
            'comment': 'test comment',
        }
    }

    shards_collection.put_part(shard_part)

    mock_metadata = Mock(spec=Metadata)
    mock_rule_metadata = Mock()
    mock_rule_metadata.severity.value = Severity.MEDIUM.value
    mock_rule_metadata.iter_mitre_attacks.return_value = ['T1005', 'T1078']  # Multiple MITRE techniques
    mock_metadata.rule.return_value = mock_rule_metadata

    with pytest.MonkeyPatch().context() as m:

        def mock_shard_to_resources(cloud, shard, rt, metadata, account_id):
            yield AWSResource(
                id=aws_resource1['id'],
                name=aws_resource1['name'],
                region=aws_resource1['region'],
                resource_type=aws_resource1['resource_type'],
                sync_date=aws_resource1['sync_date'],
                data=aws_resource1,
                arn=aws_resource1['arn'],
                date=aws_resource1['date']
            )
            yield AWSResource(
                id=aws_resource2['id'],
                name=aws_resource2['name'],
                region=aws_resource2['region'],
                resource_type=aws_resource2['resource_type'],
                sync_date=aws_resource2['sync_date'],
                data=aws_resource2,
                arn=aws_resource2['arn'],
                date=aws_resource2['date']
            )
            yield AWSResource(
                id=aws_resource3['id'],
                name=aws_resource3['name'],
                region=aws_resource3['region'],
                resource_type=aws_resource3['resource_type'],
                sync_date=aws_resource3['sync_date'],
                data=aws_resource3,
                arn=aws_resource3['arn'],
                date=aws_resource3['date']
            )

        m.setattr(
            'services.resource_exception_service._shard_to_resources',
            mock_shard_to_resources,
        )
        m.setattr(
            'services.resource_exception_service.prepare_resource_type',
            lambda rt, cloud: rt,
        )

        exception_data, non_exception_collection = (
            collection.filter_exception_resources(
                shards_collection, Cloud.AWS, mock_metadata, 'account-123'
            )
        )

    assert len(exception_data) == 1
    
    exception_info = exception_data[0]
    assert 'exception' in exception_info
    assert 'type' in exception_info
    assert 'added_date' in exception_info
    assert 'expiration_data' in exception_info
    assert 'summary' in exception_info
    
    summary = exception_info['summary']
    assert 'resources_data' in summary
    assert 'violations_data' in summary
    assert 'attacks_data' in summary
    
    resources_data = summary['resources_data']
    violations_data = summary['violations_data']
    attacks_data = summary['attacks_data']
    

    assert Severity.MEDIUM.value in resources_data
    assert Severity.MEDIUM.value in violations_data
    assert Severity.MEDIUM.value in attacks_data
    

    assert resources_data[Severity.MEDIUM.value] == 3
    assert violations_data[Severity.MEDIUM.value] == 3  
    assert attacks_data[Severity.MEDIUM.value] == 6

    assert len(non_exception_collection.shards) == 1
    non_exception_parts = list(non_exception_collection.iter_parts())
    assert len(non_exception_parts) == 1
    assert len(non_exception_parts[0].resources) == 0 


@pytest.mark.parametrize(
    'params',
    [
        pytest.param(
            {
                'resource_id': 'i-123456789',
                'location': 'us-east-1',
                'resource_type': 'aws.ec2',
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': None,
            },
            id='duplicate_resource',
        ),
        pytest.param(
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-123456789',
                'tags_filters': None,
            },
            id='duplicate_arn',
        ),
        pytest.param(
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': ['Environment=Production', 'Team=Backend'],
            },
            id='duplicate_tags',
        ),
    ],
)
def test_create_conflict_error(service, expire_at, params):
    """Test that creating duplicate exceptions raises ConflictError"""
    exception1 = service.create(**params, expire_at=expire_at)
    exception1.save()

    with pytest.raises(ConflictError) as exc_info:
        service.create(**params, expire_at=expire_at)

    assert 'already exists' in str(exc_info.value)
    assert exception1.id in str(exc_info.value)

    exception1.delete()


@pytest.mark.parametrize(
    'params1,params2',
    [
        pytest.param(
            {
                'resource_id': 'i-123456789',
                'location': 'us-east-1',
                'resource_type': 'aws.ec2',
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': None,
            },
            {
                'resource_id': 'i-987654321',
                'location': 'us-east-1',
                'resource_type': 'aws.ec2',
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': None,
            },
            id='update_to_duplicate_resource',
        ),
        pytest.param(
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-123456789',
                'tags_filters': None,
            },
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': 'arn:aws:ec2:us-east-1:123456789:instance/i-987654321',
                'tags_filters': None,
            },
            id='update_to_duplicate_arn',
        ),
        pytest.param(
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': ['Environment=Production', 'Team=Backend'],
            },
            {
                'resource_id': None,
                'location': None,
                'resource_type': None,
                'tenant_name': 'test-tenant',
                'customer_name': 'test-customer',
                'arn': None,
                'tags_filters': ['Environment=Development'],
            },
            id='update_to_duplicate_tags',
        ),
    ],
)
def test_update_conflict_error(service, expire_at, params1, params2):
    """Test that updating to duplicate parameters raises ConflictError"""
    exception1 = service.create(**params1, expire_at=expire_at)
    exception1.save()

    exception2 = service.create(**params2, expire_at=expire_at)
    exception2.save()

    with pytest.raises(ConflictError) as exc_info:
        service.update_resource_exception_by_id(
            id=exception2.id, **params1, expire_at=expire_at
        )

    assert 'already exists' in str(exc_info.value)
    assert exception1.id in str(exc_info.value)

    exception1.delete()
    exception2.delete()


def test_update_no_conflict_same_exception(service, expire_at):
    """Test that updating the same exception doesn't raise ConflictError"""
    exception = service.create(
        resource_id='i-123456789',
        location='us-east-1',
        resource_type='aws.ec2',
        tenant_name='test-tenant',
        customer_name='test-customer',
        arn=None,
        tags_filters=None,
        expire_at=expire_at,
    )
    exception.save()

    new_expire_at = (datetime.now(timezone.utc) + timedelta(days=60)).timestamp()
    updated = service.update_resource_exception_by_id(
        id=exception.id,
        resource_id='i-123456789',
        location='us-east-1',
        resource_type='aws.ec2',
        tenant_name='test-tenant',
        customer_name='test-customer',
        arn=None,
        tags_filters=None,
        expire_at=new_expire_at,
    )

    assert updated.id == exception.id
    assert updated.expire_at.timestamp() == pytest.approx(new_expire_at, abs=1)

    exception.delete()


@pytest.mark.parametrize(
    'params_list',
    [
        pytest.param(
            [
                {
                    'resource_id': 'i-123456789',
                    'location': 'us-east-1',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'test-tenant',
                    'customer_name': 'test-customer',
                },
                {
                    'resource_id': 'i-987654321',
                    'location': 'us-east-1',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'test-tenant',
                    'customer_name': 'test-customer',
                },
                {
                    'resource_id': 'i-123456789',
                    'location': 'us-west-2',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'test-tenant',
                    'customer_name': 'test-customer',
                },
                {
                    'resource_id': 'i-123456789',
                    'location': 'us-east-1',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'another-tenant',
                    'customer_name': 'test-customer',
                },
            ],
            id='different_resource_parameters',
        ),
        pytest.param(
            [
                {
                    'resource_id': 'i-123456789',
                    'location': 'us-east-1',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'test-tenant',
                    'customer_name': 'customer1',
                },
                {
                    'resource_id': 'i-123456789',
                    'location': 'us-east-1',
                    'resource_type': 'aws.ec2',
                    'tenant_name': 'test-tenant',
                    'customer_name': 'customer2',
                },
            ],
            id='different_customers',
        ),
    ],
)
def test_create_no_conflict(service, expire_at, params_list):
    """Test that creating exceptions with different parameters doesn't raise ConflictError"""
    exceptions = []
    for params in params_list:
        exc = service.create(
            arn=None, tags_filters=None, expire_at=expire_at, **params
        )
        exc.save()
        exceptions.append(exc)

    assert len(exceptions) == len(params_list)
    assert len(set(e.id for e in exceptions)) == len(params_list)

    for exc in exceptions:
        exc.delete()
