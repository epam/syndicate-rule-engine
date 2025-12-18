import pytest
from datetime import datetime, timedelta
from time import time
from uuid import uuid4

from helpers.time_helper import utc_datetime
from models.resource_exception import ResourceException


@pytest.fixture
def create_resource_exception():
    """
    Helper fixture to create resource exception instances
    """
    def _create_resource_exception(
        customer_name='TEST_CUSTOMER',
        tenant_name='AWS-TESTING',
        resource_type='aws.ec2',
        location='us-east-1',
        resource_id='i-1234567890abcdef0',
        arn=None,
        tags_filters=None,
        expire_days=30
    ):
        now = utc_datetime()
        expire_at = now + timedelta(days=expire_days)
        
        resource_exception = ResourceException(
            id=str(uuid4()),
            customer_name=customer_name,
            tenant_name=tenant_name,
            resource_type=resource_type,
            location=location,
            resource_id=resource_id,
            arn=arn,
            tags_filters=tags_filters,
            created_at=time(),
            updated_at=time(),
            expire_at=expire_at,
        )
        
        return resource_exception
    
    return _create_resource_exception


@pytest.fixture
def sample_resource_exception(create_resource_exception, main_customer, aws_tenant):
    """
    Sample resource exception for testing
    """
    return create_resource_exception(
        customer_name=main_customer.name,
        tenant_name=aws_tenant.name,
        resource_type='aws.ec2',
        location='us-east-1',
        resource_id='i-1234567890abcdef0',
        expire_days=30
    )


def test_get_resource_exception_by_id_success(
    system_user_token, sre_client, sample_resource_exception, aws_tenant, main_customer
):
    """
    Test successful retrieval of a resource exception by ID
    """
    # Save the resource exception to the database
    sample_resource_exception.save()
    
    # Make the request to get the resource exception by ID
    resp = sre_client.request(
        f'/resources/exceptions/{sample_resource_exception.id}',
        auth=system_user_token,
        data={
            'customer_id': main_customer.name
        },
    )
    
    assert resp.status_int == 200
    
    expected_data = {
        'id': sample_resource_exception.id,
        'type': sample_resource_exception.type,
        'customer_name': sample_resource_exception.customer_name,
        'tenant_name': sample_resource_exception.tenant_name,
        'resource_type': sample_resource_exception.resource_type,
        'location': sample_resource_exception.location,
        'resource_id': sample_resource_exception.resource_id,
        'created_at': sample_resource_exception.created_at,
        'updated_at': sample_resource_exception.updated_at,
        'expire_at': sample_resource_exception.expire_at.timestamp(),
    }

    assert resp.json == {
        'data': expected_data
    }


def test_get_resource_exceptions_success(
    system_user_token, sre_client, sample_resource_exception, main_customer
):
    """
    Test successful retrieval of resource exceptions list
    """
    sample_resource_exception.save()
    
    resp = sre_client.request(
        '/resources/exceptions',
        auth=system_user_token,
        data={
            'customer_id': main_customer.name,
            'tenant_name': sample_resource_exception.tenant_name,
        },
    )
    
    assert resp.status_int == 200
    
    assert 'items' in resp.json
    assert len(resp.json['items']) >= 1
    
    found_item = None
    for item in resp.json['items']:
        if item['id'] == sample_resource_exception.id:
            found_item = item
            break
    
    assert found_item is not None
    assert found_item['customer_name'] == sample_resource_exception.customer_name
    assert found_item['tenant_name'] == sample_resource_exception.tenant_name
    assert found_item['resource_type'] == sample_resource_exception.resource_type


def test_create_resource_exception_success(
    system_user_token, sre_client, main_customer, aws_tenant
):
    """
    Test successful creation of a resource exception
    """
    # Calculate a future date (30 days from now)
    future_date = (datetime.now() + timedelta(days=30)).isoformat()

    request_data = {
        'customer_id': main_customer.name,
        'tenant_name': aws_tenant.name,
        'resource_type': 'aws.ec2',
        'location': 'us-east-1',
        'resource_id': 'i-1234567890abcdef0',
        'expire_at': future_date,
    }
    
    resp = sre_client.request(
        '/resources/exceptions',
        method='POST',
        auth=system_user_token,
        data=request_data,
    )
    
    assert resp.status_int == 200
    
    assert 'data' in resp.json
    data = resp.json['data']
    
    assert data['customer_name'] == request_data['customer_id']
    assert data['tenant_name'] == request_data['tenant_name']
    assert data['resource_type'] == request_data['resource_type']
    assert data['location'] == request_data['location']
    assert data['resource_id'] == request_data['resource_id']
    assert 'id' in data
    assert 'created_at' in data
    assert 'updated_at' in data
    assert 'expire_at' in data


def test_update_resource_exception_success(
    system_user_token, sre_client, sample_resource_exception, main_customer, aws_tenant
):
    """
    Test successful update of a resource exception
    """
    # Calculate a future date (30 days from now)
    future_date = (datetime.now() + timedelta(days=30)).isoformat()

    sample_resource_exception.save()
    
    update_data = {
        'customer_id': main_customer.name,
        'tenant_name': aws_tenant.name,
        'resource_type': sample_resource_exception.resource_type,
        'location': 'us-west-2',
        'resource_id': 'i-updated123456789abcdef',
        'expire_at': future_date,
    }
    
    resp = sre_client.request(
        f'/resources/exceptions/{sample_resource_exception.id}',
        method='PUT',
        auth=system_user_token,
        data=update_data,
    )
    
    assert resp.status_int == 200
    
    assert 'data' in resp.json
    data = resp.json['data']
    
    assert data['id'] == sample_resource_exception.id
    assert data['customer_name'] == update_data['customer_id']
    assert data['tenant_name'] == update_data['tenant_name']
    assert data['location'] == update_data['location']
    assert data['resource_id'] == update_data['resource_id']


@pytest.mark.parametrize(
    'exception_params',
    [
        {'resource_type': 'aws.ec2', 'location': 'us-east-1', 'resource_id': 'i-duplicate-test-123'},
        {'arn': 'arn:aws:ec2:us-east-1:123456789012:instance/i-duplicate-arn-test'},
        {'tags_filters': ['Environment=Production', 'Team=DevOps']},
    ],
    ids=['resource_id', 'arn', 'tags_filters']
)
def test_create_resource_exception_duplicate_conflict(
    system_user_token, sre_client, main_customer, aws_tenant, exception_params
):
    """
    Test that creating a duplicate resource exception returns 409 CONFLICT
    """
    future_date = (datetime.now() + timedelta(days=30)).isoformat()

    request_data = {
        'customer_id': main_customer.name,
        'tenant_name': aws_tenant.name,
        'expire_at': future_date,
        **exception_params
    }
    
    resp1 = sre_client.request(
        '/resources/exceptions',
        method='POST',
        auth=system_user_token,
        data=request_data,
    )
    
    assert resp1.status_int == 200
    assert 'data' in resp1.json
    first_exception_id = resp1.json['data']['id']
    
    resp2 = sre_client.request(
        '/resources/exceptions',
        method='POST',
        auth=system_user_token,
        data=request_data,
    )
    
    assert resp2.status_int == 409
    assert 'message' in resp2.json
    assert 'already exists' in resp2.json['message']
    assert first_exception_id in resp2.json['message']


def test_delete_resource_exception_success(
    system_user_token, sre_client, sample_resource_exception, main_customer
):
    """
    Test successful deletion of a resource exception
    """
    sample_resource_exception.save()
    
    resp = sre_client.request(
        f'/resources/exceptions/{sample_resource_exception.id}',
        method='DELETE',
        auth=system_user_token,
        data={
            'customer_id': main_customer.name
        },
    )
    
    assert resp.status_int == 200
    
    assert 'message' in resp.json
    assert sample_resource_exception.id in resp.json['message']
    assert 'deleted' in resp.json['message']
    
    get_resp = sre_client.request(
        f'/resources/exceptions/{sample_resource_exception.id}',
        auth=system_user_token,
        data={
            'customer_id': main_customer.name
        },
    )
    
    assert get_resp.status_int == 404
