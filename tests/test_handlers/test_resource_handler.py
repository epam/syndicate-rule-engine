from unittest.mock import Mock, patch

import pytest
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers.lambda_response import SREException
from handlers.resource_handler import ResourceHandler
from helpers.constants import Cloud
from validators.swagger_request_models import ResourcesGetModel


@pytest.fixture
def valid_event():
    """Create a valid event with all correct parameters."""
    event = Mock(spec=ResourcesGetModel)
    event.customer_id = 'test_customer'
    event.tenant_name = 'test_tenant'
    event.resource_type = 'ec2'
    event.location = 'us-east-1'
    return event


@pytest.fixture
def invalid_event_bad_resource_type():
    """Create an event with an unsupported resource type."""
    event = Mock(spec=ResourcesGetModel)
    event.customer_id = 'test_customer'
    event.tenant_name = 'test_tenant'
    event.resource_type = 'unsupported_resource'
    event.location = 'us-east-1'
    return event


@pytest.fixture
def invalid_event_bad_location():
    """Create an event with an invalid location."""
    event = Mock(spec=ResourcesGetModel)
    event.customer_id = 'test_customer'
    event.tenant_name = 'test_tenant'
    event.resource_type = 'ec2'
    event.location = 'invalid-region'
    return event


@pytest.fixture
def invalid_event_mismatch_of_clouds():
    """Create an event with resource type and tenant from different clouds."""
    event = Mock(spec=ResourcesGetModel)
    event.customer_id = 'test_customer'
    event.tenant_name = 'test_tenant'
    event.resource_type = 'azure.vm'
    event.location = 'us-east-1'
    return event


@pytest.fixture
def mock_modular_service():
    """Mock modular service with customer and tenant services."""
    mock_service = Mock()
    mock_service.customer_service.return_value = Mock()
    mock_service.tenant_service.return_value = Mock()
    return mock_service


@pytest.fixture
def mock_resources_service():
    """Mock resources service."""
    mock_service = Mock()
    mock_service.get_resource_types_by_cloud.return_value = [
        'aws.ec2',
        'aws.s3',
        'azure.vm',
        'gcp.compute',
    ]

    def cloud_to_prefix_side_effect(cloud):
        if cloud == Cloud.AWS:
            return 'aws'
        elif cloud == Cloud.AZURE:
            return 'azure'
        elif cloud == Cloud.GOOGLE:
            return 'gcp'
        else:
            return str(cloud).lower()

    mock_service.cloud_to_prefix.side_effect = cloud_to_prefix_side_effect
    return mock_service


@pytest.fixture
def resource_handler(mock_modular_service, mock_resources_service):
    """Create ResourceHandler instance with mocked dependencies."""
    return ResourceHandler(
        modular_service=mock_modular_service,
        resources_service=mock_resources_service,
    )


@pytest.fixture
def mock_customer():
    """Mock customer object."""
    customer = Mock(spec=Customer)
    customer.name = 'test_customer'
    return customer


@pytest.fixture
def mock_tenant():
    """Mock tenant object."""
    tenant = Mock(spec=Tenant)
    tenant.name = 'test_tenant'
    tenant.customer_name = 'test_customer'
    tenant.cloud = 'AWS'
    return tenant


@pytest.fixture
def mock_regions():
    """Mock region validation functions."""
    with (
        patch(
            'handlers.resource_handler.get_region_by_cloud_with_global'
        ) as mock_get_regions,
        patch(
            'handlers.resource_handler.AllRegionsWithGlobal'
        ) as mock_all_regions,
    ):
        mock_get_regions.return_value = ['us-east-1', 'us-west-2', 'global']
        mock_all_regions.__contains__ = lambda self, item: item in [
            'us-east-1',
            'us-west-2',
            'eu-west-1',
            'global',
        ]
        yield mock_get_regions, mock_all_regions


def test_validate_event_valid(
    resource_handler, valid_event, mock_customer, mock_tenant, mock_regions
):
    """Test validation of a valid event with all correct parameters."""
    # resource_handler._ms.customer_service().get.return_value = mock_customer
    resource_handler._ms.tenant_service().get.return_value = mock_tenant
    resource_handler._rs.get_resource_types_by_cloud.return_value = [
        'aws.ec2'
    ]  # Include the expected resource type
    mock_get_regions, _ = mock_regions
    mock_get_regions.return_value = ['us-east-1']

    resource_handler._validate_event(valid_event)

    assert valid_event.resource_type == 'aws.ec2'


def test_validate_event_bad_resource_type(
    resource_handler,
    invalid_event_bad_resource_type,
    mock_customer,
    mock_tenant,
):
    """Test validation of an event with unsupported resource type."""
    resource_handler._ms.customer_service().get.return_value = mock_customer
    resource_handler._ms.tenant_service().get.return_value = mock_tenant
    resource_handler._rs.get_resource_types_by_cloud.return_value = [
        'aws.ec2',
        'aws.s3',
    ]  # Don't include the unsupported type

    with pytest.raises(
        SREException,
        match='Resource type aws.unsupported_resource is not supported for cloud AWS',
    ):
        resource_handler._validate_event(invalid_event_bad_resource_type)


def test_validate_event_bad_location(
    resource_handler,
    invalid_event_bad_location,
    mock_customer,
    mock_tenant,
    mock_regions,
):
    """Test validation of an event with invalid location."""
    resource_handler._ms.customer_service().get.return_value = mock_customer
    resource_handler._ms.tenant_service().get.return_value = mock_tenant
    resource_handler._rs.get_resource_types_by_cloud.return_value = ['aws.ec2']
    mock_get_regions, _ = mock_regions
    mock_get_regions.return_value = ['us-east-1', 'us-west-2']

    with pytest.raises(
        SREException,
        match='Location invalid-region is not supported for cloud AWS',
    ):
        resource_handler._validate_event(invalid_event_bad_location)


def test_validate_event_mismatch_of_clouds(
    resource_handler,
    invalid_event_mismatch_of_clouds,
    mock_customer,
    mock_tenant,
):
    """Test validation of an event with resource type and location from different clouds."""
    resource_handler._ms.customer_service().get.return_value = mock_customer
    resource_handler._ms.tenant_service().get.return_value = mock_tenant

    with pytest.raises(
        SREException,
        match='Resource type azure.vm does not match tenant cloud AWS',
    ):
        resource_handler._validate_event(invalid_event_mismatch_of_clouds)

