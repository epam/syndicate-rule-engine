from unittest.mock import Mock, patch

import pytest
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers.lambda_response import SREException
from handlers.resource_handler import ResourceHandler
from helpers.constants import Cloud, Endpoint, HTTPMethod
from services.platform_service import Platform
from validators.swagger_request_models import (
    PlatformK8sResourcesGetModel,
    ResourcesGetModel,
)


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
def mock_platform_service():
    """Mock platform service."""
    return Mock()


@pytest.fixture
def resource_handler(
    mock_modular_service, mock_resources_service, mock_platform_service
):
    """Create ResourceHandler instance with mocked dependencies."""
    return ResourceHandler(
        modular_service=mock_modular_service,
        resources_service=mock_resources_service,
        platform_service=mock_platform_service,
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
def mock_platform():
    """Mock k8s platform object."""
    platform = Mock(spec=Platform)
    platform.id = 'platform-1'
    platform.customer = 'test_customer'
    platform.tenant_name = 'test_tenant'
    return platform


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


# --------------------------------------------------------------------------
# K8S platform inventory
# --------------------------------------------------------------------------
def test_platform_resources_endpoint_is_mapped(resource_handler):
    """The handler exposes the k8s platform inventory endpoint."""
    mapping = resource_handler.mapping
    assert Endpoint.PLATFORMS_K8S_ID_RESOURCES in mapping
    assert (
        mapping[Endpoint.PLATFORMS_K8S_ID_RESOURCES][HTTPMethod.GET]
        == resource_handler.get_platform_resources
    )


def test_validate_k8s_resource_type_adds_prefix(resource_handler):
    """Resource type without a prefix gets the k8s one."""
    resource_handler._rs.cloud_to_prefix.side_effect = None
    resource_handler._rs.cloud_to_prefix.return_value = 'k8s'
    resource_handler._rs.get_resource_types_by_cloud.return_value = ['k8s.pod']

    assert resource_handler._validate_k8s_resource_type('pod') == 'k8s.pod'
    assert resource_handler._validate_k8s_resource_type('k8s.pod') == 'k8s.pod'
    assert resource_handler._validate_k8s_resource_type(None) is None


def test_validate_k8s_resource_type_rejects_other_cloud(resource_handler):
    """A non-k8s resource type is rejected."""
    resource_handler._rs.cloud_to_prefix.side_effect = None
    resource_handler._rs.cloud_to_prefix.return_value = 'k8s'
    resource_handler._rs.get_resource_types_by_cloud.return_value = ['k8s.pod']

    with pytest.raises(
        SREException, match='is not a KUBERNETES resource type'
    ):
        resource_handler._validate_k8s_resource_type('aws.ec2')


def test_validate_k8s_resource_type_rejects_unknown(resource_handler):
    """An unknown k8s resource type is rejected."""
    resource_handler._rs.cloud_to_prefix.side_effect = None
    resource_handler._rs.cloud_to_prefix.return_value = 'k8s'
    resource_handler._rs.get_resource_types_by_cloud.return_value = ['k8s.pod']

    with pytest.raises(SREException, match='is not supported for cloud'):
        resource_handler._validate_k8s_resource_type('k8s.not-a-thing')


def test_get_platform_not_found(resource_handler):
    """Unknown platform id results in 404."""
    resource_handler._ps.get_nullable.return_value = None

    with pytest.raises(SREException, match='Platform platform-1 not found'):
        resource_handler._get_platform('platform-1', 'test_customer')


def test_get_platform_wrong_customer(resource_handler, mock_platform):
    """A platform of another customer is not visible."""
    resource_handler._ps.get_nullable.return_value = mock_platform

    with pytest.raises(SREException, match='Platform platform-1 not found'):
        resource_handler._get_platform('platform-1', 'another_customer')


def test_get_platform_resources_queries_by_platform_id(
    resource_handler, mock_platform
):
    """Resources are queried scoped to the platform, not to a region."""
    resource_handler._rs.cloud_to_prefix.side_effect = None
    resource_handler._rs.cloud_to_prefix.return_value = 'k8s'
    resource_handler._rs.get_resource_types_by_cloud.return_value = ['k8s.pod']

    iterator = Mock()
    iterator.__iter__ = Mock(return_value=iter([]))
    iterator.last_evaluated_key = None
    resource_handler._rs.get_resources.return_value = iterator

    event = PlatformK8sResourcesGetModel(
        resource_type='pod', namespace='kube-system'
    )
    event.customer_id = 'test_customer'

    resource_handler.get_platform_resources.__wrapped__(
        resource_handler,
        event=event,
        platform_id='platform-1',
        platform_obj=mock_platform,
    )

    kwargs = resource_handler._rs.get_resources.call_args.kwargs
    assert kwargs['platform_id'] == 'platform-1'
    assert kwargs['resource_type'] == 'k8s.pod'
    assert kwargs['namespace'] == 'kube-system'
    assert kwargs['customer_name'] == 'test_customer'
    assert 'location' not in kwargs
    # the already resolved platform must not be queried again
    resource_handler._ps.get_nullable.assert_not_called()


def test_build_resource_dto_includes_k8s_fields(resource_handler):
    """platform_id and namespace are exposed for k8s resources."""
    resource = Mock()
    resource.id = 'uid-1'
    resource.name = 'my-pod'
    resource.location = 'global'
    resource.resource_type = 'k8s.pod'
    resource.tenant_name = 'test_tenant'
    resource.customer_name = 'test_customer'
    resource.data = {'a': 'b'}
    resource.sync_date = 0
    resource.arn = None
    resource.platform_id = 'platform-1'
    resource.namespace = 'kube-system'

    dto = resource_handler._build_resource_dto(resource)

    assert dto['platform_id'] == 'platform-1'
    assert dto['namespace'] == 'kube-system'
    assert 'arn' not in dto


def test_build_resource_dto_omits_empty_k8s_fields(resource_handler):
    """Non-k8s resources do not get empty k8s keys."""
    resource = Mock()
    resource.id = 'i-1'
    resource.name = 'inst'
    resource.location = 'us-east-1'
    resource.resource_type = 'aws.ec2'
    resource.tenant_name = 'test_tenant'
    resource.customer_name = 'test_customer'
    resource.data = {}
    resource.sync_date = 0
    resource.arn = 'arn:aws:ec2:::i-1'
    resource.platform_id = None
    resource.namespace = None

    dto = resource_handler._build_resource_dto(resource)

    assert dto['arn'] == 'arn:aws:ec2:::i-1'
    assert 'platform_id' not in dto
    assert 'namespace' not in dto
