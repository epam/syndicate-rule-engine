import pytest
from unittest.mock import patch, MagicMock

from c7n.resources.resource_map import ResourceMap as AWSResourceMap
from c7n_azure.resources.resource_map import ResourceMap as AzureResourceMap
from c7n_gcp.resources.resource_map import ResourceMap as GCPResourceMap
from c7n_kube.resources.resource_map import ResourceMap as K8sResourceMap

from helpers.constants import Cloud, EXCLUDE_RESOURCE_TYPES
from executor.job.resource_collector.collector import (
    _resolve_resource_types,
    _get_resource_types,
    RegionTask,
    CollectMode,
)


class TestResolveResourceTypes:
    """Tests for _resolve_resource_types function."""

    def test_returns_full_scope_when_no_filters(self):
        """When included is None, returns all resources minus excluded."""
        scope = {"aws.ec2", "aws.s3", "aws.lambda"}
        excluded = {"aws.lambda"}

        result = _resolve_resource_types(
            cloud=Cloud.AWS,
            scope=scope,
            included=None,
            excluded=excluded,
        )

        assert result == {"aws.ec2", "aws.s3"}

    def test_filters_by_included_types(self):
        """When included is specified, only those types are returned."""
        scope = {"aws.ec2", "aws.s3", "aws.lambda", "aws.rds"}

        result = _resolve_resource_types(
            cloud=Cloud.AWS,
            scope=scope,
            included={"aws.ec2", "aws.s3"},
            excluded=None,
        )

        assert result == {"aws.ec2", "aws.s3"}

    def test_adds_provider_prefix_if_missing(self):
        """Adds cloud provider prefix to resource types if not present."""
        scope = {"aws.ec2", "aws.s3"}

        result = _resolve_resource_types(
            cloud=Cloud.AWS,
            scope=scope,
            included={"ec2"},  # Missing 'aws.' prefix
            excluded=None,
        )

        assert result == {"aws.ec2"}

    def test_excludes_types(self):
        """Excluded types are removed from result."""
        scope = {"aws.ec2", "aws.s3", "aws.lambda"}

        result = _resolve_resource_types(
            cloud=Cloud.AWS,
            scope=scope,
            included=None,
            excluded={"aws.s3"},
        )

        assert "aws.s3" not in result
        assert "aws.ec2" in result


class TestGetResourceTypes:
    """Tests for _get_resource_types function."""

    def test_aws_resource_types(self):
        """Returns AWS resource types from ResourceMap."""
        result = _get_resource_types(cloud=Cloud.AWS, included=None)

        assert len(result) > 0
        assert all(rt.startswith("aws.") for rt in result)
        assert all(rt in AWSResourceMap for rt in result)

        for excluded in EXCLUDE_RESOURCE_TYPES:
            assert excluded not in result

    def test_azure_resource_types(self):
        """Returns Azure resource types from ResourceMap."""
        result = _get_resource_types(cloud=Cloud.AZURE, included=None)

        assert len(result) > 0
        assert all(rt.startswith("azure.") for rt in result)
        assert all(rt in AzureResourceMap for rt in result)

    def test_gcp_resource_types(self):
        """Returns GCP resource types from ResourceMap."""
        result = _get_resource_types(cloud=Cloud.GOOGLE, included=None)

        assert len(result) > 0
        assert all(rt.startswith("gcp.") for rt in result)
        assert all(rt in GCPResourceMap for rt in result)

    def test_gcp_alias(self):
        """Cloud.GCP alias works the same as Cloud.GOOGLE."""
        result_google = _get_resource_types(cloud=Cloud.GOOGLE, included=None)
        result_gcp = _get_resource_types(cloud=Cloud.GCP, included=None)

        assert result_google == result_gcp

    def test_k8s_resource_types(self):
        """Returns K8s resource types from ResourceMap."""
        result = _get_resource_types(cloud=Cloud.KUBERNETES, included=None)

        assert len(result) > 0
        assert all(rt.startswith("k8s.") for rt in result)
        assert all(rt in K8sResourceMap for rt in result)

    def test_k8s_alias(self):
        """Cloud.K8S alias works the same as Cloud.KUBERNETES."""
        result_k8s = _get_resource_types(cloud=Cloud.K8S, included=None)
        result_kubernetes = _get_resource_types(cloud=Cloud.KUBERNETES, included=None)

        assert result_k8s == result_kubernetes

    def test_filters_by_included(self):
        """Filters to only included resource types."""
        included = ("aws.ec2", "aws.s3")
        result = _get_resource_types(cloud=Cloud.AWS, included=included)

        assert result == {"aws.ec2", "aws.s3"}


class TestRegionTask:
    """Tests for RegionTask dataclass."""

    def test_creates_immutable_task(self):
        """RegionTask is frozen and immutable."""
        task = RegionTask(
            tenant_name="test-tenant",
            account_id="123456789",
            customer_name="test-customer",
            cloud="AWS",
            region="us-east-1",  # Single region, not tuple
            resource_types=("aws.ec2", "aws.s3"),
            credentials={"AWS_ACCESS_KEY_ID": "test"},
        )

        assert task.tenant_name == "test-tenant"
        assert task.account_id == "123456789"
        assert task.cloud == "AWS"
        assert task.region == "us-east-1"

        with pytest.raises(AttributeError):
            task.tenant_name = "new-name"  # type: ignore

    def test_accepts_none_resource_types(self):
        """RegionTask accepts None for resource_types."""
        task = RegionTask(
            tenant_name="test-tenant",
            account_id="123456789",
            customer_name="test-customer",
            cloud="AWS",
            region="eu-west-1",
            resource_types=None,
            credentials={},
        )

        assert task.resource_types is None


class TestCollectMode:
    """Tests for CollectMode Cloud Custodian execution mode."""

    def test_collect_mode_registered(self):
        """CollectMode is registered with Cloud Custodian."""
        from c7n.policy import execution

        # Verify 'collect' mode is registered
        assert "collect" in execution

    def test_collect_mode_schema(self):
        """CollectMode has correct schema."""
        assert CollectMode.schema is not None
        assert CollectMode.schema["type"] == "object"


class TestCustodianResourceCollector:
    """Tests for CustodianResourceCollector class."""

    def test_collector_type(self):
        """Collector has correct type."""
        from executor.job.resource_collector import CustodianResourceCollector
        from helpers.constants import ResourcesCollectorType

        assert (
            CustodianResourceCollector.collector_type
            == ResourcesCollectorType.CUSTODIAN
        )

    @patch("executor.job.resource_collector.collector.SP")
    def test_build_creates_instance(self, mock_sp):
        """build() creates a properly configured instance."""
        from executor.job.resource_collector import CustodianResourceCollector

        mock_sp.modular_client = MagicMock()
        mock_sp.resources_service = MagicMock()
        mock_sp.license_service = MagicMock()
        mock_sp.modular_client.tenant_settings_service.return_value = MagicMock()

        collector = CustodianResourceCollector.build()

        assert isinstance(collector, CustodianResourceCollector)
        assert collector._ms == mock_sp.modular_client
        assert collector._rs == mock_sp.resources_service
        assert collector._ls == mock_sp.license_service
