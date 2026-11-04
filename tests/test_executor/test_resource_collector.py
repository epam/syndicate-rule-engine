from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from helpers.constants import GLOBAL_REGION, Cloud, ResourcesCollectorType


class TestCustodianResourceCollector:
    """Tests for CustodianResourceCollector class."""

    def test_collector_type(self):
        """Collector has correct type."""
        from executor.resource_collector import CustodianResourceCollector

        assert (
            CustodianResourceCollector.collector_type
            == ResourcesCollectorType.CUSTODIAN
        )

    @patch("executor.resource_collector.collector.SP")
    def test_build_creates_instance(self, mock_sp):
        """build() creates a properly configured instance."""
        from executor.resource_collector import CustodianResourceCollector

        mock_sp.modular_client = MagicMock()
        mock_sp.resources_service = MagicMock()
        mock_sp.license_service = MagicMock()
        mock_sp.platform_service = MagicMock()
        mock_sp.modular_client.tenant_settings_service.return_value = MagicMock()

        collector = CustodianResourceCollector.build()

        assert isinstance(collector, CustodianResourceCollector)
        assert collector._ms == mock_sp.modular_client
        assert collector._rs == mock_sp.resources_service
        assert collector._ls == mock_sp.license_service
        assert collector._ps == mock_sp.platform_service


@pytest.fixture
def collector():
    from executor.resource_collector import CustodianResourceCollector

    return CustodianResourceCollector(
        modular_service=MagicMock(),
        resources_service=MagicMock(),
        license_service=MagicMock(),
        tenant_settings_service=MagicMock(),
        platform_service=MagicMock(),
    )


@pytest.fixture
def tenant():
    t = Mock()
    t.name = "TEST_TENANT"
    t.customer_name = "test_customer"
    t.project = "123456789012"
    return t


@pytest.fixture
def platform():
    p = Mock()
    p.id = "platform-1"
    p.tenant_name = "TEST_TENANT"
    return p


class TestSaveResourcesToDb:
    """Resources of k8s platforms are stored scoped to the platform."""

    def _run(self, collector, tenant, platform, tmp_path):
        iterator = Mock()
        iterator.iterate.return_value = iter([])

        scan_result = Mock()
        scan_result.iter_resources.return_value = [
            (GLOBAL_REGION, "k8s.pod", [{"a": 1}])
        ]

        with (
            patch(
                "executor.resource_collector.collector.ScanResult",
                return_value=scan_result,
            ),
            patch(
                "executor.resource_collector.collector.get_resource_iterator",
                return_value=iterator,
            ),
        ):
            collector._save_resources_to_db(
                tenant=tenant,
                cloud=Cloud.KUBERNETES,
                work_dir=Path(tmp_path),
                platform=platform,
            )
        return iterator

    def test_platform_resources_use_platform_scoped_cleanup(
        self, collector, tenant, platform, tmp_path
    ):
        self._run(collector, tenant, platform, tmp_path)

        collector._rs.remove_platform_resources.assert_called_once_with(
            platform_id="platform-1", resource_type="k8s.pod"
        )
        collector._rs.remove_policy_resources.assert_not_called()

    def test_platform_id_is_passed_to_iterator(
        self, collector, tenant, platform, tmp_path
    ):
        iterator = self._run(collector, tenant, platform, tmp_path)

        kwargs = iterator.iterate.call_args.kwargs
        assert kwargs["platform_id"] == "platform-1"
        # the platform id plays the role of an account id for k8s
        assert kwargs["account_id"] == "platform-1"
        assert kwargs["location"] == GLOBAL_REGION
        assert kwargs["tenant_name"] == "TEST_TENANT"

    def test_tenant_resources_use_region_scoped_cleanup(
        self, collector, tenant, tmp_path
    ):
        iterator = Mock()
        iterator.iterate.return_value = iter([])

        scan_result = Mock()
        scan_result.iter_resources.return_value = [
            ("us-east-1", "aws.ec2", [{"a": 1}])
        ]

        with (
            patch(
                "executor.resource_collector.collector.ScanResult",
                return_value=scan_result,
            ),
            patch(
                "executor.resource_collector.collector.get_resource_iterator",
                return_value=iterator,
            ),
        ):
            collector._save_resources_to_db(
                tenant=tenant, cloud=Cloud.AWS, work_dir=Path(tmp_path)
            )

        collector._rs.remove_policy_resources.assert_called_once_with(
            account_id="123456789012",
            location="us-east-1",
            resource_type="aws.ec2",
        )
        collector._rs.remove_platform_resources.assert_not_called()
        assert iterator.iterate.call_args.kwargs["platform_id"] is None


class TestCollectPlatform:
    def test_scans_kubernetes_globally_and_cleans_kubeconfig(
        self, collector, tenant, platform, tmp_path
    ):
        kubeconfig = tmp_path / "kubeconfig.json"
        kubeconfig.write_text("{}")

        collector._platform_credentials = Mock(
            return_value={"KUBECONFIG": str(kubeconfig)}
        )
        collector._scan_all_regions = Mock(return_value=[])
        collector._save_resources_to_db = Mock(return_value=7)

        saved = collector._collect_platform(
            tenant=tenant, platform=platform, resource_types=None
        )

        assert saved == 7
        scan_kwargs = collector._scan_all_regions.call_args.kwargs
        assert scan_kwargs["cloud"] == Cloud.KUBERNETES
        assert scan_kwargs["regions"] == {GLOBAL_REGION}

        save_kwargs = collector._save_resources_to_db.call_args.kwargs
        assert save_kwargs["platform"] is platform

        # the temporary kubeconfig must not be left behind
        assert not kubeconfig.exists()

    def test_raises_without_credentials(self, collector, tenant, platform):
        collector._platform_credentials = Mock(return_value=None)

        with pytest.raises(ValueError, match="No credentials for platform"):
            collector._collect_platform(
                tenant=tenant, platform=platform, resource_types=None
            )


class TestCollectTenantPlatforms:
    def test_aggregates_saved_resources(self, collector, tenant, platform):
        second = Mock()
        second.id = "platform-2"
        collector._ps.query_by_tenant.return_value = [platform, second]
        collector._collect_platform = Mock(side_effect=[3, 4])

        total, failed = collector._collect_tenant_platforms(
            tenant=tenant, resource_types=None
        )

        assert total == 7
        assert failed == []

    def test_one_failing_platform_does_not_stop_others(
        self, collector, tenant, platform
    ):
        second = Mock()
        second.id = "platform-2"
        collector._ps.query_by_tenant.return_value = [platform, second]
        collector._collect_platform = Mock(
            side_effect=[ValueError("boom"), 5]
        )

        total, failed = collector._collect_tenant_platforms(
            tenant=tenant, resource_types=None
        )

        assert total == 5
        assert failed == ["platform-1"]

    def test_no_platforms(self, collector, tenant):
        collector._ps.query_by_tenant.return_value = []

        total, failed = collector._collect_tenant_platforms(
            tenant=tenant, resource_types=None
        )

        assert total == 0
        assert failed == []

    def test_non_k8s_resource_types_skip_platforms(self, collector, tenant):
        """Explicitly requested non-k8s types must not trigger a k8s scan."""
        collector._collect_platform = Mock()

        total, failed = collector._collect_tenant_platforms(
            tenant=tenant, resource_types={'aws.ec2', 'azure.vm'}
        )

        assert (total, failed) == (0, [])
        collector._ps.query_by_tenant.assert_not_called()
        collector._collect_platform.assert_not_called()

    def test_only_k8s_resource_types_are_forwarded(
        self, collector, tenant, platform
    ):
        collector._ps.query_by_tenant.return_value = [platform]
        collector._collect_platform = Mock(return_value=1)

        collector._collect_tenant_platforms(
            tenant=tenant, resource_types={'aws.ec2', 'k8s.pod', 'pod'}
        )

        types = collector._collect_platform.call_args.kwargs['resource_types']
        assert set(types) == {'k8s.pod', 'pod'}
