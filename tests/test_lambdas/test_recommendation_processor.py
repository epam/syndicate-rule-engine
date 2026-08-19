"""
Tests for RecommendationProcessor multi-platform aggregation.
"""

from unittest.mock import MagicMock, patch

import pytest

from lambdas.metrics_updater.processors.recommendation.processor import (
    RecommendationProcessor,
)


def _make_k8s_item(platform_id: str, resource_id: str) -> dict:
    return {
        "resource_id": platform_id,
        "resource_type": "K8S_CLUSTER",
        "source": "SYNDICATE_RULE_ENGINE",
        "severity": "MEDIUM",
        "stats": {
            "scan_date": "2026-08-12",
            "status": "ACTIVE",
            "message": "test",
        },
        "general_actions": [],
        "recommendation": {
            "resource_id": resource_id,
            "resource_type": "POD",
            "article": "ecc-k8s-001",
            "impact": "MEDIUM",
            "description": "test",
        },
    }


def _make_cloud_item(resource_id: str) -> dict:
    return {
        "resource_id": resource_id,
        "resource_type": "INSTANCE",
        "source": "SYNDICATE_RULE_ENGINE",
        "severity": "HIGH",
        "stats": {
            "scan_date": "2026-08-12",
            "status": "ACTIVE",
            "message": "test",
        },
        "general_actions": [],
        "recommendation": {
            "article": "ecc-aws-001",
            "impact": "HIGH",
            "description": "test",
        },
    }


def _make_platform(name: str, tenant_name: str, region: str = "eu-central-1") -> MagicMock:
    platform = MagicMock()
    platform.name = name
    platform.id = f"platform-{name}"
    platform.tenant_name = tenant_name
    platform.region = region
    platform.customer = "test-customer"
    return platform


def _make_tenant(
    name: str = "test-tenant",
    project: str = "123456789012",
    cloud: str = "AWS",
    customer_name: str = "test-customer",
) -> MagicMock:
    tenant = MagicMock()
    tenant.name = name
    tenant.project = project
    tenant.cloud = cloud
    tenant.customer_name = customer_name
    return tenant


def _build_processor() -> RecommendationProcessor:
    return RecommendationProcessor(
        environment_service=MagicMock(),
        s3=MagicMock(),
        assume_role_s3=MagicMock(),
        modular_client=MagicMock(),
        cadf_event_sender=MagicMock(),
        license_service=MagicMock(),
        metadata_provider=MagicMock(),
        report_service=MagicMock(),
        platform_service=MagicMock(),
    )


class TestMergeK8sRecommendations:
    def test_extends_lists_per_region(self):
        target = {"eu-central-1": [_make_k8s_item("p1", "pod-a")]}
        source = {
            "eu-central-1": [_make_k8s_item("p2", "pod-b")],
            "us-east-1": [_make_k8s_item("p2", "pod-c")],
        }

        RecommendationProcessor._merge_k8s_recommendations(target, source)

        assert len(target["eu-central-1"]) == 2
        assert target["eu-central-1"][0]["recommendation"]["resource_id"] == "pod-a"
        assert target["eu-central-1"][1]["recommendation"]["resource_id"] == "pod-b"
        assert len(target["us-east-1"]) == 1
        assert target["us-east-1"][0]["recommendation"]["resource_id"] == "pod-c"


class TestMultiPlatformAggregation:
    @pytest.fixture
    def processor(self):
        return _build_processor()

    @pytest.fixture
    def tenant(self):
        return _make_tenant()

    def test_multiple_platforms_aggregated_before_save(self, processor, tenant):
        """Both platforms' K8s items must appear in a single save per region."""
        platform1 = _make_platform("cluster-a", tenant.name)
        platform2 = _make_platform("cluster-b", tenant.name)

        item1 = _make_k8s_item(platform1.id, "pod-a")
        item2 = _make_k8s_item(platform2.id, "pod-b")

        processor._get_cluster_parents = MagicMock(
            return_value={
                platform1.name: platform1,
                platform2.name: platform2,
            }
        )
        processor._modular_client.tenant_service.return_value.get.return_value = tenant
        processor._license_service.get_customer_metadata.return_value = MagicMock()
        processor._s3.common_prefixes.return_value = []

        def k8s_side_effect(platform, metadata):
            if platform.name == "cluster-a":
                return {"eu-central-1": [item1]}
            return {"eu-central-1": [item2]}

        processor._get_platform_k8s_recommendations = MagicMock(
            side_effect=k8s_side_effect
        )
        processor._get_tenant_recommendations = MagicMock(return_value={})
        processor._save_recommendation = MagicMock()
        processor._send_event_to_maestro = MagicMock()

        with (
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "PlatformReportsBucketKeysBuilder"
            ) as mock_platform_kb,
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "TenantReportsBucketKeysBuilder"
            ) as mock_tenant_kb,
        ):
            mock_platform_kb.return_value.latest_key.return_value = "k8s/latest"
            mock_tenant_kb.return_value.latest_key.return_value = "tenant/latest"
            processor._process_data()

        assert processor._save_recommendation.call_count == 1
        save_kwargs = processor._save_recommendation.call_args.kwargs
        assert save_kwargs["region"] == "eu-central-1"
        assert save_kwargs["tenant"] is tenant

        content = save_kwargs["content"]
        assert "pod-a" in content
        assert "pod-b" in content
        assert platform1.id in content
        assert platform2.id in content

        assert processor._send_event_to_maestro.call_count == 1
        maestro_kwargs = processor._send_event_to_maestro.call_args.kwargs
        assert maestro_kwargs["tenant"] is tenant

    def test_cloud_and_multi_platform_k8s_merged(self, processor, tenant):
        """Cloud recs plus both platforms' K8s recs must be saved together."""
        platform1 = _make_platform("cluster-a", tenant.name)
        platform2 = _make_platform("cluster-b", tenant.name)

        item1 = _make_k8s_item(platform1.id, "pod-a")
        item2 = _make_k8s_item(platform2.id, "pod-b")
        cloud_item = _make_cloud_item("i-123")

        processor._get_cluster_parents = MagicMock(
            return_value={
                platform1.name: platform1,
                platform2.name: platform2,
            }
        )
        processor._modular_client.tenant_service.return_value.get.return_value = tenant
        processor._license_service.get_customer_metadata.return_value = MagicMock()
        processor._s3.common_prefixes.return_value = []

        def k8s_side_effect(platform, metadata):
            if platform.name == "cluster-a":
                return {"eu-central-1": [item1]}
            return {"eu-central-1": [item2]}

        processor._get_platform_k8s_recommendations = MagicMock(
            side_effect=k8s_side_effect
        )
        processor._get_tenant_recommendations = MagicMock(
            return_value={"eu-central-1": [cloud_item]}
        )
        processor._save_recommendation = MagicMock()
        processor._send_event_to_maestro = MagicMock()

        with (
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "PlatformReportsBucketKeysBuilder"
            ) as mock_platform_kb,
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "TenantReportsBucketKeysBuilder"
            ) as mock_tenant_kb,
        ):
            mock_platform_kb.return_value.latest_key.return_value = "k8s/latest"
            mock_tenant_kb.return_value.latest_key.return_value = "tenant/latest"
            processor._process_data()

        assert processor._save_recommendation.call_count == 1
        content = processor._save_recommendation.call_args.kwargs["content"]
        assert "i-123" in content
        assert "pod-a" in content
        assert "pod-b" in content
        assert processor._send_event_to_maestro.call_count == 1

    def test_platforms_with_different_regions_all_saved(self, processor, tenant):
        """Recommendations for distinct platform regions must all be persisted."""
        platform1 = _make_platform("cluster-a", tenant.name, region="eu-central-1")
        platform2 = _make_platform("cluster-b", tenant.name, region="us-east-1")

        item1 = _make_k8s_item(platform1.id, "pod-a")
        item2 = _make_k8s_item(platform2.id, "pod-b")

        processor._get_cluster_parents = MagicMock(
            return_value={
                platform1.name: platform1,
                platform2.name: platform2,
            }
        )
        processor._modular_client.tenant_service.return_value.get.return_value = tenant
        processor._license_service.get_customer_metadata.return_value = MagicMock()
        processor._s3.common_prefixes.return_value = []

        def k8s_side_effect(platform, metadata):
            if platform.name == "cluster-a":
                return {"eu-central-1": [item1]}
            return {"us-east-1": [item2]}

        processor._get_platform_k8s_recommendations = MagicMock(
            side_effect=k8s_side_effect
        )
        # Cloud recs in a region that does not match either platform must not
        # wipe K8s data from other regions.
        processor._get_tenant_recommendations = MagicMock(
            return_value={"eu-west-1": [_make_cloud_item("i-west")]}
        )
        processor._save_recommendation = MagicMock()
        processor._send_event_to_maestro = MagicMock()

        with (
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "PlatformReportsBucketKeysBuilder"
            ) as mock_platform_kb,
            patch(
                "lambdas.metrics_updater.processors.recommendation.processor."
                "TenantReportsBucketKeysBuilder"
            ) as mock_tenant_kb,
        ):
            mock_platform_kb.return_value.latest_key.return_value = "k8s/latest"
            mock_tenant_kb.return_value.latest_key.return_value = "tenant/latest"
            processor._process_data()

        saved_by_region = {
            call.kwargs["region"]: call.kwargs["content"]
            for call in processor._save_recommendation.call_args_list
        }
        assert set(saved_by_region) == {"eu-central-1", "us-east-1", "eu-west-1"}
        assert "pod-a" in saved_by_region["eu-central-1"]
        assert "pod-b" in saved_by_region["us-east-1"]
        assert "i-west" in saved_by_region["eu-west-1"]
        assert processor._send_event_to_maestro.call_count == 1
