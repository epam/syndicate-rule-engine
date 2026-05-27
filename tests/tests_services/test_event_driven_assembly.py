"""Tests for event-driven assembly (vendor index, K8s refs, strategies)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

from helpers.constants import Cloud
from services.event_driven.assembly.assembly_index import AssemblyBucketKey
from services.event_driven.assembly.assembly_service import (
    EventDrivenAssemblyService,
    restrict_rules_to_scan,
)
from services.event_driven.assembly.resource_refs import K8sResourceRef
from services.event_driven.assembly.strategies import (
    ResourceRefStrategyCatalog,
)
from services.event_driven.assembly.strategies.k8s import (
    KubernetesPlatformPolicyBundleStrategy,
    KubernetesResourceRefStrategy,
)
from services.event_driven.domain import VendorKind
from services.event_driven.domain.models import KubernetesMetadata


def _k8s_ref(
    uid: str,
    *,
    name: str = 'pod-a',
    namespace: str | None = 'ns1',
    kind: str = 'Pod',
) -> K8sResourceRef:
    return K8sResourceRef(
        metadata=KubernetesMetadata(
            resource_uid=uid,
            kind=kind,
            name=name,
            namespace=namespace,
        )
    )


class TestKubernetesPlatformPolicyBundleStrategyScanRows:
    def test_single_uid_one_entry(self) -> None:
        refs = [_k8s_ref('uid-1')]
        rows = KubernetesPlatformPolicyBundleStrategy._scan_rows_for_rule_refs(
            refs
        )
        assert len(rows) == 1
        assert rows[0].namespace == 'ns1'
        assert rows[0].uids == 'uid-1'

    def test_multiple_uids_same_namespace_merges_to_uid_list(self) -> None:
        refs = [
            _k8s_ref('uid-1', name='a'),
            _k8s_ref('uid-2', name='b'),
        ]
        rows = KubernetesPlatformPolicyBundleStrategy._scan_rows_for_rule_refs(
            refs
        )
        assert len(rows) == 1
        assert rows[0].namespace == 'ns1'
        assert rows[0].name is None
        assert set(rows[0].uids) == {'uid-1', 'uid-2'}

    def test_missing_rule_gets_empty_policy_entries(self) -> None:
        req = KubernetesPlatformPolicyBundleStrategy._build_request_for_scanned_rules(
            ['r1', 'r2'],
            {'r1': (_k8s_ref('u1'),)},
        )
        assert 'r1' in req.policies
        assert 'r2' in req.policies
        assert req.policies['r2'] == []


class TestRestrictRulesToScan:
    def test_intersection(self) -> None:
        assert sorted(restrict_rules_to_scan(['a', 'b', 'c'], {'b', 'd'})) == [
            'b'
        ]


class TestKubernetesResourceRefStrategy:
    def test_non_k8s_via_catalog_returns_none(self) -> None:
        cat = ResourceRefStrategyCatalog.default()
        rec = SimpleNamespace(
            cloud=Cloud.AWS.value,
            metadata={'resource_uid': 'x'},
        )
        assert cat.for_cloud(Cloud.AWS.value).try_extract(rec) is None

    def test_k8s_valid_metadata(self) -> None:
        strat = KubernetesResourceRefStrategy()
        rec = SimpleNamespace(
            cloud=Cloud.KUBERNETES.value,
            metadata={
                'resource_uid': 'u1',
                'kind': 'Pod',
                'name': 'p',
                'namespace': 'ns',
            },
        )
        ref = strat.try_extract(rec)
        assert ref is not None
        assert isinstance(ref, K8sResourceRef)
        assert ref.metadata.resource_uid == 'u1'


class TestBuildVendorRuleIndex:
    def test_merges_rules_and_k8s_refs_in_same_bucket(self) -> None:
        ed_rules = Mock()
        ed_rules.get_rules.return_value = {'rule-a'}

        svc = EventDrivenAssemblyService(
            tenant_service=MagicMock(),
            platform_service=MagicMock(),
            job_service=MagicMock(),
            environment_service=MagicMock(),
            ed_rules_service=ed_rules,
            get_license=MagicMock(),
            get_ruleset=MagicMock(),
            submit_event_driven_jobs=MagicMock(),
        )
        svc.resolve_tenant_name = Mock(return_value='tenant-1')

        ev1 = SimpleNamespace(
            vendor=VendorKind.AWS,
            events=[
                SimpleNamespace(
                    cloud=Cloud.KUBERNETES.value,
                    region_name='global',
                    source_name='s',
                    event_name='e',
                    platform_id='plat-1',
                    tenant_name=None,
                    metadata={
                        'resource_uid': 'uid-1',
                        'kind': 'Pod',
                        'name': 'p1',
                        'namespace': 'ns1',
                    },
                ),
            ],
        )
        ev2 = SimpleNamespace(
            vendor=VendorKind.AWS,
            events=[
                SimpleNamespace(
                    cloud=Cloud.KUBERNETES.value,
                    region_name='global',
                    source_name='s',
                    event_name='e2',
                    platform_id='plat-1',
                    tenant_name=None,
                    metadata={
                        'resource_uid': 'uid-2',
                        'kind': 'Pod',
                        'name': 'p2',
                        'namespace': 'ns1',
                    },
                ),
            ],
        )

        index = svc.build_vendor_rule_index([ev1, ev2])
        key = AssemblyBucketKey(platform_id='plat-1', region_name='global')
        bucket = index.cloud_assembly_map(VendorKind.AWS)[
            Cloud.KUBERNETES.value
        ]['tenant-1'][key]
        assert bucket.rules == {'rule-a'}
        assert set(bucket.refs_by_rule['rule-a']) == {
            K8sResourceRef(
                metadata=KubernetesMetadata(
                    resource_uid='uid-1',
                    kind='Pod',
                    name='p1',
                    namespace='ns1',
                )
            ),
            K8sResourceRef(
                metadata=KubernetesMetadata(
                    resource_uid='uid-2',
                    kind='Pod',
                    name='p2',
                    namespace='ns1',
                )
            ),
        }

    def test_k8s_platform_lookup_cached_within_index_build(self) -> None:
        platform = SimpleNamespace(tenant_name='tenant-plat')
        platform_service = MagicMock()
        platform_service.get_nullable.return_value = platform
        resolved_tenant = SimpleNamespace(name='tenant-plat')
        tenant_service = MagicMock()
        tenant_service.get.return_value = resolved_tenant

        ed_rules = Mock()
        ed_rules.get_rules.return_value = {'r1'}

        svc = EventDrivenAssemblyService(
            tenant_service=tenant_service,
            platform_service=platform_service,
            job_service=MagicMock(),
            environment_service=MagicMock(),
            ed_rules_service=ed_rules,
            get_license=MagicMock(),
            get_ruleset=MagicMock(),
            submit_event_driven_jobs=MagicMock(),
        )

        k8s_rec = dict(
            cloud=Cloud.KUBERNETES.value,
            region_name='global',
            source_name='s',
            platform_id='plat-same',
            tenant_name=None,
            metadata=None,
        )
        ev1 = SimpleNamespace(
            vendor=VendorKind.AWS,
            events=[SimpleNamespace(**k8s_rec, event_name='e1')],
        )
        ev2 = SimpleNamespace(
            vendor=VendorKind.AWS,
            events=[SimpleNamespace(**k8s_rec, event_name='e2')],
        )

        svc.build_vendor_rule_index([ev1, ev2])
        assert platform_service.get_nullable.call_count == 1

    def test_aws_resolves_tenant_name_from_account_id(self) -> None:
        tenant = SimpleNamespace(name='tn-from-acc')
        tenant_service = MagicMock()
        tenant_service.i_get_by_acc.return_value = iter([tenant])

        ed_rules = Mock()
        ed_rules.get_rules.return_value = {'r-aws'}

        svc = EventDrivenAssemblyService(
            tenant_service=tenant_service,
            platform_service=MagicMock(),
            job_service=MagicMock(),
            environment_service=MagicMock(),
            ed_rules_service=ed_rules,
            get_license=MagicMock(),
            get_ruleset=MagicMock(),
            submit_event_driven_jobs=MagicMock(),
        )

        ev = SimpleNamespace(
            vendor=VendorKind.AWS,
            events=[
                SimpleNamespace(
                    cloud=Cloud.AWS.value,
                    region_name='eu-central-1',
                    source_name='ssm.amazonaws.com',
                    event_name='UpdateInstanceInformation',
                    platform_id=None,
                    account_id='323549576358',
                    tenant_name=None,
                    metadata=None,
                )
            ],
        )
        index = svc.build_vendor_rule_index([ev])
        tenant_service.i_get_by_acc.assert_called_once()
        key = AssemblyBucketKey(platform_id=None, region_name='eu-central-1')
        bucket = index.cloud_assembly_map(VendorKind.AWS)[Cloud.AWS.value][
            'tn-from-acc'
        ][key]
        assert bucket.rules == {'r-aws'}


class TestApplyLicensesFiltersRefs:
    def test_filters_rule_ref_map_to_restricted_rules(self) -> None:
        from modular_sdk.models.tenant import Tenant

        from services.event_driven.assembly.job_rule_refs import JobRuleRefs

        tenant = Mock(spec=Tenant)
        tenant.name = 't1'
        tenant.customer_name = 'c1'
        tenant.cloud = Cloud.AWS.value

        job = Mock()
        job.platform_id = None
        job.rules_to_scan = ['r_keep', 'r_drop']
        job.tenant_name = 't1'

        ref_map = JobRuleRefs(
            by_rule={
                'r_keep': frozenset({_k8s_ref('u1')}),
                'r_drop': frozenset({_k8s_ref('u2')}),
            },
        )

        lic = Mock()
        lic.license_key = 'L1'
        lic.ruleset_ids = ['rs1']
        lic.event_driven = {'active': True}

        ruleset = Mock()
        ruleset.cloud = Cloud.AWS.value
        ruleset.rules = ['r_keep']

        svc = EventDrivenAssemblyService(
            tenant_service=MagicMock(),
            platform_service=MagicMock(),
            job_service=MagicMock(),
            environment_service=MagicMock(),
            ed_rules_service=MagicMock(),
            get_license=lambda _t: lic,
            get_ruleset=lambda _id: ruleset,
            submit_event_driven_jobs=MagicMock(),
        )

        allowed = svc._apply_licenses_and_filter_rule_refs(
            [(tenant, job, ref_map)]
        )
        assert len(allowed) == 1
        out_job, out_refs = allowed[0]
        assert set(out_job.rules_to_scan) == {'r_keep'}
        assert out_refs is not None
        assert set(out_refs.by_rule.keys()) == {'r_keep'}
        assert len(out_refs.by_rule['r_keep']) == 1
