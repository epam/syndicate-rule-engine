from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from executor.job.execution.orchestrator import (
    collect_k8s_rbac_snapshot,
    has_rbac_policies,
)
from helpers.constants import (
    K8S_RBAC_OPTIONAL_RESOURCE_TYPES,
    K8S_RBAC_REQUIRED_RESOURCE_TYPES,
    K8S_RBAC_RESOURCE_TYPES,
    Cloud,
)
from services.k8s_rbac_snapshot import RbacSnapshotWriter
from services.reports_bucket import PlatformReportsBucketKeysBuilder

BUILD = (
    'executor.resource_collector.collector.CustodianResourceCollector.build'
)
ORCH = 'executor.job.execution.orchestrator'


def _ctx(platform=True):
    return SimpleNamespace(
        platform=(
            SimpleNamespace(id='p1', customer='customer') if platform else None
        ),
        tenant='tenant',
        job=SimpleNamespace(
            id='j1',
            platform_id='p1',
            job_type='standard',
            submitted_at='2026-01-01T00:00:00Z',
        ),
    )


@pytest.mark.parametrize(
    'resources,expected',
    (
        (['k8s.pod', 'k8s.deployment'], False),
        (['k8s.cluster-role'], True),
        (['role-binding'], True),
        ([], False),
    ),
)
def test_has_rbac_policies(resources, expected):
    policies = [
        {'name': str(i), 'resource': r} for i, r in enumerate(resources)
    ]
    assert has_rbac_policies(policies) is expected


def _collect(ctx, scanned=K8S_RBAC_RESOURCE_TYPES, side_effect=None):
    """
    Runs collect_k8s_rbac_snapshot with a fake collector that feeds the
    given resource types to the snapshot writer. Returns (result, s3 mock)
    """
    warnings = []
    collector = MagicMock()

    def collect_platform(on_resources, **kwargs):
        if side_effect:
            raise side_effect
        for rt in scanned:
            on_resources(rt, [{'metadata': {'name': rt}}])
        return len(scanned)

    collector.collect_platform.side_effect = collect_platform
    with (
        patch(BUILD, return_value=collector),
        patch(f'{ORCH}.SP') as sp,
    ):
        sp.environment_service.default_reports_bucket_name.return_value = (
            'reports'
        )
        result = collect_k8s_rbac_snapshot(ctx, warnings)
    return result, warnings, collector, sp.s3


def test_no_platform():
    warnings = []
    with patch(BUILD) as build:
        result = collect_k8s_rbac_snapshot(_ctx(platform=False), warnings)
    build.assert_not_called()
    assert result is False
    assert warnings == []


def test_plugins_disabled(monkeypatch):
    monkeypatch.delenv('SRE_ENABLE_CUSTOM_CC_PLUGINS', raising=False)
    warnings = []
    with patch(BUILD) as build:
        result = collect_k8s_rbac_snapshot(_ctx(), warnings)
    build.assert_not_called()
    assert result is False
    assert len(warnings) == 1
    assert 'SRE_ENABLE_CUSTOM_CC_PLUGINS' in warnings[0]


def test_collects_complete_snapshot(monkeypatch):
    monkeypatch.setenv('SRE_ENABLE_CUSTOM_CC_PLUGINS', 'true')
    ctx = _ctx()
    result, warnings, collector, s3 = _collect(ctx)

    kwargs = collector.collect_platform.call_args.kwargs
    assert kwargs['resource_types'] == K8S_RBAC_RESOURCE_TYPES
    assert kwargs['platform'] is ctx.platform
    keys = PlatformReportsBucketKeysBuilder(ctx.platform)
    manifest_key = keys.rbac_inventory_snapshot_manifest_key(ctx.job)
    assert result is True
    assert warnings == []
    manifest = s3.gz_put_json.call_args_list[-1].kwargs
    assert manifest['key'] == manifest_key
    assert manifest['obj']['complete'] is True
    s3.gz_get_object.assert_not_called()


def test_incomplete_snapshot_is_not_published(monkeypatch):
    monkeypatch.setenv('SRE_ENABLE_CUSTOM_CC_PLUGINS', 'true')
    result, warnings, _, _ = _collect(
        _ctx(), scanned=K8S_RBAC_REQUIRED_RESOURCE_TYPES[1:]
    )
    assert result is False
    assert warnings == [
        (
            'RBAC inventory snapshot is incomplete: roles or role bindings '
            'could not be collected'
        )
    ]


def test_missing_optional_types_are_warning(monkeypatch):
    monkeypatch.setenv('SRE_ENABLE_CUSTOM_CC_PLUGINS', 'true')
    result, warnings, _, _ = _collect(
        _ctx(), scanned=K8S_RBAC_REQUIRED_RESOURCE_TYPES
    )
    assert result is True
    [warning] = warnings
    assert 'k8s.pod, k8s.replica-set' in warning


def test_collection_failure_is_warning(monkeypatch):
    monkeypatch.setenv('SRE_ENABLE_CUSTOM_CC_PLUGINS', 'true')
    result, warnings, _, _ = _collect(_ctx(), side_effect=RuntimeError('x'))
    assert result is False
    assert warnings == ['Could not collect RBAC inventory snapshot']


class TestRbacSnapshotWriter:
    @staticmethod
    def _writer():
        s3 = MagicMock()
        writer = RbacSnapshotWriter(
            s3_client=s3,
            bucket='reports',
            platform=SimpleNamespace(id='p1', customer='customer'),
            job=SimpleNamespace(
                id='j1',
                platform_id='p1',
                job_type='standard',
                submitted_at='2026-01-01T00:00:00Z',
            ),
        )
        return writer, s3

    def test_writes_slim_resources(self):
        writer, s3 = self._writer()
        pod = {
            'metadata': {
                'name': 'web-1',
                'namespace': 'prod',
                'managed_fields': [{'big': 'x' * 100}],
                'owner_references': [{'kind': 'ReplicaSet', 'name': 'web'}],
            },
            'spec': {'serviceAccountName': 'web', 'containers': [{}]},
            'status': {'phase': 'Running'},
        }
        writer.write('k8s.pod', [pod])
        writer.write('k8s.deployment', [{}])

        s3.gz_put_json.assert_called_once()
        [written] = s3.gz_put_json.call_args.kwargs['obj']
        assert written == {
            'metadata': {
                'name': 'web-1',
                'namespace': 'prod',
                'owner_references': [
                    {'kind': 'ReplicaSet', 'name': 'web', 'controller': True}
                ],
            },
            'spec': {'service_account_name': 'web'},
        }

    def test_manifest(self):
        writer, s3 = self._writer()
        s3.gz_put_json.side_effect = [None, RuntimeError('x'), None, None]
        writer.write('k8s.role', [])
        writer.write('k8s.pod', [])  # fails
        writer.write('k8s.cluster-role', [])
        manifest = writer.finish()

        assert s3.gz_put_json.call_args.kwargs['key'] == writer.manifest_key
        assert manifest['complete'] is False
        assert manifest['platform_id'] == 'p1'
        assert manifest['job_id'] == 'j1'
        statuses = manifest['resource_types']
        assert set(statuses) == set(K8S_RBAC_RESOURCE_TYPES)
        assert statuses['k8s.role']['complete'] is True
        assert statuses['k8s.role']['key'].endswith('k8s.role')
        assert statuses['k8s.pod'] == {'key': None, 'complete': False}
        assert writer.missing_optional_types == list(
            K8S_RBAC_OPTIONAL_RESOURCE_TYPES
        )

    def test_complete_without_optional_types(self):
        writer, _ = self._writer()
        for rt in K8S_RBAC_REQUIRED_RESOURCE_TYPES:
            writer.write(rt, [])
        assert writer.finish()['complete'] is True


def test_finalize_publishes_latest_pair_after_completed_scan():
    from executor.job.execution.job_execution_core import finalize_job_scan

    job = SimpleNamespace(
        id='j1',
        platform_id='p1',
        job_type='standard',
        submitted_at='2026-01-01T00:00:00Z',
        stopped_at='2026-01-01T00:05:00Z',
        scan_checkpoint={},
    )
    ctx = SimpleNamespace(job=job, add_warnings=MagicMock())
    keys = PlatformReportsBucketKeysBuilder(
        SimpleNamespace(id='p1', customer='customer')
    )
    collection = MagicMock()
    collection.iter_parts.return_value = [object()]
    partial_store = MagicMock()
    partial_store.load_partial_collection.return_value = collection
    with (
        patch(
            'executor.job.execution.job_execution_core.cleanup_temporary_credentials'
        ),
        patch(
            'executor.job.execution.job_execution_core.ScanPartialStore',
            return_value=partial_store,
        ),
        patch(
            'executor.job.execution.job_execution_core.finalize_standard_job_reports'
        ),
        patch(
            'executor.job.execution.job_execution_core.SP.environment_service.default_reports_bucket_name',
            return_value='reports',
        ),
        patch(
            'executor.job.execution.job_execution_core.SP.job_service.update'
        ),
        patch(
            'executor.job.execution.job_execution_core.SP.s3.gz_put_json'
        ) as put,
        patch(
            'executor.job.execution.job_execution_core.utc_iso',
            return_value='2026-01-01T00:06:00Z',
        ),
    ):
        finalize_job_scan(
            ctx,
            keys_builder=keys,
            cloud=Cloud.KUBERNETES,
            credentials={},
            regions={'global'},
            successful=1,
            warnings=[],
            failed={},
            completed_regions={'global'},
            rbac_snapshot_ready=True,
        )

    put.assert_called_once_with(
        bucket='reports',
        key=keys.latest_rbac_manifest_key(),
        obj={'job_id': 'j1', 'published_at': '2026-01-01T00:06:00Z'},
    )


def test_finalize_does_not_publish_incomplete_scan_pair():
    from executor.job.execution.job_execution_core import finalize_job_scan

    job = SimpleNamespace(
        id='j1',
        platform_id='p1',
        job_type='standard',
        submitted_at='2026-01-01T00:00:00Z',
        stopped_at='2026-01-01T00:05:00Z',
    )
    ctx = SimpleNamespace(job=job, add_warnings=MagicMock())
    keys = PlatformReportsBucketKeysBuilder(
        SimpleNamespace(id='p1', customer='customer')
    )

    with (
        patch(
            'executor.job.execution.job_execution_core.cleanup_temporary_credentials'
        ),
        patch(
            'executor.job.execution.job_execution_core.finalize_standard_job_reports'
        ),
        patch(
            'executor.job.execution.job_execution_core.SP.environment_service.default_reports_bucket_name',
            return_value='reports',
        ),
        patch(
            'executor.job.execution.job_execution_core.SP.s3.gz_put_json'
        ) as put,
    ):
        finalize_job_scan(
            ctx,
            keys_builder=keys,
            cloud=Cloud.KUBERNETES,
            credentials={},
            regions={'global'},
            successful=1,
            warnings=[],
            failed={},
            completed_regions=set(),
            rbac_snapshot_ready=True,
        )

    put.assert_not_called()


def _run_job(runner, policies):
    from executor.job.execution import orchestrator

    ctx = SimpleNamespace(
        platform=SimpleNamespace(id='p1'),
        tenant='tenant',
        job=SimpleNamespace(id='j1', job_type='x', affected_license=None),
    )
    with (
        patch(
            f'{ORCH}._prepare_job_scan',
            return_value=(Cloud.KUBERNETES, {}, None, policies, {'global'}),
        ),
        patch(
            f'{ORCH}.execute_job_region_scan',
            return_value=(1, [], {}, {'global'}),
        ),
        patch(
            f'{ORCH}.collect_k8s_rbac_snapshot', return_value=True
        ) as collect,
        patch(f'{ORCH}.finalize_job_scan') as finalize,
        patch(f'{ORCH}.SP'),
    ):
        getattr(orchestrator, runner)(ctx)
    return collect, finalize.call_args.kwargs


def test_standard_job_publishes_rbac_snapshot():
    collect, kwargs = _run_job(
        'run_standard_job', [{'name': 'r', 'resource': 'k8s.role'}]
    )
    collect.assert_called_once()
    assert kwargs['rbac_snapshot_ready'] is True


def test_job_without_rbac_policies_skips_snapshot():
    collect, kwargs = _run_job(
        'run_standard_job', [{'name': 'r', 'resource': 'k8s.pod'}]
    )
    collect.assert_not_called()
    assert kwargs['rbac_snapshot_ready'] is False


def test_reactive_job_skips_rbac_snapshot():
    collect, kwargs = _run_job(
        'run_reactive_job', [{'name': 'r', 'resource': 'k8s.role'}]
    )
    collect.assert_not_called()
    assert not kwargs.get('rbac_snapshot_ready')
