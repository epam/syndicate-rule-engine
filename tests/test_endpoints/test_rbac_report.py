import io
from datetime import UTC, datetime
from urllib.parse import unquote

import openpyxl
import pytest
import requests

from helpers.constants import K8S_RBAC_RESOURCE_TYPES, Cloud, JobType
from services import SP
from services.reports_bucket import PlatformReportsBucketKeysBuilder
from services.sharding import ShardPart, ShardsCollectionFactory, ShardsS3IO

RULE = 'ecc-k8s-001-cluster_role_wildcard'


@pytest.fixture()
def rbac_job(k8s_platform, create_k8s_platform_job):
    job = create_k8s_platform_job(k8s_platform, datetime.now(UTC))
    job.save()
    return job


@pytest.fixture()
def rbac_collection(k8s_platform, rbac_job):
    collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
    collection.put_parts(
        [
            ShardPart(
                policy=RULE,
                location='global',
                resources=[
                    {
                        'metadata': {'name': 'super', 'uid': 'super'},
                        'rules': [
                            {
                                'api_groups': ['*'],
                                'resources': ['*'],
                                'verbs': ['*'],
                            }
                        ],
                    }
                ],
            )
        ]
    )
    collection.meta = {
        RULE: {
            'resource': 'k8s.cluster-role',
            'description': 'Minimize wildcard use',
        }
    }
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=PlatformReportsBucketKeysBuilder(k8s_platform).job_result(
            rbac_job
        ),
        client=SP.s3,
    )
    collection.write_all()

    latest = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
    latest.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=PlatformReportsBucketKeysBuilder(k8s_platform).latest_key(),
        client=SP.s3,
    )
    latest.meta = collection.meta
    latest.write_meta()
    return collection


@pytest.fixture()
def rbac_snapshot(k8s_platform, rbac_job):
    keys = PlatformReportsBucketKeysBuilder(k8s_platform)
    inventory = {
        resource_type: [] for resource_type in K8S_RBAC_RESOURCE_TYPES
    }
    inventory['k8s.cluster-role'] = [
        {
            'metadata': {'name': 'super', 'uid': 'super'},
            'rules': [
                {'api_groups': ['*'], 'resources': ['*'], 'verbs': ['*']}
            ],
        }
    ]
    inventory['k8s.cluster-role-binding'] = [
        {
            'metadata': {'name': 'crb', 'uid': 'crb'},
            'role_ref': {'kind': 'ClusterRole', 'name': 'super'},
            'subjects': [
                {'kind': 'ServiceAccount', 'name': 'app', 'namespace': 'team'}
            ],
        }
    ]
    inventory['k8s.pod'] = [
        {
            'metadata': {'name': 'app-pod', 'namespace': 'team', 'uid': 'p'},
            'spec': {'service_account_name': 'app'},
        }
    ]
    resource_types = {}
    for resource_type, resources in inventory.items():
        key = keys.rbac_inventory_snapshot_resource_key(
            rbac_job, resource_type
        )
        SP.s3.gz_put_json(
            bucket=SP.environment_service.default_reports_bucket_name(),
            key=key,
            obj=resources,
        )
        resource_types[resource_type] = {'key': key, 'complete': True}
    manifest = {
        'schema_version': 1,
        'platform_id': k8s_platform.id,
        'job_id': rbac_job.id,
        'complete': True,
        'resource_types': resource_types,
    }
    SP.s3.gz_put_json(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys.rbac_inventory_snapshot_manifest_key(rbac_job),
        obj=manifest,
    )
    return manifest


@pytest.fixture()
def rbac_latest_pair(k8s_platform, rbac_job, rbac_collection, rbac_snapshot):
    keys = PlatformReportsBucketKeysBuilder(k8s_platform)
    SP.s3.gz_put_json(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys.latest_rbac_manifest_key(),
        obj={'job_id': rbac_job.id, 'published_at': '2026-01-01T00:00:00Z'},
    )


@pytest.fixture()
def rbac_empty_latest_pair(k8s_platform, rbac_job, rbac_collection):
    keys = PlatformReportsBucketKeysBuilder(k8s_platform)
    resource_types = {}
    for resource_type in K8S_RBAC_RESOURCE_TYPES:
        key = keys.rbac_inventory_snapshot_resource_key(
            rbac_job, resource_type
        )
        SP.s3.gz_put_json(
            bucket=SP.environment_service.default_reports_bucket_name(),
            key=key,
            obj=[],
        )
        resource_types[resource_type] = {'key': key, 'complete': True}
    SP.s3.gz_put_json(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys.rbac_inventory_snapshot_manifest_key(rbac_job),
        obj={
            'platform_id': k8s_platform.id,
            'job_id': rbac_job.id,
            'complete': True,
            'resource_types': resource_types,
        },
    )
    SP.s3.gz_put_json(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys.latest_rbac_manifest_key(),
        obj={'job_id': rbac_job.id, 'published_at': '2026-01-01T00:00:00Z'},
    )


def _url(platform, job_id=None):
    if job_id:
        return f'/reports/rbac/jobs/{job_id}'
    return f'/reports/rbac/platforms/k8s/{platform.id}/latest'


def _sheets(content) -> dict[str, list[dict]]:
    """
    Downloads the report and returns the rows of each sheet as dicts keyed
    by the column headers
    """
    body = requests.get(content['url']).content
    book = openpyxl.load_workbook(io.BytesIO(body))
    result = {}
    for sheet in book.worksheets:
        header, *rows = sheet.iter_rows(min_row=5, values_only=True)
        result[sheet.title] = [dict(zip(header, row)) for row in rows]
    return result


def test_rbac_report(
    sre_client,
    system_user_token,
    k8s_platform,
    rbac_job,
    rbac_latest_pair,
):
    resp = sre_client.request(
        _url(k8s_platform),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 200, resp.json
    content = resp.json['data']
    assert content['format'] == 'xlsx'
    assert content['platform_id'] == k8s_platform.id
    assert content['job_id'] == rbac_job.id
    assert 'warnings' not in content
    filename = f'rbac-platform-{k8s_platform.id}-job-{rbac_job.id}.xlsx'
    assert filename in unquote(content['url'])
    [(sheet, [row])] = _sheets(content).items()
    assert sheet == 'ecc-k8s-001'
    assert row['Subject kind'] == 'ServiceAccount'
    assert row['Subject name'] == 'app'
    assert row['Effective namespace'] == 'cluster-wide'
    assert row['Pod names'] == 'app-pod'
    assert row['Binding name'] == 'crb'


def test_rbac_report_ignores_json_format(
    sre_client, system_user_token, k8s_platform, rbac_latest_pair
):
    resp = sre_client.request(
        _url(k8s_platform),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer, 'format': 'json'},
    )
    assert resp.status_int == 200, resp.json
    assert resp.json['data']['format'] == 'xlsx'


def test_rbac_report_empty_inventory(
    sre_client, system_user_token, k8s_platform, rbac_empty_latest_pair
):
    resp = sre_client.request(
        _url(k8s_platform),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 200
    content = resp.json['data']
    assert len(content['warnings']) == 1
    assert 'inventory' in content['warnings'][0]
    [[row]] = _sheets(content).values()
    assert row['Subject name'] == '(not bound)'


def test_rbac_report_not_found(sre_client, system_user_token, k8s_platform):
    resp = sre_client.request(
        '/reports/rbac/platforms/k8s/unknown/latest',
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404


def test_rbac_report_job_route(
    sre_client,
    system_user_token,
    k8s_platform,
    rbac_job,
    rbac_collection,
    rbac_snapshot,
):
    resp = sre_client.request(
        _url(k8s_platform, rbac_job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )

    assert resp.status_int == 200, resp.json
    assert resp.json['data']['job_id'] == rbac_job.id


def test_rbac_report_job_not_found(
    sre_client, system_user_token, k8s_platform
):
    resp = sre_client.request(
        '/reports/rbac/jobs/unknown',
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404


def _rewrite_manifest(platform, job, update):
    keys = PlatformReportsBucketKeysBuilder(platform)
    bucket = SP.environment_service.default_reports_bucket_name()
    key = keys.rbac_inventory_snapshot_manifest_key(job)
    manifest = SP.report_service._get_gz_data(key)
    SP.s3.gz_put_json(bucket=bucket, key=key, obj=update(manifest))


def test_rbac_report_without_pods_is_partial(
    sre_client,
    system_user_token,
    k8s_platform,
    rbac_job,
    rbac_collection,
    rbac_snapshot,
):
    def drop_pods(manifest):
        manifest['complete'] = True
        manifest['resource_types']['k8s.pod'] = {
            'key': None,
            'complete': False,
        }
        return manifest

    _rewrite_manifest(k8s_platform, rbac_job, drop_pods)
    resp = sre_client.request(
        _url(k8s_platform, rbac_job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 200, resp.json
    content = resp.json['data']
    [warning] = content['warnings']
    assert 'k8s.pod' in warning
    [[row]] = _sheets(content).values()
    assert row['Subject name'] == 'app'
    assert row['Pods'] == '-'
    assert row['Pod names'] == '-'


@pytest.mark.parametrize(
    'update',
    (
        lambda m: {
            **m,
            'resource_types': {
                **m['resource_types'],
                'k8s.role-binding': {'key': None, 'complete': False},
            },
        },
        lambda m: {**m, 'resource_types': None},
        lambda m: [m],
    ),
    ids=('missing-required-type', 'malformed-types', 'not-an-object'),
)
def test_rbac_report_invalid_snapshot_is_not_found(
    sre_client,
    system_user_token,
    k8s_platform,
    rbac_job,
    rbac_collection,
    rbac_snapshot,
    update,
):
    _rewrite_manifest(k8s_platform, rbac_job, update)
    resp = sre_client.request(
        _url(k8s_platform, rbac_job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404


def test_rbac_report_for_event_driven_job_is_not_found(
    sre_client, system_user_token, k8s_platform, rbac_job
):
    rbac_job.job_type = JobType.REACTIVE
    rbac_job.save()
    resp = sre_client.request(
        _url(k8s_platform, rbac_job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404
    assert 'event-driven' in resp.json['message']
