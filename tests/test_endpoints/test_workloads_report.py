import io
from datetime import UTC, datetime
from urllib.parse import unquote

import openpyxl
import pytest
import requests

from helpers.constants import Cloud, JobType
from services import SP
from services.reports_bucket import PlatformReportsBucketKeysBuilder
from services.sharding import ShardPart, ShardsCollectionFactory, ShardsS3IO

POD_RULE = 'ecc-k8s-066-privileged_containers'
ROLE_RULE = 'ecc-k8s-001-cluster_role_wildcard'
META = {
    POD_RULE: {'resource': 'k8s.pod', 'description': 'Privileged pods'},
    ROLE_RULE: {
        'resource': 'k8s.cluster-role',
        'description': 'Minimize wildcard use',
    },
}


def _pod(name, owner=None):
    metadata = {'name': name, 'namespace': 'team', 'uid': name}
    if owner:
        metadata['owner_references'] = [
            {'kind': owner[0], 'name': owner[1], 'controller': True}
        ]
    return {
        'metadata': metadata,
        'c7n:MatchedFilters': ['spec.containers[*].securityContext'],
    }


def _parts():
    return [
        ShardPart(
            policy=POD_RULE,
            location='global',
            resources=[
                _pod('web-5d-a', ('ReplicaSet', 'web-5d')),
                _pod('web-5d-b', ('ReplicaSet', 'web-5d')),
                _pod('bare'),
            ],
        ),
        ShardPart(
            policy=ROLE_RULE,
            location='global',
            resources=[{'metadata': {'name': 'super', 'uid': 'super'}}],
        ),
    ]


def _write(key):
    collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
    collection.put_parts(_parts())
    collection.meta = META
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=key,
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()


@pytest.fixture()
def workloads_job(k8s_platform, create_k8s_platform_job):
    job = create_k8s_platform_job(k8s_platform, datetime.now(UTC))
    job.save()
    keys = PlatformReportsBucketKeysBuilder(k8s_platform)
    _write(keys.job_result(job))
    _write(keys.latest_key())
    return job


def _url(platform, report_type, job_id=None):
    if job_id:
        return f'/reports/workloads/{report_type}/jobs/{job_id}'
    return (
        f'/reports/workloads/platforms/k8s/{platform.id}/{report_type}/latest'
    )


def _sheets(content) -> dict[str, list[dict]]:
    body = requests.get(content['url']).content
    book = openpyxl.load_workbook(io.BytesIO(body))
    result = {}
    for sheet in book.worksheets:
        if sheet.title in {'Introduction', 'Workloads'}:
            continue
        header, *rows = sheet.iter_rows(min_row=5, values_only=True)
        result[sheet.title] = [dict(zip(header, row)) for row in rows]
    return result


def test_workloads_detailed_latest(
    sre_client, system_user_token, k8s_platform, workloads_job
):
    resp = sre_client.request(
        _url(k8s_platform, 'detailed'),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 200, resp.json
    content = resp.json['data']
    assert content['format'] == 'xlsx'
    assert content['platform_id'] == k8s_platform.id
    filename = f'workloads-detailed-platform-{k8s_platform.id}-latest.xlsx'
    assert filename in unquote(content['url'])
    [(sheet, rows)] = _sheets(content).items()
    assert sheet == 'ecc-k8s-066'
    assert [
        (r['Pod name'], r['Namespace'], r['Owner kind'], r['Owner name'])
        for r in rows
    ] == [
        ('bare', 'team', 'Pod', 'bare'),
        ('web-5d-a', 'team', 'ReplicaSet', 'web-5d'),
        ('web-5d-b', 'team', 'ReplicaSet', 'web-5d'),
    ]
    assert rows[0]['Matched filters'] == 'spec.containers[*].securityContext'


def test_workloads_summary_job(
    sre_client, system_user_token, k8s_platform, workloads_job
):
    resp = sre_client.request(
        _url(k8s_platform, 'summary', workloads_job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer, 'rule': 'ecc-k8s-066'},
    )
    assert resp.status_int == 200, resp.json
    content = resp.json['data']
    assert content['job_id'] == workloads_job.id
    filename = (
        f'workloads-summary-platform-{k8s_platform.id}'
        f'-job-{workloads_job.id}.xlsx'
    )
    assert filename in unquote(content['url'])
    [rows] = _sheets(content).values()
    assert [
        (r['Owner name'], r['Owner kind'], r['Namespace'], r['Pods'])
        for r in rows
    ] == [('bare', 'Pod', 'team', 1), ('web-5d', 'ReplicaSet', 'team', 2)]


def test_workloads_unknown_rule(
    sre_client, system_user_token, k8s_platform, workloads_job
):
    resp = sre_client.request(
        _url(k8s_platform, 'detailed'),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer, 'rule': 'ecc-k8s-001'},
    )
    assert resp.status_int == 404


def test_workloads_platform_not_found(
    sre_client, system_user_token, k8s_platform
):
    resp = sre_client.request(
        '/reports/workloads/platforms/k8s/unknown/summary/latest',
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404


def test_workloads_ed_job_not_found(
    sre_client,
    system_user_token,
    k8s_platform,
    create_k8s_platform_job,
):
    job = create_k8s_platform_job(k8s_platform, datetime.now(UTC))
    job.job_type = JobType.REACTIVE
    job.save()
    resp = sre_client.request(
        _url(k8s_platform, 'detailed', job.id),
        auth=system_user_token,
        data={'customer_id': k8s_platform.customer},
    )
    assert resp.status_int == 404
