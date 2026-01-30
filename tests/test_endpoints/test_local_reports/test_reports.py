import pytest

from executor.services.report_service import JobResult
from helpers.constants import JobState, Cloud, PolicyErrorType
from helpers.time_helper import utc_datetime
from services import SP
from services.reports_bucket import (
    TenantReportsBucketKeysBuilder,
    StatisticsBucketKeysBuilder,
)
from services.sharding import ShardsCollectionFactory, ShardsS3IO
from ...commons import dicts_equal


@pytest.fixture(autouse=True)
def aws_job(aws_tenant, aws_scan_result, create_tenant_job):
    # don't need to keep results of individual jobs, but need their statistics
    failed = {
        (
            'eu-west-1',
            'ecc-aws-427-rds_cluster_without_tag_information',
        ): (
            PolicyErrorType.ACCESS,
            'AccessDenied Exception',
            [],
        ),
        ('global', 'ecc-aws-527-waf_global_webacl_not_empty'): (
            PolicyErrorType.ACCESS,
            'AccessDenied Exception',
            [],
        ),
    }

    job = create_tenant_job(aws_tenant, utc_datetime(), JobState.SUCCEEDED)
    job.save()

    # prepare data for pipeline
    result = JobResult(aws_scan_result, Cloud.AWS)
    collection = ShardsCollectionFactory.from_cloud(Cloud.AWS)
    collection.put_parts(result.iter_shard_parts(failed))
    collection.meta = result.rules_meta()
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=TenantReportsBucketKeysBuilder(aws_tenant).latest_key(),
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()

    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=TenantReportsBucketKeysBuilder(aws_tenant).job_result(job),
        client=SP.s3,
    )
    collection.write_all()

    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(
            aws_tenant,
            failed
        ),
    )
    return job


def test_digest_report_aws_job(system_user_token, sre_client, aws_job):
    resp = sre_client.request(
        f'/reports/digests/jobs/{aws_job.id}',
        auth=system_user_token,
        data={
            'customer_id': aws_job.customer_name
        },  # on behalf because system
    )
    assert resp.status_int == 200
    assert resp.json == {
        'data': {
            'content': {
                'failed_checks': {'severity': {'Unknown': 10}, 'total': 10},
                'successful_checks': 3,
                'total_checks': 13,
                'violating_resources': 23,
            },
            'customer_name': 'TEST_CUSTOMER',
            'format': 'json',
            'job_id': aws_job.id,
            'job_type': 'standard',
            'obfuscated': False,
            'tenant_name': 'AWS-TESTING',
        }
    }


def test_details_report_aws_job(
    system_user_token, sre_client, aws_job, load_expected
):
    resp = sre_client.request(
        f'/reports/details/jobs/{aws_job.id}',
        auth=system_user_token,
        data={
            'customer_id': aws_job.customer_name
        },  # on behalf because system
    )
    assert resp.status_int == 200
    expected = load_expected('aws_details_report')
    expected['data']['job_id'] = aws_job.id
    assert dicts_equal(resp.json, expected)


def test_errors_report_aws_job(
    system_user_token, sre_client, aws_job, load_expected
):
    resp = sre_client.request(
        f'/reports/errors/jobs/{aws_job.id}',
        auth=system_user_token,
        data={
            'customer_id': aws_job.customer_name
        },  # on behalf because system
    )
    assert resp.status_int == 200

    def key(x):
        return x['policy']
    returned = resp.json
    expected = load_expected('aws_job_errors')
    returned['items'].sort(key=key)
    expected['items'].sort(key=key)
    assert returned == expected


def test_raw_report_aws_job(
    system_user_token, sre_client, aws_tenant, aws_job
):
    resp = sre_client.request(
        f'/reports/raw/tenants/{aws_tenant.name}/state/latest',
        auth=system_user_token,
        data={
            'customer_id': aws_job.customer_name
        },  # on behalf because system
    )
    assert resp.status_int == 200
    # todo test presigned url?
