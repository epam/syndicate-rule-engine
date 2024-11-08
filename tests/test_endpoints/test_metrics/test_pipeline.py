"""
Metrics pipeline collects data for the current week for all customers and
tenants. These tests check whether the data is processed as we expect
"""
import time
from datetime import timedelta

import pytest

from executor.services.report_service import JobResult
from helpers.constants import JobState, Cloud, PolicyErrorType
from services import SP
from services.reports_bucket import TenantReportsBucketKeysBuilder, \
    StatisticsBucketKeysBuilder
from services.sharding import ShardsCollectionFactory, ShardsS3IO
from ...commons import AWS_ACCOUNT_ID, AZURE_ACCOUNT_ID, GOOGLE_ACCOUNT_ID, dicts_equal


@pytest.fixture()
def aws_jobs(aws_tenant, aws_scan_result, create_tenant_job, create_tenant_br,
             report_bounds):
    # don't need to keep results of individual jobs, but need their statistics
    start = report_bounds[0]

    job = create_tenant_job(aws_tenant, start + timedelta(minutes=120),
                            JobState.SUCCEEDED)
    create_tenant_job(aws_tenant, start + timedelta(days=1),
                      JobState.FAILED).save()
    br = create_tenant_br(aws_tenant, start + timedelta(minutes=180),
                          JobState.SUCCEEDED)

    job.save()
    br.save()

    # prepare data for pipeline
    result = JobResult(aws_scan_result, Cloud.AWS)
    collection = ShardsCollectionFactory.from_cloud(Cloud.AWS)
    collection.put_parts(result.iter_shard_parts())
    collection.meta = result.rules_meta()
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=TenantReportsBucketKeysBuilder(aws_tenant).latest_key(),
        client=SP.s3
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(aws_tenant, {
            ("eu-west-1", "ecc-aws-427-rds_cluster_without_tag_information"): (
                PolicyErrorType.ACCESS,
                "AccessDenied Exception",
                [],
            ),
            ("global", "ecc-aws-527-waf_global_webacl_not_empty"): (
                PolicyErrorType.ACCESS,
                "AccessDenied Exception",
                [],
            ),
        })
    )


@pytest.fixture()
def azure_jobs(azure_tenant, azure_scan_result, create_tenant_job,
               create_tenant_br,
               report_bounds):
    # don't need to keep results of individual jobs, but need their statistics
    start = report_bounds[0]

    job = create_tenant_job(azure_tenant, start + timedelta(minutes=300),
                            JobState.SUCCEEDED)
    create_tenant_job(azure_tenant, start + timedelta(minutes=60),
                      JobState.FAILED).save()
    br = create_tenant_br(azure_tenant, start + timedelta(minutes=900),
                          JobState.SUCCEEDED)

    job.save()
    br.save()

    # prepare data for pipeline
    result = JobResult(azure_scan_result, Cloud.AZURE)
    collection = ShardsCollectionFactory.from_cloud(Cloud.AZURE)
    collection.put_parts(result.iter_shard_parts())
    collection.meta = result.rules_meta()
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=TenantReportsBucketKeysBuilder(azure_tenant).latest_key(),
        client=SP.s3
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(azure_tenant, {
            ('global', 'ecc-azure-125-nsg_mysql'): (
                PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
            ('global', 'ecc-azure-313-cis_postgresql_log_min_messages'): (
                PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
        })
    )


@pytest.fixture()
def google_jobs(google_tenant, google_scan_result, create_tenant_job,
                create_tenant_br,
                report_bounds):
    # don't need to keep results of individual jobs, but need their statistics
    start = report_bounds[0]
    job = create_tenant_job(google_tenant, start + timedelta(minutes=400),
                            JobState.SUCCEEDED)
    create_tenant_job(google_tenant, start + timedelta(minutes=360),
                      JobState.FAILED).save()
    br = create_tenant_br(google_tenant, start + timedelta(minutes=500),
                          JobState.SUCCEEDED)

    job.save()
    br.save()

    # prepare data for pipeline
    result = JobResult(google_scan_result, Cloud.GOOGLE)
    collection = ShardsCollectionFactory.from_cloud(Cloud.GOOGLE)
    collection.put_parts(result.iter_shard_parts())
    collection.meta = result.rules_meta()
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=TenantReportsBucketKeysBuilder(google_tenant).latest_key(),
        client=SP.s3
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(google_tenant, {
            ('global', 'ecc-gcp-027-dnssec_for_cloud_dns'): (
                PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
            ('global', 'ecc-gcp-101-load_balancer_access_logging_disabled'): (
                PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
        })
    )


def test_metrics_update_denied(sre_client):
    resp = sre_client.request('/metrics/update', 'POST')
    assert resp.status_int == 401
    assert resp.json == {'message': 'Unauthorized'}


@pytest.mark.slow
def test_metrics_update(
        sre_client,
        system_user_token,
        aws_jobs,
        azure_jobs,
        google_jobs,
        report_bounds,
        load_expected
):
    """
    This test case has three tenants one for each cloud. Each tenant has
    two standard jobs (one failed) and one event driven job during the current
    collecting period.
    """
    resp = sre_client.request('/metrics/update', 'POST',
                              auth=system_user_token)
    assert resp.status_int == 202
    assert resp.json == {'message': 'Metrics update has been submitted'}
    time.sleep(4)  # don't know how to check underlying thread is finished
    # here we check only tenant metrics processor outcome
    _, end = report_bounds
    end = end.date()

    # validating tenant metrics results
    aws_data = SP.s3.gz_get_json('metrics',
                                 f'TEST_CUSTOMER/accounts/{end.isoformat()}/{AWS_ACCOUNT_ID}.json')
    assert aws_data, 'AWS data must not be empty'
    assert dicts_equal(aws_data, load_expected('metrics/aws_account'))

    azure_data = SP.s3.gz_get_json('metrics',
                                   f'TEST_CUSTOMER/accounts/{end.isoformat()}/{AZURE_ACCOUNT_ID}.json')
    assert azure_data, 'AZURE data must not be empty'
    assert dicts_equal(azure_data, load_expected('metrics/azure_account'))

    google_data = SP.s3.gz_get_json('metrics',
                                    f'TEST_CUSTOMER/accounts/{end.isoformat()}/{GOOGLE_ACCOUNT_ID}.json')
    assert google_data, 'GOOGLE data must not be empty'
    assert dicts_equal(google_data, load_expected('metrics/google_account'))

    # todo validated whether montly metrics are collected
    # todo validate weekly scan statistics

    # validating tenant group metrics results
    group_data = SP.s3.gz_get_json('metrics', f'TEST_CUSTOMER/tenants/{end.isoformat()}/testing.json')
    assert group_data, 'Group data must not be empty'
    assert dicts_equal(group_data, load_expected('metrics/tenant_group'))

    # validation tenant
