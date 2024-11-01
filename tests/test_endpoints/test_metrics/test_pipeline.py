"""
Metrics pipeline collects data for the current week for all customers and
tenants. These tests check whether the data is processed as we expect
"""
import time
import uuid
from datetime import datetime, timezone

import pytest
from dateutil.relativedelta import relativedelta, SU
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from executor.services.report_service import JobResult
from helpers.constants import JobState, Cloud, PolicyErrorType
from helpers.time_helper import utc_iso
from models.batch_results import BatchResults
from models.job import Job
from models.setting import Setting
from services import SP
from services.reports_bucket import TenantReportsBucketKeysBuilder, \
    StatisticsBucketKeysBuilder
from services.setting_service import SettingKey
from services.sharding import ShardsCollectionFactory, ShardsS3IO
from ...commons import AWS_ACCOUNT_ID, AZURE_ACCOUNT_ID, GOOGLE_ACCOUNT_ID


@pytest.fixture()
def main_customer(mocked_mongo_client) -> Customer:
    customer = Customer(
        name='TEST_CUSTOMER',
        display_name='test customer',
        admins=[],
        is_active=True
    )
    customer.save()
    return customer


@pytest.fixture()
def aws_tenant(main_customer):
    tenant = Tenant(
        name='AWS-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='AWS',
        project=AWS_ACCOUNT_ID,
        contacts={},
        activation_date=datetime.now(timezone.utc)
    )
    tenant.save()
    return tenant


@pytest.fixture()
def azure_tenant(main_customer):
    tenant = Tenant(
        name='AZURE-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='AZURE',
        project=AZURE_ACCOUNT_ID,
        contacts={},
        activation_date=datetime.now(timezone.utc)
    )
    tenant.save()
    return tenant


@pytest.fixture()
def google_tenant(main_customer):
    tenant = Tenant(
        name='GOOGLE-TESTING',
        display_name='testing',
        display_name_to_lower='testing',
        read_only=False,
        is_active=True,
        customer_name=main_customer.name,
        cloud='GOOGLE',
        project=GOOGLE_ACCOUNT_ID,
        contacts={},
        activation_date=datetime.now(timezone.utc)
    )
    tenant.save()
    return tenant


@pytest.fixture()
def reports_marker():
    """
    Sets marker for current week from Sunday till Sunday
    """
    now = datetime.now().date()
    start = now + relativedelta(weekday=SU(-1))
    end = now + relativedelta(weekday=SU(+1))
    Setting(
        name=SettingKey.REPORT_DATE_MARKER,
        value={
            "current_week_date": end.isoformat(),
            "last_week_date": start.isoformat()
        }
    ).save()
    Setting(
        name=SettingKey.SEND_REPORTS,
        value=True
    ).save()


@pytest.fixture()
def create_tenant_job():
    def factory(tenant: Tenant, status: JobState = JobState.SUCCEEDED) -> Job:
        return Job(
            id=str(uuid.uuid4()),
            batch_job_id='batch_job_id',
            tenant_name=tenant.name,
            customer_name=tenant.customer_name,
            status=status.value,
            created_at=utc_iso(),
            started_at=utc_iso(),
            stopped_at=utc_iso(),
            rulesets=['TESTING']
        )

    return factory


@pytest.fixture()
def create_tenant_br():
    def factory(tenant: Tenant,
                status: JobState = JobState.SUCCEEDED) -> BatchResults:
        return BatchResults(
            id=str(uuid.uuid4()),
            job_id='batch_job_id',
            status=status.value,
            cloud_identifier=tenant.project,
            tenant_name=tenant.name,
            customer_name=tenant.customer_name,
            stopped_at=utc_iso(),
        )

    return factory


@pytest.fixture()
def aws_jobs(aws_tenant, aws_scan_result, create_tenant_job, create_tenant_br):
    # don't need to keep results of individual jobs, but need their statistics
    job = create_tenant_job(aws_tenant, JobState.SUCCEEDED)
    create_tenant_job(aws_tenant, JobState.FAILED).save()
    br = create_tenant_br(aws_tenant, JobState.SUCCEEDED)

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
def azure_jobs(azure_tenant, azure_scan_result, create_tenant_job, create_tenant_br):
    # don't need to keep results of individual jobs, but need their statistics
    job = create_tenant_job(azure_tenant, JobState.SUCCEEDED)
    create_tenant_job(azure_tenant, JobState.FAILED).save()
    br = create_tenant_br(azure_tenant, JobState.SUCCEEDED)

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
            ('global', 'ecc-azure-125-nsg_mysql'): (PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
            ('global', 'ecc-azure-313-cis_postgresql_log_min_messages'): (PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
        })
    )


@pytest.fixture()
def google_jobs(google_tenant, google_scan_result, create_tenant_job, create_tenant_br):
    # don't need to keep results of individual jobs, but need their statistics
    job = create_tenant_job(google_tenant, JobState.SUCCEEDED)
    create_tenant_job(google_tenant, JobState.FAILED).save()
    br = create_tenant_br(google_tenant, JobState.SUCCEEDED)

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
            ('global', 'ecc-gcp-027-dnssec_for_cloud_dns'): (PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
            ('global', 'ecc-gcp-101-load_balancer_access_logging_disabled'): (PolicyErrorType.ACCESS.value, 'AccessDenied exception', []),
        })
    )


def test_metrics_update_denied(sre_client):
    resp = sre_client.request('/metrics/update', 'POST')
    assert resp.status_int == 401
    assert resp.json == {'message': 'Unauthorized'}


def test_metrics_update(
        sre_client,
        system_user_token,
        s3_buckets,
        reports_marker,
        aws_jobs,
        azure_jobs,
        google_jobs
):
    """
    This test case has three tenants one for each cloud. Each tenant has
    two standard jobs (one failed) and one event driven job during the current
    collecting period.
    """
    resp = sre_client.request('/metrics/update', 'POST',
                              auth=system_user_token)
    # probably sleep
