from datetime import datetime, timezone, timedelta

import pytest
from modular_sdk.models.parent import Parent
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud
from models.batch_results import BatchResults
from models.job import Job
from services.clients.s3 import S3Url
from services.platform_service import Platform
from services.reports_bucket import TenantReportsBucketKeysBuilder, \
    ReportsBucketKeysBuilder, PlatformReportsBucketKeysBuilder, \
    StatisticsBucketKeysBuilder, MetricsBucketKeysBuilder


@pytest.fixture
def aws_tenant() -> Tenant:
    return Tenant(
        name='TEST-TENANT',
        display_name='Test tenant',
        is_active=True,
        customer_name='TEST-CUSTOMER',
        cloud='AWS',
        project='123123123123'
    )


@pytest.fixture
def k8s_platform() -> Platform:
    return Platform(
        parent=Parent(
            parent_id='platform_id',
            customer_id='TEST-CUSTOMER',
            type='PLATFORM_K8S',
            description='Test platform',
            meta={
                'name': 'test',
                'region': 'eu-west-1',
                'type': 'EKS'
            },
            is_deleted=False,
            type_scope='PLATFORM_K8S#SPECIFIC#TEST-TENANT'
        )
    )


@pytest.fixture
def standard_job() -> Job:
    return Job(
        id='job_id',
        tenant_name='TEST-TENANT',
        customer_name='TEST-CUSTOMER',
        submitted_at='2023-11-27T14:29:08.694447Z',
        status='SUCCEEDED',
    )


@pytest.fixture
def platform_job() -> Job:
    return Job(
        id='job_id',
        tenant_name='TEST-TENANT',
        customer_name='TEST-CUSTOMER',
        submitted_at='2023-11-27T14:29:08.694447Z',
        status='SUCCEEDED',
        platform_id='platform_id'
    )


@pytest.fixture
def ed_job() -> BatchResults:
    return BatchResults(
        id='job_id',
        tenant_name='TEST-TENANT',
        customer_name='TEST-CUSTOMER',
        submitted_at='2023-11-27T14:29:08.694447Z',
        status='SUCCEEDED',
    )


@pytest.fixture
def tenant_reports_builder(aws_tenant) -> TenantReportsBucketKeysBuilder:
    return TenantReportsBucketKeysBuilder(aws_tenant)


@pytest.fixture
def platform_reports_builder(k8s_platform) -> PlatformReportsBucketKeysBuilder:
    return PlatformReportsBucketKeysBuilder(k8s_platform)


class TestTenantReportsBucketKeyBuilder:
    def test_cloud(self, tenant_reports_builder):
        assert tenant_reports_builder.cloud == Cloud.AWS

    def test_job_result(self, tenant_reports_builder, standard_job):
        res = tenant_reports_builder.job_result(standard_job)
        assert res == 'raw/TEST-CUSTOMER/AWS/123123123123/jobs/standard/2023-11-27-14/job_id/result/'

    def test_ed_job_result(self, tenant_reports_builder, ed_job):
        res = tenant_reports_builder.ed_job_result(ed_job)
        assert res == 'raw/TEST-CUSTOMER/AWS/123123123123/jobs/event-driven/2023-11-27-14/job_id/result/'

    def test_ed_job_difference(self, tenant_reports_builder, ed_job):
        res = tenant_reports_builder.ed_job_difference(ed_job)
        assert res == 'raw/TEST-CUSTOMER/AWS/123123123123/jobs/event-driven/2023-11-27-14/job_id/difference/'

    def test_latest_key(self, tenant_reports_builder):
        res = tenant_reports_builder.latest_key()
        assert res == 'raw/TEST-CUSTOMER/AWS/123123123123/latest/'

    def test_snapshots_folder(self, tenant_reports_builder):
        res = tenant_reports_builder.snapshots_folder()
        assert res == 'raw/TEST-CUSTOMER/AWS/123123123123/snapshots/'

    def test_snapshot_key(self, tenant_reports_builder):
        now = datetime.now(tz=timezone.utc)
        res = tenant_reports_builder.snapshot_key(now)
        assert res == f'raw/TEST-CUSTOMER/AWS/123123123123/snapshots/{now.strftime("%Y-%m-%d-%H")}/'

    def test_one_time_on_demand(self):
        res = ReportsBucketKeysBuilder.one_time_on_demand()
        assert res.startswith('on-demand/')


class TestPlatformReportsBucketKeyBuilder:
    def test_cloud(self, platform_reports_builder):
        assert platform_reports_builder.cloud == Cloud.KUBERNETES

    def test_job_result(self, platform_reports_builder, platform_job):
        res = platform_reports_builder.job_result(platform_job)
        assert res == 'raw/TEST-CUSTOMER/KUBERNETES/test-eu-west-1/jobs/standard/2023-11-27-14/job_id/'

    def test_ed_job(self, platform_reports_builder, ed_job):
        with pytest.raises(NotImplementedError):
            platform_reports_builder.ed_job_result(ed_job)
        with pytest.raises(NotImplementedError):
            platform_reports_builder.ed_job_difference(ed_job)

    def test_latest_key(self, platform_reports_builder):
        res = platform_reports_builder.latest_key()
        assert res == 'raw/TEST-CUSTOMER/KUBERNETES/test-eu-west-1/latest/'

    def test_snapshots_folder(self, platform_reports_builder):
        res = platform_reports_builder.snapshots_folder()
        assert res == 'raw/TEST-CUSTOMER/KUBERNETES/test-eu-west-1/snapshots/'


class TestStatisticsBucketKeyBuilder:
    def test_job_statistics(self, standard_job):
        res = StatisticsBucketKeysBuilder.job_statistics(standard_job)
        assert res == 'job-statistics/standard/job_id/statistics.json'

    def test_ed_job_statistics(self, ed_job):
        res = StatisticsBucketKeysBuilder.job_statistics(ed_job)
        assert res == 'job-statistics/event-driven/job_id/statistics.json'

    def test_report_statistics(self):
        now = datetime.now(timezone.utc)
        res = StatisticsBucketKeysBuilder.report_statistics(
            now=now,
            customer='TEST-CUSTOMER'
        )
        assert res == f'report-statistics/diagnostic/TEST-CUSTOMER/{now.strftime("%Y-%m")}/diagnostic_report.json'

    def test_xray_log(self):
        now = datetime.now(timezone.utc)
        res = StatisticsBucketKeysBuilder.xray_log('job_id')
        assert res == f'xray/executor/{now.year}/{now.month}/{now.day}/job_id.log'


class TestMetricsBucketKeyBuilder:
    def test_account_metrics(self, aws_tenant):
        now = datetime.now(timezone.utc)
        res = MetricsBucketKeysBuilder(aws_tenant).account_metrics(now)
        assert res == f'TEST-CUSTOMER/accounts/{now.date().isoformat()}/123123123123.json'

    def test_account_monthly_metrics(self, aws_tenant):
        now = datetime.now(timezone.utc)
        res = MetricsBucketKeysBuilder(aws_tenant).account_monthly_metrics(now)
        next_m = now.month + 1
        if next_m == 12: next_m = 1
        assert res == f'TEST-CUSTOMER/accounts/monthly/{now.year}-{next_m}-01/123123123123.json'


def test_s3_url():
    url = S3Url('s3://bucket/path/to/file')
    assert url.bucket == 'bucket'
    assert url.key == 'path/to/file'

    url = S3Url('bucket/path/to/file')
    assert url.bucket == 'bucket'
    assert url.key == 'path/to/file'

    url = S3Url('bucket/path/to/file/')
    assert url.bucket == 'bucket'
    assert url.key == 'path/to/file/'
