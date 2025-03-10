from datetime import datetime, timezone

import pytest

from helpers.constants import Cloud, ReportType
from models.batch_results import BatchResults
from models.job import Job
from models.metrics import ReportMetrics
from services.clients.s3 import S3Url
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    ReportMetricsBucketKeysBuilder,
    ReportsBucketKeysBuilder,
    StatisticsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)


@pytest.fixture
def standard_job(aws_tenant) -> Job:
    return Job(
        id='job_id',
        tenant_name=aws_tenant.name,
        customer_name=aws_tenant.customer_name,
        submitted_at='2023-11-27T14:29:08.694447Z',
        status='SUCCEEDED',
    )


@pytest.fixture
def platform_job(k8s_platform) -> Job:
    return Job(
        id='job_id',
        tenant_name=k8s_platform.tenant_name,
        customer_name=k8s_platform.customer,
        submitted_at='2023-11-27T14:29:08.694447Z',
        status='SUCCEEDED',
        platform_id=k8s_platform.id,
    )


@pytest.fixture
def ed_job(aws_tenant) -> BatchResults:
    return BatchResults(
        id='job_id',
        tenant_name=aws_tenant.name,
        customer_name=aws_tenant.customer_name,
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
        assert (
            res
            == 'raw/TEST_CUSTOMER/AWS/123456789012/jobs/standard/2023-11-27-14/job_id/result/'
        )

    def test_ed_job_result(self, tenant_reports_builder, ed_job):
        res = tenant_reports_builder.ed_job_result(ed_job)
        assert (
            res
            == 'raw/TEST_CUSTOMER/AWS/123456789012/jobs/event-driven/2023-11-27-14/job_id/result/'
        )

    def test_ed_job_difference(self, tenant_reports_builder, ed_job):
        res = tenant_reports_builder.ed_job_difference(ed_job)
        assert (
            res
            == 'raw/TEST_CUSTOMER/AWS/123456789012/jobs/event-driven/2023-11-27-14/job_id/difference/'
        )

    def test_latest_key(self, tenant_reports_builder):
        res = tenant_reports_builder.latest_key()
        assert res == 'raw/TEST_CUSTOMER/AWS/123456789012/latest/'

    def test_snapshots_folder(self, tenant_reports_builder):
        res = tenant_reports_builder.snapshots_folder()
        assert res == 'raw/TEST_CUSTOMER/AWS/123456789012/snapshots/'

    def test_snapshot_key(self, tenant_reports_builder):
        now = datetime.now(tz=timezone.utc)
        res = tenant_reports_builder.snapshot_key(now)
        assert (
            res
            == f'raw/TEST_CUSTOMER/AWS/123456789012/snapshots/{now.strftime("%Y-%m-%d-%H")}/'
        )

    def test_one_time_on_demand(self):
        res = ReportsBucketKeysBuilder.one_time_on_demand()
        assert res.startswith('on-demand/')


class TestPlatformReportsBucketKeyBuilder:
    def test_cloud(self, platform_reports_builder):
        assert platform_reports_builder.cloud == Cloud.KUBERNETES

    def test_job_result(self, platform_reports_builder, platform_job):
        res = platform_reports_builder.job_result(platform_job)
        assert (
            res
            == 'raw/TEST_CUSTOMER/KUBERNETES/test-eu-west-1/jobs/standard/2023-11-27-14/job_id/'
        )

    def test_ed_job(self, platform_reports_builder, ed_job):
        with pytest.raises(NotImplementedError):
            platform_reports_builder.ed_job_result(ed_job)
        with pytest.raises(NotImplementedError):
            platform_reports_builder.ed_job_difference(ed_job)

    def test_latest_key(self, platform_reports_builder):
        res = platform_reports_builder.latest_key()
        assert res == 'raw/TEST_CUSTOMER/KUBERNETES/test-eu-west-1/latest/'

    def test_snapshots_folder(self, platform_reports_builder):
        res = platform_reports_builder.snapshots_folder()
        assert res == 'raw/TEST_CUSTOMER/KUBERNETES/test-eu-west-1/snapshots/'


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
            now=now, customer='TEST_CUSTOMER'
        )
        assert (
            res
            == f'report-statistics/diagnostic/TEST_CUSTOMER/{now.strftime("%Y-%m")}/diagnostic_report.json'
        )

    def test_xray_log(self):
        now = datetime.now(timezone.utc)
        res = StatisticsBucketKeysBuilder.xray_log('job_id')
        assert (
            res == f'xray/executor/{now.year}/{now.month}/{now.day}/job_id.log'
        )


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


class TestReportMetricsBucketKeysBuilder:
    def test_metrics_key_tenant(self, aws_tenant):
        dt = '2025-02-01T00:00:00Z'
        item = ReportMetrics(
            key=ReportMetrics.build_key_for_tenant(
                ReportType.OPERATIONAL_OVERVIEW, aws_tenant
            ),
            end=dt,
        )
        assert (
            ReportMetricsBucketKeysBuilder.metrics_key(item)
            == 'metrics/TEST_CUSTOMER/OPERATIONAL_OVERVIEW/AWS/AWS-TESTING/2025-02-01-00-00-00-000000/data.json.gz'
        )

    def test_metrics_key_platform(self, k8s_platform):
        dt = '2025-02-01T00:00:00Z'
        item = ReportMetrics(
            key=ReportMetrics.build_key_for_platform(
                ReportType.OPERATIONAL_KUBERNETES, k8s_platform
            ),
            end=dt,
        )
        assert (
            ReportMetricsBucketKeysBuilder.metrics_key(item)
            == 'metrics/TEST_CUSTOMER/OPERATIONAL_KUBERNETES/KUBERNETES/platform_id/2025-02-01-00-00-00-000000/data.json.gz'
        )

    def test_metrics_key_project(self):
        dt = '2025-02-01T00:00:00Z'
        item = ReportMetrics(
            key=ReportMetrics.build_key_for_project(
                ReportType.PROJECT_OVERVIEW, 'TEST_CUSTOMER', 'testing'
            ),
            end=dt,
        )
        assert (
            ReportMetricsBucketKeysBuilder.metrics_key(item)
            == 'metrics/TEST_CUSTOMER/PROJECT_OVERVIEW/testing/2025-02-01-00-00-00-000000/data.json.gz'
        )

    def test_metrics_key_customer(self):
        dt = '2025-02-01T00:00:00Z'
        item = ReportMetrics(
            key=ReportMetrics.build_key_for_customer(
                ReportType.C_LEVEL_ATTACKS, 'TEST_CUSTOMER'
            ),
            end=dt,
        )
        assert (
            ReportMetricsBucketKeysBuilder.metrics_key(item)
            == 'metrics/TEST_CUSTOMER/C_LEVEL_ATTACKS/2025-02-01-00-00-00-000000/data.json.gz'
        )
