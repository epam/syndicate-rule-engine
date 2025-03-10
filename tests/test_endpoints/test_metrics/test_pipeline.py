"""
Metrics pipeline collects data for the current week for all customers and
tenants. These tests check whether the data is processed as we expect
"""

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from dateutil.relativedelta import relativedelta

from executor.services.report_service import JobResult
from helpers.constants import Cloud, JobState, PolicyErrorType, ReportType
from helpers.time_helper import utc_datetime
from lambdas.custodian_metrics_updater.processors.new_metrics_collector import (
    MetricsCollector,
)
from services import SP
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    StatisticsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.sharding import ShardsCollectionFactory, ShardsS3IO

from ...commons import dicts_equal


@pytest.fixture()
def aws_jobs(
    aws_tenant, aws_scan_result, create_tenant_job, create_tenant_br, utcnow
):
    # don't need to keep results of individual jobs, but need their statistics
    start = utcnow.replace(hour=0)

    job = create_tenant_job(
        aws_tenant, start + timedelta(seconds=1), JobState.SUCCEEDED
    )
    create_tenant_job(
        aws_tenant, start + timedelta(seconds=2), JobState.FAILED
    ).save()
    br = create_tenant_br(
        aws_tenant, start + timedelta(seconds=3), JobState.SUCCEEDED
    )

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
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(
            aws_tenant,
            {
                (
                    'eu-west-1',
                    'ecc-aws-427-rds_cluster_without_tag_information',
                ): (PolicyErrorType.ACCESS, 'AccessDenied Exception', []),
                ('global', 'ecc-aws-527-waf_global_webacl_not_empty'): (
                    PolicyErrorType.ACCESS,
                    'AccessDenied Exception',
                    [],
                ),
            },
        ),
    )


@pytest.fixture()
def azure_jobs(
    azure_tenant,
    azure_scan_result,
    create_tenant_job,
    create_tenant_br,
    utcnow,
):
    # don't need to keep results of individual jobs, but need their statistics
    start = utcnow.replace(hour=0)

    job = create_tenant_job(
        azure_tenant, start + timedelta(seconds=1), JobState.SUCCEEDED
    )
    create_tenant_job(
        azure_tenant, start + timedelta(seconds=2), JobState.FAILED
    ).save()
    br = create_tenant_br(
        azure_tenant, start + timedelta(seconds=3), JobState.SUCCEEDED
    )

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
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(
            azure_tenant,
            {
                ('global', 'ecc-azure-125-nsg_mysql'): (
                    PolicyErrorType.ACCESS.value,
                    'AccessDenied exception',
                    [],
                ),
                ('global', 'ecc-azure-313-cis_postgresql_log_min_messages'): (
                    PolicyErrorType.ACCESS.value,
                    'AccessDenied exception',
                    [],
                ),
            },
        ),
    )


@pytest.fixture()
def google_jobs(
    google_tenant,
    google_scan_result,
    create_tenant_job,
    create_tenant_br,
    utcnow,
):
    # don't need to keep results of individual jobs, but need their statistics
    start = utcnow.replace(hour=0)

    job = create_tenant_job(
        google_tenant, start + timedelta(seconds=1), JobState.SUCCEEDED
    )
    create_tenant_job(
        google_tenant, start + timedelta(seconds=2), JobState.FAILED
    ).save()
    br = create_tenant_br(
        google_tenant, start + timedelta(seconds=3), JobState.SUCCEEDED
    )

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
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(
            google_tenant,
            {
                ('global', 'ecc-gcp-027-dnssec_for_cloud_dns'): (
                    PolicyErrorType.ACCESS.value,
                    'AccessDenied exception',
                    [],
                ),
                (
                    'global',
                    'ecc-gcp-101-load_balancer_access_logging_disabled',
                ): (
                    PolicyErrorType.ACCESS.value,
                    'AccessDenied exception',
                    [],
                ),
            },
        ),
    )


@pytest.fixture()
def k8s_platform_jobs(
    aws_tenant, k8s_platform, k8s_scan_result, create_k8s_platform_job, utcnow
):
    start = utcnow.replace(hour=0)

    job = create_k8s_platform_job(
        k8s_platform, start + timedelta(seconds=1), JobState.SUCCEEDED
    )
    create_k8s_platform_job(
        k8s_platform, start + timedelta(seconds=2), JobState.FAILED
    ).save()

    job.save()

    # prepare data for pipeline
    result = JobResult(k8s_scan_result, Cloud.KUBERNETES)
    collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
    collection.put_parts(result.iter_shard_parts())
    collection.meta = result.rules_meta()
    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=PlatformReportsBucketKeysBuilder(k8s_platform).latest_key(),
        client=SP.s3,
    )
    collection.write_all()
    collection.write_meta()
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(aws_tenant, {}),
    )


def test_metrics_update_denied(sre_client):
    resp = sre_client.request('/metrics/update', 'POST')
    assert resp.status_int == 401
    assert resp.json == {'message': 'Unauthorized'}


def test_whole_period():
    now = datetime(
        year=2024,
        month=1,
        day=10,
        hour=12,
        minute=10,
        second=35,
        tzinfo=timezone.utc,
    )

    class RTMock:
        def __init__(self, r_start, r_end):
            self.r_start = r_start
            self.r_end = r_end

    r1 = RTMock(r_start=relativedelta(days=-1), r_end=relativedelta())
    r2 = RTMock(
        r_end=relativedelta(hour=0, minute=0, second=0),
        r_start=relativedelta(months=-1),
    )
    start, end = MetricsCollector.whole_period(now, r1, r2)  # pyright: ignore
    assert start == datetime(2023, 12, 10, 12, 10, 35, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 10, 12, 10, 35, tzinfo=timezone.utc)

    r3 = RTMock(r_start=None, r_end=relativedelta())
    r4 = RTMock(r_start=None, r_end=relativedelta(hour=0, minute=0, second=0))
    start, end = MetricsCollector.whole_period(now, r3, r4)  # pyright: ignore
    assert start is None
    assert end == now


def test_metrics_update_operational_project(
    sre_client,
    system_user_token,
    aws_jobs,
    azure_jobs,
    google_jobs,
    k8s_platform_jobs,
    load_expected,
    aws_tenant,
    azure_tenant,
    google_tenant,
    k8s_platform,
    main_customer,
    set_license_metadata,
):
    set_license_metadata('metrics_metadata')
    # resp = sre_client.request(
    #     '/metrics/update', 'POST', auth=system_user_token
    # )
    # assert resp.status_int == 202
    # assert resp.json == {'message': 'Metrics update has been submitted'}
    # time.sleep(2)  # don't know how to check underlying thread is finished
    MetricsCollector.build().__call__()

    # checking operational (per tenant)
    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_OVERVIEW
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_overview'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        azure_tenant, ReportType.OPERATIONAL_OVERVIEW
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/azure_operational_overview'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        google_tenant, ReportType.OPERATIONAL_OVERVIEW
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/google_operational_overview'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_RESOURCES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_resources'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        azure_tenant, ReportType.OPERATIONAL_RESOURCES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/azure_operational_resources'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        google_tenant, ReportType.OPERATIONAL_RESOURCES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/google_operational_resources'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_RULES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_rules'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        azure_tenant, ReportType.OPERATIONAL_RULES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/azure_operational_rules'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        google_tenant, ReportType.OPERATIONAL_RULES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/google_operational_rules'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_FINOPS
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_finops'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_COMPLIANCE
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_compliance'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        aws_tenant, ReportType.OPERATIONAL_ATTACKS
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_operational_attacks'),
    )

    item = SP.report_metrics_service.get_latest_for_platform(
        k8s_platform, ReportType.OPERATIONAL_KUBERNETES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/k8s_operational'),
    )

    item = SP.report_metrics_service.get_latest_for_tenant(
        azure_tenant, ReportType.OPERATIONAL_DEPRECATION
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/azure_operational_deprecations'),
    )

    item = SP.report_metrics_service.get_latest_for_project(
        main_customer.name,
        aws_tenant.display_name_to_lower,
        ReportType.PROJECT_OVERVIEW,
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_project_overview'),
    )

    item = SP.report_metrics_service.get_latest_for_project(
        main_customer.name,
        aws_tenant.display_name_to_lower,
        ReportType.PROJECT_COMPLIANCE,
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_project_compliance'),
    )

    item = SP.report_metrics_service.get_latest_for_project(
        main_customer.name,
        aws_tenant.display_name_to_lower,
        ReportType.PROJECT_RESOURCES,
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_project_resources'),
    )

    item = SP.report_metrics_service.get_latest_for_project(
        main_customer.name,
        aws_tenant.display_name_to_lower,
        ReportType.PROJECT_ATTACKS,
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_project_attacks'),
    )

    item = SP.report_metrics_service.get_latest_for_project(
        main_customer.name,
        aws_tenant.display_name_to_lower,
        ReportType.PROJECT_FINOPS,
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/aws_project_finops'),
    )


def test_metrics_update_department_c_level(
    sre_client,
    system_user_token,
    aws_jobs,
    azure_jobs,
    google_jobs,
    load_expected,
    aws_tenant,
    azure_tenant,
    google_tenant,
    main_customer,
    utcnow,
    set_license_metadata,
):
    set_license_metadata('metrics_metadata')
    future_date = utcnow + relativedelta(months=+1)

    def mocked(x=None):
        if not x:
            return future_date
        return utc_datetime(x)

    with (
        patch(
            'lambdas.custodian_metrics_updater.processors.new_metrics_collector.utc_datetime',
            mocked,
        ),
        patch('services.reports.utc_datetime', mocked),
    ):
        MetricsCollector.build().__call__()
        # resp = sre_client.request(
        #     '/metrics/update', 'POST', auth=system_user_token
        # )
        # assert resp.status_int == 202
        # assert resp.json == {'message': 'Metrics update has been submitted'}
        # time.sleep(2)  # don't know how to check underlying thread is finished
    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.C_LEVEL_OVERVIEW
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/c_level_overview'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.C_LEVEL_COMPLIANCE
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/c_level_compliance')
    )
    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.C_LEVEL_ATTACKS
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/c_level_attacks')
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_resources_by_cloud'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_tenants_resources'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_compliance_by_cloud'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_tenants_compliance'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_attacks_by_cloud'),
    )

    item = SP.report_metrics_service.get_exactly_for_customer(
        main_customer, ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS
    )
    assert dicts_equal(
        SP.report_metrics_service.fetch_data(item),
        load_expected('metrics/department_top_tenants_attacks'),
    )
