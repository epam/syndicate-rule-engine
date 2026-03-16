from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable
import msgspec
import pytest

from helpers.constants import (
    Cloud,
    JobType,
    ReportType,
    RemediationComplexity,
    Severity,
)
from helpers.reports import adjust_resource_type
from helpers.time_helper import utc_datetime
from models.job import Job
from services.metadata import Deprecation, Metadata, RuleMetadata
from services.reports import (
    DeprecationReportGenerator,
    JobMetricsDataSource,
    Report,
    ShardsCollectionDataSource,
    add_diff,
)
from services.resources import AZUREResource, MaestroReportResourceView
from services.sharding import AWSRegionDistributor, ShardPart, ShardsCollection

from ..commons import AWS_ACCOUNT_ID, dicts_equal, mock_date_today


@pytest.fixture
def create_job() -> Callable[[str, str], Job]:
    def factory(_id: str, submitted_at: str) -> Job:
        return Job(id=_id, submitted_at=submitted_at, job_type=JobType.STANDARD)

    return factory


@pytest.fixture(scope='class')
def aws_shards_collection(aws_shards_path: Path) -> ShardsCollection:
    col = ShardsCollection(AWSRegionDistributor(2))
    with open(aws_shards_path / 'meta.json', 'rb') as fp:
        col.meta = msgspec.json.decode(fp.read())
    with open(aws_shards_path / '0.json', 'rb') as fp:
        col.put_parts(msgspec.json.decode(fp.read(), type=list[ShardPart]))
    with open(aws_shards_path / '1.json', 'rb') as fp:
        col.put_parts(msgspec.json.decode(fp.read(), type=list[ShardPart]))
    return col


@pytest.fixture(scope='class')
def metadata(load_metadata) -> Metadata:
    return load_metadata('test_collection_data_source', Metadata)


class TestJobMetricsDataSource:
    def test_subsets(self, create_job):
        j0 = create_job('0', '2024-11-16T12:44:54.000000Z')
        j1 = create_job('1', '2024-11-16T12:44:54.000000Z')
        j2 = create_job('2', '2024-11-16T12:44:58.000000Z')
        j3 = create_job('3', '2024-11-16T12:45:02.000000Z')
        j4 = create_job('4', '2024-11-16T12:45:30.000000Z')
        j5 = create_job('5', '2024-11-16T12:45:54.000000Z')
        j6 = create_job('6', '2024-11-16T12:46:00.000000Z')
        j7 = create_job('7', '2024-11-16T12:46:58.000000Z')
        j8 = create_job('8', '2024-11-16T12:49:00.000000Z')
        j9 = create_job('8', '2024-11-16T12:49:00.000000Z')

        source = JobMetricsDataSource([j0, j1, j2, j3, j4, j5, j6, j7, j8, j9])
        assert len(source) == 10

        assert len(source.subset()) == 10
        assert (
            len(
                source.subset(
                    start=utc_datetime('2024-11-16T12:44:54.000000Z')
                )
            )
            == 10
        )
        assert (
            len(source.subset(end=utc_datetime('2024-11-16T12:44:54.000000Z')))
            == 0
        )

        assert (
            len(
                source.subset(
                    start=utc_datetime('2024-11-16T12:49:00.000000Z')
                )
            )
            == 2
        )
        assert (
            len(source.subset(end=utc_datetime('2024-11-16T12:49:00.000000Z')))
            == 8
        )

        assert (
            len(
                source.subset(
                    start=utc_datetime('2024-11-16T12:49:00.000000Z'),
                    end=utc_datetime('2024-11-16T12:49:00.000000Z'),
                )
            )
            == 0
        )
        assert (
            len(
                source.subset(
                    start=utc_datetime('2024-11-16T12:49:00.000000Z'),
                    end=utc_datetime('2024-11-16T12:49:01.000000Z'),
                )
            )
            == 2
        )

        assert tuple(
            source.subset(
                utc_datetime('2024-11-16T12:44:58.000000Z'),
                utc_datetime('2024-11-16T12:46:00.000000Z'),
            )
        ) == (j2, j3, j4, j5)

        assert (
            len(
                source.subset(
                    start=utc_datetime('2024-11-16T12:44:53.000000Z'),
                    end=utc_datetime('2024-11-16T12:49:01.000000Z'),
                )
            )
            == 10
        )

    def test_getitem_subsets(self, create_job):
        j0 = create_job('0', '2024-11-16T12:44:54.000000Z')
        j1 = create_job('1', '2024-11-16T12:44:54.000000Z')
        j2 = create_job('2', '2024-11-16T12:44:58.000000Z')
        j3 = create_job('3', '2024-11-16T12:45:02.000000Z')

        source = JobMetricsDataSource([j0, j1, j2, j3])
        assert len(source[: utc_datetime('2024-11-16T12:44:58.000000Z')]) == 2
        assert len(source[utc_datetime('2024-11-16T12:45:02.000000Z') :]) == 1
        assert (
            len(
                source[
                    utc_datetime('2024-11-16T12:44:54.000000Z') : utc_datetime(
                        '2024-11-16T12:45:02.000000Z'
                    )
                ]
            )
            == 3
        )
        assert len(source[:]) == 4
        with pytest.raises(NotImplementedError):
            _ = source[
                : utc_datetime('2024-11-16T12:45:02.000000Z') : timedelta(
                    minutes=1
                )
            ]

        assert tuple(source[0:2]) == (j0, j1)

    def test_is_empty(self, create_job):
        j0 = create_job('0', '2024-11-16T12:44:54.000000Z')
        assert not JobMetricsDataSource([])
        assert JobMetricsDataSource([j0])

        assert len(JobMetricsDataSource([])) == 0
        assert len(JobMetricsDataSource([j0])) == 1

        if JobMetricsDataSource([]):
            pytest.fail('Empty JobsMetricsDataSource must not pass if-clause')


def test_adjust_rt():
    assert adjust_resource_type('aws.iam-role') == 'iam-role'
    assert adjust_resource_type('iam-role') == 'iam-role'


class TestShardsCollectionDataSource:
    def test_n_unique(self, aws_shards_collection, metadata):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.n_unique == 26

    def test_region_severities(self, aws_shards_collection, metadata):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.region_severities(unique=True) == {
            'eu-central-1': {'Unknown': 6, 'Info': 2, 'High': 1},
            'global': {'Unknown': 4, 'Info': 1},
            'eu-north-1': {'Medium': 1},
            'eu-west-1': {'Unknown': 6, 'Medium': 1, 'High': 3},
            'eu-west-3': {'Medium': 1},
        }

    def test_region_severities_no_unique(
        self, aws_shards_collection, metadata
    ):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.region_severities(unique=False) == {
            'eu-central-1': {'Unknown': 6, 'Medium': 1, 'Info': 2, 'High': 1},
            'global': {'Unknown': 4, 'Info': 1},
            'eu-north-1': {'Medium': 1},
            'eu-west-1': {'Unknown': 6, 'Medium': 3, 'High': 3},
            'eu-west-3': {'Medium': 1},
        }

    def test_severities(self, aws_shards_collection, metadata):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.severities() == {
            'Unknown': 16,
            'Info': 3,
            'High': 4,
            'Medium': 3,
        }

    def test_region_services(self, aws_shards_collection, metadata):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.region_services() == {
            'eu-central-1': {
                'Security Group': 3,
                'Sns': 3,
                'AWS S3': 1,
                'AWS Security Hub': 1,
                'Amazon Elastic Block Store': 1,
            },
            'global': {'Cloudtrail': 1, 'Iam Group': 3, 'AWS Account': 1},
            'eu-north-1': {'AWS S3': 1},
            'eu-west-1': {'Ebs': 2, 'Ec2': 2, 'Ecr': 2, 'AWS S3': 4},
            'eu-west-3': {'AWS S3': 1},
        }

    def test_services(self, aws_shards_collection, metadata):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            metadata=metadata,
            cloud=Cloud.AWS,
            account_id=AWS_ACCOUNT_ID
        )
        assert source.services() == {
            'Security Group': 3,
            'Sns': 3,
            'AWS S3': 7,
            'AWS Security Hub': 1,
            'Amazon Elastic Block Store': 1,
            'Cloudtrail': 1,
            'Iam Group': 3,
            'AWS Account': 1,
            'Ebs': 2,
            'Ec2': 2,
            'Ecr': 2,
        }


def test_add_diff():
    current = {
        'key': 'value',
        'scans': 10,
        'resources': {'EC2': 5, 'S3': 2, 'SNS': 0, 'ECR': 5},
        'scanned_tenants': 4,
    }
    previous = {
        'key': 'value',
        'scans': 5,
        'resources': {'EC2': 2, 'S3': 10, 'SNS': 0, 'SQS': 0},
        'scanned_tenants': 4,
    }
    add_diff(current, previous, exclude=('scanned_tenants',))
    assert current == {
        'key': 'value',
        'scans': {'value': 10, 'diff': 5},
        'resources': {
            'EC2': {'value': 5, 'diff': 3},
            'S3': {'value': 2, 'diff': -8},
            'SNS': {'value': 0, 'diff': 0},
            'ECR': {'value': 5},
        },
        'scanned_tenants': 4,
    }

    current = {
        'key': 'value',
        'coverage': {'CIS v7': 0.2, 'CIS v8': 0.3, 'HIPAA': 0.1},
    }
    add_diff(current, {'coverage': {'CIS v7': 0.4}})
    assert current == {
        'key': 'value',
        'coverage': {
            'CIS v7': {'value': 0.2, 'diff': -0.2},
            'CIS v8': {'value': 0.3},
            'HIPAA': {'value': 0.1},
        },
    }


class TestDeprecationReportGenerator:
    """Tests for DeprecationReportGenerator and previous_period_findings."""

    @pytest.fixture
    def deprecation_rule_metadata(self):
        """Rule metadata for a rule deprecated in report period (2025-07-17)."""
        return RuleMetadata(
            source='testing',
            category='Deprecation > Runtime version',
            service_section='Compute',
            service='Defender Pricing',
            article='',
            impact='',
            remediation='Remediation text here',
            severity=Severity.UNKNOWN,
            report_fields=(),
            periodic=False,
            standard={},
            mitre={},
            waf={},
            remediation_complexity=RemediationComplexity.UNKNOWN,
            deprecation=Deprecation(
                date=date(2025, 7, 17), link='https://example.com'
            ),
            cloud='AZURE',
            events=None,
        )

    @pytest.fixture
    def deprecation_metadata(self, deprecation_rule_metadata):
        """Metadata with one deprecation rule."""
        return Metadata(
            rules={
                'ecc-azure-096-cis_sec_defender_azure_sql': deprecation_rule_metadata,
            },
        )

    @pytest.fixture
    def deprecation_rule_resources(self):
        """One Azure resource for the deprecation rule."""
        resource = AZUREResource(
            id='/subscriptions/3d615fa8-05c6-47ea-990d-9d162testing/providers/Microsoft.Security/pricings/SqlServers',
            name='SqlServers',
            location='global',
            resource_type='azure.defender-pricing',
            sync_date=1729502334.624495,
            data={},
        )
        return {'ecc-azure-096-cis_sec_defender_azure_sql': {resource}}

    def test_deprecation_report_with_previous_period_findings(
        self,
        deprecation_metadata,
        deprecation_rule_resources,
        load_expected,
    ):
        """Generator adds previous_period_findings when rule is just deprecated."""
        with mock_date_today('datetime.date', date(2025, 7, 18)):
            report = Report.derive_report(ReportType.OPERATIONAL_DEPRECATION)
            generator = DeprecationReportGenerator(
                metadata=deprecation_metadata,
                view=MaestroReportResourceView(),
                scope=None,
            )
            start = datetime(2025, 7, 17)
            end = datetime(2025, 7, 18)
            result = tuple(
                report.accept(
                    generator,
                    rule_resources=deprecation_rule_resources,
                    meta={},
                    start=start,
                    end=end,
                    previous_period_findings={
                        'ecc-azure-096-cis_sec_defender_azure_sql': 3,
                    },
                )
            )
            expected = [
                {
                    "category": "Runtime version",
                    "deprecation_date": "2025-07-17",
                    "is_deprecated": True,
                    "deprecation_severity": "High",
                    "deprecation_link": "https://example.com",
                    "policy": "ecc-azure-096-cis_sec_defender_azure_sql",
                    "resources": {
                    "global": [
                        {
                        "id": "/subscriptions/3d615fa8-05c6-47ea-990d-9d162testing/providers/Microsoft.Security/pricings/SqlServers",
                        "name": "SqlServers",
                        "sre:date": 1729502334.624495
                        }
                    ]
                    },
                    "previous_period_findings": 3
                }
            ]
            assert len(result) == len(expected)
            assert dicts_equal(list(result), expected)

    def test_deprecation_report_without_previous_period_findings(
        self,
        deprecation_metadata,
        deprecation_rule_resources,
        load_expected,
    ):
        """Generator does not add previous_period_findings when not provided."""
        with mock_date_today('datetime.date', date(2025, 7, 18)):
            report = Report.derive_report(ReportType.OPERATIONAL_DEPRECATION)
            generator = DeprecationReportGenerator(
                metadata=deprecation_metadata,
                view=MaestroReportResourceView(),
                scope=None,
            )
            start = datetime(2025, 7, 17)
            end = datetime(2025, 7, 18)
            result = tuple(
                report.accept(
                    generator,
                    rule_resources=deprecation_rule_resources,
                    meta={},
                    start=start,
                    end=end,
                    previous_period_findings=None,
                )
            )
            # Same structure as metrics/azure_operational_deprecations data item,
            # but without previous_period_findings
            expected_item = load_expected(
                'metrics/azure_operational_deprecations'
            )['data'][0]
            assert len(result) == 1
            assert 'previous_period_findings' not in result[0]
            assert result[0]['policy'] == expected_item['policy']
            assert result[0]['category'] == expected_item['category']
            assert result[0]['resources'] == expected_item['resources']

    def test_deprecation_report_deprecation_date_outside_period(
        self,
        deprecation_metadata,
        deprecation_rule_resources,
    ):
        """Generator does not add previous_period_findings when deprecation date is outside report period."""
        with mock_date_today('datetime.date', date(2025, 7, 18)):
            report = Report.derive_report(ReportType.OPERATIONAL_DEPRECATION)
            generator = DeprecationReportGenerator(
                metadata=deprecation_metadata,
                view=MaestroReportResourceView(),
                scope=None,
            )
            # Period where 2025-07-17 is NOT included (e.g. previous week)
            start = datetime(2025, 7, 10)
            end = datetime(2025, 7, 16)
            result = tuple(
                report.accept(
                    generator,
                    rule_resources=deprecation_rule_resources,
                    meta={},
                    start=start,
                    end=end,
                    previous_period_findings={
                        'ecc-azure-096-cis_sec_defender_azure_sql': 3,
                    },
                )
            )
            assert len(result) == 1
            assert 'previous_period_findings' not in result[0]
