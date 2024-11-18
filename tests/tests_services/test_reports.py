from datetime import timedelta
from pathlib import Path

import msgspec
import pytest

from helpers.time_helper import utc_datetime
from models.job import Job
from services.ambiguous_job_service import AmbiguousJob
from services.mappings_collector import MappingsCollector
from services.reports import JobMetricsDataSource, ShardsCollectionDataSource
from services.sharding import ShardsCollection, AWSRegionDistributor, ShardPart


@pytest.fixture
def create_job():
    def factory(_id: str, submitted_at: str) -> AmbiguousJob:
        return AmbiguousJob(Job(
            id=_id,
            submitted_at=submitted_at
        ))

    return factory


@pytest.fixture
def empty_mappings_collector():
    return MappingsCollector()


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
        assert len(source.subset(
            start=utc_datetime('2024-11-16T12:44:54.000000Z'))) == 10
        assert len(source.subset(
            end=utc_datetime('2024-11-16T12:44:54.000000Z'))) == 0

        assert len(source.subset(
            start=utc_datetime('2024-11-16T12:49:00.000000Z'))) == 2
        assert len(source.subset(
            end=utc_datetime('2024-11-16T12:49:00.000000Z'))) == 8

        assert len(
            source.subset(start=utc_datetime('2024-11-16T12:49:00.000000Z'),
                          end=utc_datetime(
                              '2024-11-16T12:49:00.000000Z'))) == 0
        assert len(
            source.subset(start=utc_datetime('2024-11-16T12:49:00.000000Z'),
                          end=utc_datetime(
                              '2024-11-16T12:49:01.000000Z'))) == 2

        assert tuple(source.subset(utc_datetime('2024-11-16T12:44:58.000000Z'),
                                   utc_datetime(
                                       '2024-11-16T12:46:00.000000Z'))) == (
                   j2, j3, j4, j5
               )

        assert len(
            source.subset(start=utc_datetime('2024-11-16T12:44:53.000000Z'),
                          end=utc_datetime(
                              '2024-11-16T12:49:01.000000Z'))) == 10

    def test_getitem_subsets(self, create_job):
        j0 = create_job('0', '2024-11-16T12:44:54.000000Z')
        j1 = create_job('1', '2024-11-16T12:44:54.000000Z')
        j2 = create_job('2', '2024-11-16T12:44:58.000000Z')
        j3 = create_job('3', '2024-11-16T12:45:02.000000Z')

        source = JobMetricsDataSource([j0, j1, j2, j3])
        assert len(source[:utc_datetime('2024-11-16T12:44:58.000000Z')]) == 2
        assert len(source[utc_datetime('2024-11-16T12:45:02.000000Z'):]) == 1
        assert len(source[
                   utc_datetime('2024-11-16T12:44:54.000000Z'):utc_datetime(
                       '2024-11-16T12:45:02.000000Z')]) == 3
        assert len(source[:]) == 4
        with pytest.raises(NotImplementedError):
            _ = source[:utc_datetime('2024-11-16T12:45:02.000000Z'):timedelta(
                minutes=1)]

        assert tuple(source[0:2]) == (j0, j1)

    def test_is_empty(self, create_job):
        j0 = create_job('0', '2024-11-16T12:44:54.000000Z')
        assert not JobMetricsDataSource([])
        assert JobMetricsDataSource([j0])

        assert len(JobMetricsDataSource([])) == 0
        assert len(JobMetricsDataSource([j0])) == 1

        if JobMetricsDataSource([]):
            pytest.fail('Empty JobsMetricsDataSource must not pass if-clause')


class TestShardsCollectionDataSource:
    def test_n_unique(self, aws_shards_collection, empty_mappings_collector):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            mappings_collector=empty_mappings_collector
        )
        assert source.n_unique == 23

    def test_adjust_rt(self):
        assert ShardsCollectionDataSource.adjust_resource_type(
            'aws.iam-role') == 'iam-role'
        assert ShardsCollectionDataSource.adjust_resource_type(
            'iam-role') == 'iam-role'

    def test_region_severities_no_metadata(self, aws_shards_collection,
                                           empty_mappings_collector):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            mappings_collector=empty_mappings_collector
        )
        assert source.region_severities() == {
            'eu-central-1': {'Unknown': 7},
            'global': {'Unknown': 4},
            'eu-north-1': {'Unknown': 1},
            'eu-west-1': {'Unknown': 10},
            'eu-west-3': {'Unknown': 1}
        }
        assert source.severities() == {
            'Unknown': 23
        }

    def test_report_types_no_metadata(self, aws_shards_collection,
                                      empty_mappings_collector):
        source = ShardsCollectionDataSource(
            collection=aws_shards_collection,
            mappings_collector=empty_mappings_collector
        )
        assert source.resource_types() == {
            'Security Group': 3,
            'Cloudtrail': 1,
            'Sns': 3,
            'S3': 10,
            'Iam Group': 3,
            'Ebs': 2,
            'Ec2': 2,
            'Ecr': 2
        }
