import io
import operator
from unittest.mock import create_autospec, MagicMock

import pytest

from helpers.constants import PolicyErrorType
from services.clients.s3 import S3Client
from services.sharding import (SingleShardDistributor, ShardPart,
                               AWSRegionDistributor, Shard, ShardsIterator,
                               ShardsS3IO, ShardsCollection)


@pytest.fixture
def make_shard_part():
    def _make_shard_part(location='global',
                         policy='example-policy', resources=None,
                         error=None, previous_timestamp: float | None = None):
        return ShardPart(
            policy=policy,
            location=location,
            resources=resources or [],
            error=error,
            previous_timestamp=previous_timestamp
        )

    return _make_shard_part


@pytest.fixture
def make_shard(make_shard_part):
    def _make_shard() -> Shard:
        shard = Shard()
        part1 = make_shard_part('global', 'policy1', [{'k1': 'v1'}])
        part2 = make_shard_part('global', 'policy1', [{'k2': 'v2'}])
        part3 = make_shard_part('global', 'policy2', [{'k3': 'v3'}])
        shard.put(part1)
        shard.put(part2)
        shard.put(part3)
        return shard

    return _make_shard


class TestShardDistributors:
    def test_single(self, make_shard_part):
        item = SingleShardDistributor()
        assert item.distribute_part(make_shard_part('global')) == 0
        assert item.distribute_part(make_shard_part('eu-central-1')) == 0
        assert item.distribute_part(make_shard_part('eu-west-1')) == 0

    def test_aws_region_1(self, make_shard_part):
        item = AWSRegionDistributor(1)
        assert item.distribute_part(make_shard_part('global')) == 0
        assert item.distribute_part(make_shard_part('eu-central-1')) == 0
        assert item.distribute_part(make_shard_part('eu-west-1')) == 0

    def test_aws_region_2(self, make_shard_part):
        item = AWSRegionDistributor(2)
        assert item.distribute_part(make_shard_part('global')) == 0
        assert item.distribute_part(make_shard_part('eu-central-1')) == 0
        assert item.distribute_part(make_shard_part('eu-west-1')) == 1

    def test_aws_region_10(self, make_shard_part):
        item = AWSRegionDistributor(10)
        assert item.distribute_part(make_shard_part('global')) == 0
        assert item.distribute_part(make_shard_part('eu-central-1')) == 2
        assert item.distribute_part(make_shard_part('eu-west-2')) == 4
        assert item.distribute_part(make_shard_part('eu-west-1')) == 3

    def test_aws_region_31(self, make_shard_part):
        item = AWSRegionDistributor(31)
        assert item.distribute_part(make_shard_part('global')) == 0
        assert item.distribute_part(make_shard_part('us-east-1')) == 1
        assert item.distribute_part(make_shard_part('eu-central-1')) == 12
        assert item.distribute_part(make_shard_part('eu-west-2')) == 14
        assert item.distribute_part(make_shard_part('eu-west-1')) == 13


class TestShardPart:
    def test_ts(self, make_shard_part):
        part1 = make_shard_part()
        part2 = make_shard_part()
        assert part1.timestamp <= part2.timestamp

    def test_error(self, make_shard_part):
        part = make_shard_part('global', 'policy', [], 'ACCESS:User: admin is not authorized')
        assert part.error_type is PolicyErrorType.ACCESS
        assert part.error_message == 'User: admin is not authorized'


class TestShard:
    def test_put_priority(self, make_shard_part):
        part1 = make_shard_part('global', 'policy', [{'k1': 'v1'}])
        part2 = make_shard_part('global', 'policy', [{'k2': 'v2'}])
        shard = Shard()
        shard.put(part1)
        shard.put(part2)
        assert len(shard) == 1
        assert next(iter(shard)) is part2

    def test_update_priority(self, make_shard_part):
        part1 = make_shard_part('global', 'policy1', [{'k1': 'v1'}])
        part2 = make_shard_part('global', 'policy1', [{'k2': 'v2'}])
        part3 = make_shard_part('global', 'policy2', [{'k3': 'v3'}])
        shard1 = Shard()
        shard2 = Shard()
        shard1.put(part1)
        shard2.put(part2)
        shard2.put(part3)
        shard1.update(shard2)
        assert len(shard1) == 2
        assert tuple(shard1) == (part2, part3)


class TestShardIterator:
    def test_it(self, make_shard):
        shard1 = make_shard()
        shard2 = make_shard()
        shard3 = make_shard()
        it = ShardsIterator({0: shard1, 1: shard2, 2: shard3}, 3)
        assert list(it) == [(0, shard1), (1, shard2), (2, shard3)]

        it = ShardsIterator({0: shard1, 1: shard2, 2: shard3}, 1)
        assert list(it) == [(0, shard1)]

        it = ShardsIterator({1: shard2, 2: shard3}, 1)
        assert list(it) == []

        it = ShardsIterator({1: shard2, 2: shard3}, 2)
        assert list(it) == [(1, shard2)]


class TestShardsS3IO:
    @staticmethod
    def create_writer() -> tuple[ShardsS3IO, MagicMock]:
        client = create_autospec(S3Client)
        writer = ShardsS3IO(
            bucket='reports',
            key='one/two/three',
            client=client
        )
        return writer, client

    def test_write(self, make_shard):
        shard = make_shard()
        writer, client = self.create_writer()
        writer.write(1, shard)
        client.gz_put_object.assert_called()

    def test_write_meta(self):
        writer, client = self.create_writer()
        writer.write_meta({})
        client.gz_put_json.assert_called_with(
            bucket='reports',
            key='one/two/three/meta.json',
            obj={}
        )

    def test_read_raw(self):
        writer, client = self.create_writer()
        client.gz_get_object.return_value = io.BytesIO(
            b'[{"p":"policy","l":"global","t":1711309249.0,"r":[]}]'
        )
        res = writer.read_raw(1)[0]
        assert res.policy == 'policy'
        assert res.location == 'global'
        assert res.timestamp == 1711309249.0
        assert res.resources == []
        assert res.error is None
        assert res.previous_timestamp is None

        client.gz_get_object.assert_called()

    def test_read_meta(self):
        writer, client = self.create_writer()
        client.gz_get_json.return_value = {'policy': {'description': 'data'}}
        assert writer.read_meta() == {'policy': {'description': 'data'}}
        client.gz_get_json.assert_called_with(
            bucket='reports',
            key='one/two/three/meta.json'
        )


class TestShardCollection:
    @staticmethod
    def create_collection() -> ShardsCollection:
        return ShardsCollection(
            distributor=AWSRegionDistributor(2)
        )

    def test_put_part(self, make_shard_part):
        collection = self.create_collection()
        part1 = make_shard_part(location='global')
        part2 = make_shard_part(location='eu-central-1')
        part3 = make_shard_part(location='eu-west1')
        collection.put_parts((part1, part2, part3))
        assert len(collection) == 2
        assert (part1, part2, part3) == tuple(collection.iter_parts())

        assert all(isinstance(i[1], Shard) for i in collection)
        assert list(i[0] for i in collection) == [0, 1]

    def test_update(self, make_shard_part):
        """
        Tests that newer findings are kept in case two similar are found
        :return:
        """
        part1 = make_shard_part(
            location='global',
            policy='policy1',
            resources=[{'k1': 'v1'}, {'k2': 'v2'}]
        )
        part2 = make_shard_part(
            location='global',
            policy='policy1',
            resources=[{'k1': 'v1'}]
        )
        part3 = make_shard_part(
            location='eu-west-1',
            policy='policy2',
            resources=[{'k3': 'v3'}]
        )
        c1 = self.create_collection()
        c2 = self.create_collection()

        c1.put_part(part1)
        c1.put_part(part3)
        c2.put_part(part2)

        c1.update(c2)
        assert (part2, part3) == tuple(c1.iter_parts())

    def test_subtract(self, make_shard_part):
        part1 = make_shard_part(
            location='global',
            policy='policy1',
            resources=[{'k1': 'v1'}, {'k4': 'v4'}]
        )
        part2 = make_shard_part(
            location='global',
            policy='policy1',
            resources=[{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v3'}]
        )
        part3 = make_shard_part(
            location='eu-west-1',
            policy='policy2',
            resources=[{'k3': 'v3'}]
        )
        c1 = self.create_collection()
        c2 = self.create_collection()

        c1.put_part(part1)
        c2.put_part(part2)
        c2.put_part(part3)

        diff = c2 - c1
        assert isinstance(diff, ShardsCollection)
        assert isinstance(diff.distributor, SingleShardDistributor)
        parts = sorted(diff.iter_parts(), key=operator.attrgetter('policy'))
        assert len(parts) == 2
        p1, p2 = parts
        assert (len(p1.resources) == 2 and {'k2': 'v2'} in p1.resources
                and {'k3': 'v3'} in p1.resources)
        assert p2.resources == [{'k3': 'v3'}]
