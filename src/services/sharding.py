import io
import tempfile
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import PurePosixPath
from typing import (
    TYPE_CHECKING,
    Generator,
    Iterable,
    Iterator,
    TypedDict,
    cast,
)

import msgspec

from helpers import hashable
from helpers.constants import GLOBAL_REGION, Cloud, PolicyErrorType
from services.clients.s3 import S3Client

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant

# do not change the order, just append new regions. This collection is only
# for shards distributor
AWS_REGIONS = (
    'us-east-1',
    'us-east-2',
    'us-west-1',
    'us-west-2',
    'ap-south-1',
    'ap-northeast-1',
    'ap-northeast-2',
    'ap-northeast-3',
    'ap-southeast-1',
    'ap-southeast-2',
    'ca-central-1',
    'eu-central-1',
    'eu-west-1',
    'eu-west-2',
    'eu-west-3',
    'eu-north-1',
    'sa-east-1',
    'ap-southeast-3',
    'ap-southeast-4',
    'af-south-1',
    'ap-east-1',
    'ap-south-2',
    'eu-south-1',
    'eu-south-2',
    'eu-central-2',
    'il-central-1',
    'me-south-1',
    'me-central-1',
    'us-gov-east-1',
    'us-gov-west-1',
)


class RuleMeta(TypedDict):
    description: str
    resource: str
    comment: str


class ShardPart(
    msgspec.Struct, frozen=True, eq=False, kw_only=True, omit_defaults=True
):
    """
    "policy" & "location" & "timestamp" always exist. They indicate that the
    "policy" was executed against the "location" last at the "timestamp".

    If error is None it means that the latest execution was successful,
    it happened at the "timestamp" and "resources" were found (no
    vulns found if "resources" is empty").

    If error is not None, it means that latest execution failed, it happened
    at the "timestamp". In this case if "resources_timestamp" exists, it means
    that this policy used to find resources and that is the last time in
    executed successfully. If "resources_timestamp" is None, you can ignore
    "resources" field

    """

    policy: str = msgspec.field(name='p')
    location: str = msgspec.field(name='l', default=GLOBAL_REGION)
    timestamp: float = msgspec.field(default_factory=time.time, name='t')
    resources: list[dict] = msgspec.field(default_factory=list, name='r')

    error: str | None = msgspec.field(default=None, name='e')
    # resources timestamp should be always be None if error is None
    previous_timestamp: float | None = msgspec.field(default=None, name='T')

    def has_error(self) -> bool:
        return self.error is not None

    def last_successful_timestamp(self) -> float | None:
        """
        If this method returns None it means that this policy never
        executed successfully and "resources" may be ignored. If it returns
        a timestamp - that will be time of its latest successful execution
        and "resources" can be considered valid as of that date
        """
        if self.error is None:
            return self.timestamp
        # error exists
        return self.previous_timestamp

    @property
    def error_type(self) -> PolicyErrorType | None:
        if not self.error:
            return
        return PolicyErrorType(self.error.split(':', maxsplit=1)[0])

    @property
    def error_message(self) -> None:
        if not self.error:
            return
        return self.error.split(':', maxsplit=1)[1]

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self.policy}:{self.location}>'


class Shard(Iterable[ShardPart]):
    """
    Shard store shard parts. This shard implementation uses policy and
    location as a key to a shard part. This means that we can update
    resources within policy & region. Maybe we will need some other
    implementations in the future
    """

    __slots__ = ('_data',)

    def __init__(self, data: dict | None = None):
        self._data: dict[tuple[str, str], ShardPart] = data or dict()

    def __iter__(self) -> Iterator[ShardPart]:
        return self._data.values().__iter__()

    def __len__(self) -> int:
        return self._data.__len__()

    def __bool__(self) -> bool:
        return bool(self._data)

    @property
    def raw(self) -> dict[tuple[str, str], ShardPart]:
        return self._data

    def put(self, part: ShardPart) -> None:
        """
        Adds a part to the shard in case such part does not exist yet.
        Each part contains its timestamp. In case we try to put a part and
        the same one already exists - the one with higher timestamp will
        be kept
        :param part:
        :return:
        """
        key = (part.policy, part.location)
        if key not in self._data:
            self._data[key] = part
            return
        existing = self._data[key]
        if existing.timestamp > part.timestamp:
            # existing part is newer so ignoring that one
            return
        # here existing is older, so need to replace with a new one properly
        if part.error is not None:
            # new one with error
            if existing.error is None:
                ts = existing.timestamp
            else:
                ts = existing.previous_timestamp
            part = ShardPart(
                policy=part.policy,
                location=part.location,
                timestamp=part.timestamp,
                resources=existing.resources,
                error=part.error,
                previous_timestamp=ts,
            )
        self._data[key] = part

    def pop(self, policy: str, location: str) -> ShardPart | None:
        """
        Removes part from this shard
        """
        return self._data.pop((policy, location), None)

    def get(self, policy: str, location: str) -> ShardPart | None:
        return self._data.get((policy, location), None)

    def update(self, shard: 'Shard') -> None:
        """
        Updates shard parts in the current shard with parts from existing one.
        :param shard:
        :return:
        """
        for part in shard:
            self.put(part)


class ShardDataDistributor(ABC):
    """
    Defines logic how we must distribute parts between shards. We always have
    N shards and some trait based on which we must assign a piece of data to
    a shard.
    This class knows how and based on what attributes to distribute
    """

    def __init__(self, n: int = 1):
        self._n = n

    @property
    def shards_number(self) -> int:
        return self._n

    @abstractmethod
    def distribute(self, **kwargs) -> int:
        """
        Pure idempotent function without side effects. Must return
        shard number in range [0; n-1]
        Must accept some attributes and values based on which the distribution
        happens. Concrete incoming values depend on the implementation
        :param kwargs:
        :return:
        """

    @abstractmethod
    def key(self, part: ShardPart) -> dict:
        """
        Must retrieve data from the given part based on which the
        distribution happens
        :param part:
        :return:
        """

    def distribute_part(self, part: ShardPart) -> int:
        """
        Must return shard for a concrete shard part
        :param part:
        :return:
        """
        return self.distribute(**self.key(part))


class SingleShardDistributor(ShardDataDistributor):
    """
    Is used to distribute azure and gcp findings. Since regions on
    gcp projects and azure subscriptions are scanned all at once -> it
    probably won't be efficient to distribute those by regions, because we
    will spend more money on S3 requests than could save on traffic
    """

    def key(self, part: ShardPart) -> dict:
        return {}

    def distribute(self, **kwargs) -> int:
        return 0


class AWSRegionDistributor(ShardDataDistributor):
    """
    The approach of distributing AWS resources to different shards by
    regions can be efficient because mostly users scan only some of their
    regions. Since findings is a collection which we keep up-to-date and
    consistent must update it each time. So in order not to download the
    whole data from S3 each time - we can make this distribution.
    """

    regions = {r: i for i, r in enumerate([GLOBAL_REGION, *AWS_REGIONS])}

    def key(self, part: ShardPart) -> dict:
        return dict(region=part.location)

    def distribute(self, region: str) -> int:
        """
        Distributes regions by shards
        :param region:
        :return:
        """
        index = self.regions.get(region)
        if index is None:
            index = len(self.regions)
        return index % self._n


class ShardsIO(ABC):
    """
    Defines an interface for shards writer
    """

    __slots__ = ()

    @abstractmethod
    def write(self, n: int, shard: Shard):
        """
        Must write one shard
        :param n:
        :param shard:
        :return:
        """

    def write_many(self, pairs: Iterable[tuple[int, Shard]]):
        for n, shard in pairs:
            self.write(n, shard)

    def read_raw_many(
        self, numbers: Iterable[int]
    ) -> Iterator[list[ShardPart]]:
        return filter(lambda x: x is not None, map(self.read_raw, numbers))

    @abstractmethod
    def read_raw(self, n: int) -> list[ShardPart] | None:
        """
        Reads a specific shard but returns raw json
        :param n:
        :return:
        """

    @abstractmethod
    def write_meta(self, meta: dict): ...

    @abstractmethod
    def read_meta(self) -> dict: ...


class ShardsS3IO(ShardsIO):
    """
    Writer V1
    """

    __slots__ = '_bucket', '_root', '_client'
    _encoder = msgspec.json.Encoder()

    def __init__(self, bucket: str, key: str, client: S3Client):
        """
        :param bucket:
        :param key: root folder where to put shards
        """
        self._bucket = bucket
        self._root = key
        self._client = client

    @property
    def key(self) -> str:
        return self._root

    @key.setter
    def key(self, value: str):
        self._root = value

    def _key(self, n: int) -> str:
        return str((PurePosixPath(self._root) / str(n)).with_suffix('.json'))

    def write(self, n: int, shard: Shard):
        self._client.gz_put_object(
            bucket=self._bucket,
            key=self._key(n),
            body=self._encoder.encode(tuple(shard)),
            gz_buffer=tempfile.TemporaryFile(),
        )

    def read_raw(self, n: int) -> list[ShardPart] | None:
        obj = self._client.gz_get_object(
            bucket=self._bucket,
            key=self._key(n),
            gz_buffer=tempfile.TemporaryFile(),
        )
        if not obj:
            return
        return msgspec.json.decode(
            cast(io.BytesIO, obj).getvalue(), type=list[ShardPart]
        )

    def write_meta(self, meta: dict):
        self._client.gz_put_json(
            bucket=self._bucket,
            key=str((PurePosixPath(self._root) / 'meta.json')),
            obj=meta,
        )

    def read_meta(self) -> dict:
        return (
            self._client.gz_get_json(
                bucket=self._bucket,
                key=str((PurePosixPath(self._root) / 'meta.json')),
            )
            or {}
        )


class ShardsIterator(Iterator[tuple[int, Shard]]):
    def __init__(self, shards: dict, n: int):
        self.shards = shards
        self.n = n  # max number of shards
        self._reset()

    def __iter__(self):
        self._reset()
        return self

    def _reset(self):
        self.indexes = iter(range(self.n))  # [0; n)

    def __next__(self) -> tuple[int, Shard]:
        item, index = None, None
        while not item:
            index = next(self.indexes)
            item = self.shards.get(index)
        return index, item


class ShardsCollection(Iterable[tuple[int, Shard]]):
    """
    Light abstraction over shards, shards writer and distributor
    """

    __slots__ = '_distributor', 'io', 'shards', 'meta'

    def __init__(
        self, distributor: ShardDataDistributor, io: ShardsIO | None = None
    ):
        self._distributor = distributor
        self.io = io

        self.shards: defaultdict[int, Shard] = defaultdict(Shard)
        self.meta: dict[str, RuleMeta] = {}

    def __iter__(self) -> Iterator[tuple[int, Shard]]:
        """
        Iterates over inner shards and their number in order
        :return:
        """
        return ShardsIterator(
            shards=self.shards, n=self._distributor.shards_number
        )

    def __len__(self) -> int:
        return self.shards.__len__()

    def __bool__(self) -> bool:
        return any([bool(shard) for shard in self.shards.values()])

    def iter_parts(self) -> Generator[ShardPart, None, None]:
        """
        Yields only parts that executed successfully at least once.
        """
        for _, shard in self:
            for part in shard:
                # if part.last_successful_timestamp()
                if part.error is None or part.previous_timestamp:
                    yield part

    def iter_all_parts(self) -> Generator[ShardPart, None, None]:
        """
        Yields all parts even those without resources. So, you should handle
        """
        for _, shard in self:
            yield from shard

    def iter_error_parts(self) -> Generator[ShardPart, None, None]:
        for _, shard in self:
            for part in shard:
                if part.error is not None:
                    yield part

    def update(self, other: 'ShardsCollection'):
        """
        Puts shard parts from the given collection to this one,
        redistributing them
        """
        for _, shard in other:
            self.put_parts(shard)

    def __sub__(self, other: 'ShardsCollection') -> 'ShardsCollection':
        """
        Returns a difference between two collections. Uses
        SingleShardDistributor for the new collection
        """
        new = ShardsCollectionFactory.difference()
        for part in self.iter_parts():
            existing = None
            for _, shard in other:
                p = shard.get(part.policy, part.location)
                if p:
                    existing = p
                    break
            if not existing:  # keeping the current one without changes
                new.put_part(part)
                continue
            # other similar part exist, need to get difference
            if part.error is not None:
                # current part with error, just keep it
                new.put_part(part)
                continue
            # current part without error, getting difference
            if not existing.last_successful_timestamp():
                # existing newer executed successfully
                new.put_part(part)
                continue
            # current without error and existing has some resources
            # here need to get difference between two resources lists
            # TODO: maybe add resource type to shard parts,
            #  find difference based on ids instead of hashable

            _new = {hashable(item) for item in part.resources}
            _old = {hashable(item) for item in existing.resources}

            new.put_part(
                ShardPart(
                    policy=part.policy,
                    location=part.location,
                    timestamp=part.timestamp,
                    resources=list(_new - _old),
                    error=None,
                    previous_timestamp=None,
                )
            )
        return new

    @property
    def distributor(self) -> ShardDataDistributor:
        return self._distributor

    def put_part(self, part: ShardPart) -> None:
        """
        Distribute the given shard part to its shard
        :param part:
        :return:
        """
        n = self._distributor.distribute_part(part)
        self.shards[n].put(part)

    def drop_part(self, part: ShardPart | str, location: str | None = None, /):
        """
        Removes a part from this collection
        """
        if isinstance(part, str):
            if not location:
                raise ValueError('provide location as second parameter')
            # just for distributor
            part = ShardPart(policy=part, location=location)
        n = self._distributor.distribute_part(part)
        self.shards[n].pop(part.policy, part.location)

    def put_parts(self, parts: Iterable[ShardPart]) -> None:
        """
        Puts multiple parts. It distributes the given parts to their right
        shards according to the current distributor.
        :param parts: Can be another shard since a shard can be iterated
        over its parts
        :return:
        """
        for part in parts:
            self.put_part(part)

    def write_all(self):
        """
        Writes all the shards that are currently in memory
        :return:
        """
        self.io.write_many(iter(self))

    def fetch_by_indexes(self, it: Iterable[int]):
        """
        Fetches shards by specified indexes
        """
        for parts in self.io.read_raw_many(set(it)):
            self.put_parts(parts)

    def fetch_all(self):
        """
        Fetches all the shards
        :return:
        """
        it = range(self._distributor.shards_number)
        self.fetch_by_indexes(it)

    def fetch(self, **kwargs):
        """
        Fetch only one shard, distributes its part to the right local
        shards according to the distributor
        :param kwargs: depends on the self._distributor instance
        :return:
        """
        n = self._distributor.distribute(**kwargs)
        self.fetch_by_indexes([n])

    def fetch_multiple(self, params: list[dict]):
        it = [self._distributor.distribute(**kw) for kw in params]
        self.fetch_by_indexes(it)

    def fetch_modified(self):
        """
        Fetches only those shards that were modified locally
        :return:
        """
        self.fetch_by_indexes(self.shards.keys())

    def update_meta(self, other: dict[str, RuleMeta]):
        for rule, data in other.items():
            self.meta.setdefault(rule, {}).update(data)

    def fetch_meta(self):
        self.update_meta(self.io.read_meta())

    def write_meta(self):
        if self.meta:
            self.io.write_meta(self.meta)


class ShardsCollectionFactory:
    """
    Builds distributors but without writers
    """

    @staticmethod
    def _cloud_distributor(cloud: Cloud) -> ShardDataDistributor:
        match cloud:
            case Cloud.AWS:
                return AWSRegionDistributor(2)
            case _:
                return SingleShardDistributor()

    @staticmethod
    def from_cloud(cloud: Cloud) -> ShardsCollection:
        return ShardsCollection(
            distributor=ShardsCollectionFactory._cloud_distributor(cloud)
        )

    @staticmethod
    def from_tenant(tenant: 'Tenant') -> ShardsCollection:
        cloud = Cloud[tenant.cloud.upper()]
        return ShardsCollectionFactory.from_cloud(cloud)

    @staticmethod
    def difference() -> ShardsCollection:
        """
        Event driven reports are differences (only new resources that were
        found by a job). Their size is mostly low, so we don't need sharding
        there
        """
        return ShardsCollection(distributor=SingleShardDistributor())
