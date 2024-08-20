from abc import ABC, abstractmethod
from collections import ChainMap, defaultdict
import io
from pathlib import PurePosixPath
import pickle
import tempfile
import time
from typing import (
    BinaryIO,
    Generator,
    Iterable,
    Iterator,
    Literal,
    TYPE_CHECKING,
    TypedDict,
    cast,
)

import msgspec

from helpers import hashable, urljoin
from helpers.constants import Cloud, GLOBAL_REGION
from services import SP
from services.clients.s3 import S3Client

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant

# do not change the order, just append new regions. This collection is only
# for shards distributor
AWS_REGIONS = (
    'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
    'ap-south-1', 'ap-northeast-1', 'ap-northeast-2', 'ap-northeast-3',
    'ap-southeast-1', 'ap-southeast-2', 'ca-central-1', 'eu-central-1',
    'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-north-1', 'sa-east-1',
    'ap-southeast-3', 'ap-southeast-4', 'af-south-1', 'ap-east-1',
    'ap-south-2', 'eu-south-1', 'eu-south-2', 'eu-central-2',
    'il-central-1', 'me-south-1', 'me-central-1', 'us-gov-east-1',
    'us-gov-west-1'
)


class ShardPartDict(TypedDict):
    p: str  # policy name
    l: str  # region
    r: list[dict]  # resources
    t: float  # timestamp


class BaseShardPart:
    """
    Keeps a list of resources and some attributes that define the belonging
    of these resources. Can be region, namespace, group - something that
    differentiate these resources from some others
    Each part has policy attribute. We assume that if some policy (policy
    in a region in case of AWS) was scanned its output can safely be
    considered true and the previous output can be overriden by the new one.
    This class is just an interface
    """
    policy: str
    location: str
    resources: list[dict]
    timestamp: float

    def drop(self): ...

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self.policy}:{self.location}>'

    def serialize(self) -> ShardPartDict:
        return {
            'p': self.policy,
            'l': self.location,
            'r': self.resources,
            't': self.timestamp
        }


class ShardPart(msgspec.Struct, BaseShardPart, frozen=True):
    policy: str = msgspec.field(name='p')
    location: str = msgspec.field(name='l', default=GLOBAL_REGION)
    timestamp: float = msgspec.field(default_factory=time.time, name='t')
    resources: list[dict] = msgspec.field(default_factory=list, name='r')


class LazyPickleShardPart(BaseShardPart):
    __slots__ = 'policy', 'location', '_resources', 'timestamp', 'filepath'

    def __init__(self, filepath: str, policy: str,
                 location: str = GLOBAL_REGION,
                 timestamp: float | None = None):
        self.policy: str = policy
        self.location: str = location
        self.timestamp: float = timestamp or time.time()
        self._resources: list[dict] | None = None
        self.filepath: str = filepath

    @classmethod
    def from_resources(cls, resources: list[dict], policy: str,
                       location: str = GLOBAL_REGION):
        """
        Creates shard part with given resources but immediately dumps them
        to inner file storage
        :param resources:
        :param policy:
        :param location:
        :return:
        """
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            pickle.dump(resources, fp, protocol=pickle.HIGHEST_PROTOCOL)
        return cls(
            filepath=fp.name,
            policy=policy,
            location=location
        )

    @property
    def resources(self) -> list[dict]:
        if self._resources is None:
            with open(self.filepath, 'rb') as fp:
                self._resources = pickle.load(fp)
        return self._resources

    def drop(self) -> None:
        self._resources = None  # not sure if it can help


class Shard(Iterable[BaseShardPart]):
    """
    Shard store shard parts. This shard implementation uses policy and
    location as a key to a shard part. This means that we can update
    resources within policy & region. Maybe we will need some other
    implementations in the future
    """
    __slots__ = ('_data',)

    def __init__(self, data: dict | None = None):
        self._data: dict[tuple[str, str], BaseShardPart] = data or dict()

    def __iter__(self) -> Iterator[BaseShardPart]:
        return self._data.values().__iter__()

    def __len__(self) -> int:
        return self._data.__len__()

    @property
    def raw(self) -> dict[tuple[str, str], BaseShardPart]:
        return self._data

    def put(self, part: BaseShardPart) -> None:
        """
        Adds a part to the shard in case such part does not exist yet.
        Each part contains its timestamp. In case we try to put a part and
        the same one already exists - the one with higher timestamp will
        be kept
        :param part:
        :return:
        """
        key = (part.policy, part.location)
        existing = self._data.get(key)
        if existing and existing.timestamp > part.timestamp:
            return
        self._data[key] = part

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
    def key(self, part: BaseShardPart) -> dict:
        """
        Must retrieve data from the given part based on which the
        distribution happens
        :param part:
        :return:
        """

    def distribute_part(self, part: BaseShardPart) -> int:
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

    def key(self, part: BaseShardPart) -> dict:
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

    def key(self, part: BaseShardPart) -> dict:
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

    def read_raw_many(self, numbers: Iterable[int]
                      ) -> Iterator[list[BaseShardPart]]:
        return filter(lambda x: x is not None, map(self.read_raw, numbers))

    @abstractmethod
    def read_raw(self, n: int) -> list[BaseShardPart] | None:
        """
        Reads a specific shard but returns raw json
        :param n:
        :return:
        """

    @abstractmethod
    def write_meta(self, meta: dict):
        ...

    @abstractmethod
    def read_meta(self) -> dict:
        ...


class ShardsS3IO(ShardsIO):
    """
    Writer V1
    """
    __slots__ = '_bucket', '_root', '_client', '_encoder'

    def __init__(self, bucket: str, key: str, client: S3Client):
        """
        :param bucket:
        :param key: root folder where to put shards
        """
        self._bucket = bucket
        self._root = key
        self._client = client
        self._encoder = msgspec.json.Encoder()

    def shard_to_filelike(self, shard: Shard) -> BinaryIO:
        encoder = self._encoder
        buf = tempfile.TemporaryFile()
        _first = True
        for part in shard:
            if _first:
                s = b'['
                _first = False
            else:
                s = b','
            buf.write(s)
            buf.write(encoder.encode(part.serialize()))
        buf.write(b']')
        buf.seek(0)
        return buf

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
            body=self.shard_to_filelike(shard),
        )

    def read_raw(self, n: int) -> list[BaseShardPart] | None:
        obj = self._client.gz_get_object(
            bucket=self._bucket,
            key=self._key(n),
            gz_buffer=tempfile.TemporaryFile(),
        )
        if not obj:
            return
        return msgspec.json.decode(cast(io.BytesIO, obj).getvalue(),
                                   type=list[ShardPart])

    def write_meta(self, meta: dict):
        self._client.gz_put_json(
            bucket=self._bucket,
            key=str((PurePosixPath(self._root) / 'meta.json')),
            obj=meta
        )

    def read_meta(self) -> dict:
        return self._client.gz_get_json(
            bucket=self._bucket,
            key=str((PurePosixPath(self._root) / 'meta.json'))
        ) or {}


class ShardsS3IOV2(ShardsS3IO):
    """
    Writer v2. Writes shard parts as json lines
    """
    @staticmethod
    def shard_to_filelike(shard: Shard) -> BinaryIO:
        encoder = msgspec.json.Encoder()
        buf = tempfile.TemporaryFile()
        first = True
        for part in shard:
            if not first:
                buf.write(b'\n')
            else:
                first = False
            buf.write(encoder.encode(part.serialize()))
        buf.seek(0)
        return buf


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
    __slots__ = '_distributor', '_io', 'shards', 'meta'

    def __init__(self, distributor: ShardDataDistributor,
                 io: ShardsIO | None = None):
        self._distributor = distributor
        self._io = io

        self.shards: defaultdict[int, Shard] = defaultdict(Shard)
        self.meta = {}

    def __iter__(self) -> Iterator[tuple[int, Shard]]:
        """
        Iterates over inner shards and their number in order
        :return:
        """
        return ShardsIterator(
            shards=self.shards,
            n=self._distributor.shards_number
        )

    def __len__(self) -> int:
        return self.shards.__len__()

    def iter_parts(self) -> Generator[BaseShardPart, None, None]:
        for _, shard in self:
            yield from shard

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
        # todo rewrite
        new = ShardsCollectionFactory.difference()

        current = ChainMap(*[shard.raw for _, shard in self])
        other = ChainMap(*[shard.raw for _, shard in other])
        for key, part in current.items():
            other_part = other.get(key)
            if not other_part:  # definitely new
                new.put_part(part)
                continue
            current_res = set(hashable(res) for res in part.resources)
            other_res = set(hashable(res) for res in other_part.resources)
            new.put_part(ShardPart(
                policy=part.policy,
                location=part.location,
                resources=list(current_res - other_res)
            ))
        return new

    @property
    def distributor(self) -> ShardDataDistributor:
        return self._distributor

    @property
    def io(self) -> ShardsIO:
        return self._io

    @io.setter
    def io(self, value: ShardsIO):
        self._io = value

    def put_part(self, part: BaseShardPart) -> None:
        """
        Distribute the given shard part to its shard
        :param part:
        :return:
        """
        n = self._distributor.distribute_part(part)
        self.shards[n].put(part)

    def put_parts(self, parts: Iterable[BaseShardPart]) -> None:
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
        self._io.write_many(iter(self))

    def fetch_by_indexes(self, it: Iterable[int]):
        """
        Fetches shards by specified indexes
        """
        for parts in self._io.read_raw_many(set(it)):
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

    def update_meta(self, other: dict):
        for rule, data in other.items():
            self.meta.setdefault(rule, {}).update(data)

    def fetch_meta(self):
        self.update_meta(self._io.read_meta())

    def write_meta(self):
        if self.meta:
            self._io.write_meta(self.meta)


class ShardingConfig:
    filename = '.conf'

    __slots__ = 'version',

    def __init__(self, version: Literal[1, 2]):
        """
        :param version:
        1 - one shard is a json - list of dits
        2 - one shard is a bunch of JSONs separated by newline
        """
        self.version = version
        # todo add distributor, and maybe smt else here

    @classmethod
    def from_raw(cls, dct: dict) -> 'ShardingConfig':
        return cls(**dct)

    @classmethod
    def build_default(cls) -> 'ShardingConfig':
        return cls(version=1)


class ShardsCollectionFactory:
    """
    Builds distributors but without writers
    """
    @staticmethod
    def from_s3_key(cloud: Cloud, key: str) -> ShardsCollection:
        bucket = SP.environment_service.default_reports_bucket_name()
        item = SP.s3.gz_get_json(bucket, urljoin(key, ShardingConfig.filename))
        if item:
            conf = ShardingConfig(item)
        else:
            conf = ShardingConfig.build_default()
        col = ShardsCollection(
            distributor=ShardsCollectionFactory._cloud_distributor(cloud)
        )
        match conf.version:
            case 1:
                col.io = ShardsS3IO(bucket=bucket, key=key, client=SP.s3)
            case 2:
                col.io = ShardsS3IOV2(bucket=bucket, key=key, client=SP.s3)
            case _:
                raise RuntimeError('Invalid shards version')
        return col

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
