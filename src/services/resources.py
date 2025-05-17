from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import chain
from typing import TYPE_CHECKING, Generator, Generic, Iterator, TypeVar, cast

from typing_extensions import Self

from helpers import get_path
from helpers.constants import GLOBAL_REGION, Cloud
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso

if TYPE_CHECKING:
    from c7n.manager import ResourceManager
    from c7n.query import TypeInfo as AWSTypeInfo
    from c7n_gcp.query import TypeInfo as GCPTypeInfo  # noqa

    from services.metadata import Metadata, RuleMetadata
    from services.sharding import BaseShardPart, ShardsCollection

_LOG = get_logger(__name__)

T = TypeVar('T')


class ResourceVisitor(ABC, Generic[T]):
    @abstractmethod
    def visitAWSResource(
        self, resource: 'AWSResource', /, *args, **kwargs
    ) -> T: ...

    @abstractmethod
    def visitAZUREResource(
        self, resource: 'AZUREResource', /, *args, **kwargs
    ) -> T: ...

    @abstractmethod
    def visitGOOGLEResource(
        self, resource: 'GOOGLEResource', /, *args, **kwargs
    ) -> T: ...

    @abstractmethod
    def visitK8SResource(
        self, resource: 'K8SResource', /, *args, **kwargs
    ) -> T: ...


class CloudResource(ABC):
    __slots__ = (
        'id',
        'name',
        'location',
        'sync_date',
        'data',
        'resource_type',
        '_frozen',
        '_hash',
        '_discriminators',
    )

    def __init__(
        self,
        *,
        id: str,
        name: str,
        location=GLOBAL_REGION,
        resource_type: str,
        sync_date: float,
        data: dict,
        discriminators: tuple[str, ...] = (),
    ):
        self.id: str = id
        self.name: str = name
        self.location: str = location
        self.resource_type: str = resource_type
        self.sync_date: float = sync_date
        self.data: dict = data

        self._discriminators = discriminators
        self._hash = None
        self._frozen = True

    @contextmanager
    def unsafe_unfreeze(self) -> Generator[Self, None, None]:
        """
        If you know what you're doing
        """
        previous = self._frozen
        object.__setattr__(self, '_frozen', False)
        try:
            yield self
        finally:
            object.__setattr__(self, '_frozen', previous)

    def unsafe_reset_hash(self):
        """
        You should understand that this method can cause bugs if you kept 
        the resource inside some hashable collections.
        """
        with self.unsafe_unfreeze():
            self._hash = None

    @abstractmethod
    def accept(self, visitor: ResourceVisitor[T], /, *args, **kwargs) -> T: ...

    @abstractmethod
    def _members(self) -> tuple:
        """
        Should return a tuple of members that must be used for hashing and
        comparing two different resources
        """

    def __setattr__(self, key, value):
        if getattr(self, '_frozen', False):
            raise AttributeError(
                'Trying to set attribute on a frozen instance'
            )
        return super().__setattr__(key, value)

    def __eq__(self, other):
        if type(self) is not type(other):
            return False
        return self._members() == other._members()

    def __hash__(self) -> int:
        if self._hash is None:
            object.__setattr__(self, '_hash', hash(self._members()))
        return self._hash

    def __repr__(self) -> str:
        return f'<{self.resource_type}: {self.id}>'

    @property
    def region(self) -> str:
        return self.location


class AWSResource(CloudResource):
    __slots__ = 'arn', 'date'

    def __init__(
        self, *, region: str, arn: str | None, date: str | int | None, **kwargs
    ):
        self.arn: str | None = arn
        self.date: str | int | None = date
        super().__init__(**kwargs, location=region)

    @property
    def tags(self) -> dict:
        t = self.data.get('Tags') or []
        return {pair['Key']: pair['Value'] for pair in t}

    @property
    def labels(self) -> dict:
        return self.tags

    def _members(self) -> tuple:
        return (
            self.id,
            self.name,
            self.arn,
            self.region,
            self.resource_type,
            self._discriminators,
        )

    def accept(self, visitor: ResourceVisitor[T], /, *args, **kwargs) -> T:
        return visitor.visitAWSResource(self, *args, **kwargs)

    def date_as_utc_iso(self) -> str | None:
        if self.date is None:
            return
        # different aws resources have different types of their date
        if isinstance(self.date, str):
            try:
                return utc_iso(utc_datetime(self.date))
            except ValueError:
                return self.date
        # isinstance(self.date, (int, float))
        ts = self.date
        if ts > 10**11:
            # probably milliseconds
            ts /= 1000
        return utc_iso(datetime.fromtimestamp(ts, tz=timezone.utc))


class AZUREResource(CloudResource):
    @property
    def tags(self) -> dict:
        return self.data.get('tags') or {}

    @property
    def labels(self) -> dict:
        return self.tags

    @property
    def group(self) -> str | None:
        return self.data.get('resourceGroup')

    @property
    def type(self) -> str:
        return self.data['type']

    def _members(self) -> tuple:
        return (
            self.id,
            self.name,
            self.location,
            self.resource_type,
            self._discriminators,
        )

    def accept(self, visitor: ResourceVisitor[T], /, *args, **kwargs) -> T:
        return visitor.visitAZUREResource(self, *args, **kwargs)


class GOOGLEResource(CloudResource):
    __slots__ = ('urn',)

    def __init__(self, *, urn: str | None, **kwargs):
        self.urn: str | None = urn
        super().__init__(**kwargs)

    @property
    def labels(self) -> dict:
        return self.data.get('labels') or {}

    @property
    def tags(self) -> dict:
        return self.labels

    def _members(self) -> tuple:
        return (
            self.id,
            self.name,
            self.urn,
            self.location,
            self.resource_type,
            self._discriminators,
        )

    def accept(self, visitor: ResourceVisitor[T], /, *args, **kwargs) -> T:
        return visitor.visitGOOGLEResource(self, *args, **kwargs)


class K8SResource(CloudResource):
    __slots__ = ('namespace',)

    def __init__(self, namespace: str | None, **kwargs):
        self.namespace = namespace
        super().__init__(**kwargs)

    @property
    def labels(self) -> dict:
        return self.data.get('labels') or {}

    @property
    def tags(self) -> dict:
        return self.labels

    def _members(self) -> tuple:
        return (
            self.id,
            self.name,
            self.namespace,
            self.resource_type,
            self._discriminators,
        )

    def accept(self, visitor: ResourceVisitor[T], /, *args, **kwargs) -> T:
        return visitor.visitK8SResource(self, *args, **kwargs)


class InPlaceResourceView(ResourceVisitor[dict]):
    """
    Converts resource to its dict representation. Does not create a new dict
    object but uses the one inside the resource for performance purpose. So,
    be careful.
    """

    def __init__(self, full: bool = False):
        self._full = full

    def _get_base(
        self, resource: 'CloudResource', fields: tuple[str, ...]
    ) -> dict:
        if self._full:
            return resource.data
        else:
            return {k: resource.data[k] for k in fields if k in resource.data}

    def visitAWSResource(
        self, resource: 'AWSResource', /, *args, **kwargs
    ) -> dict:
        base = self._get_base(resource, kwargs.get('report_fields', ()))
        base['id'] = resource.id
        base['name'] = resource.name
        if resource.arn is not None:
            base['arn'] = resource.arn
        if resource.date is not None:
            base['date'] = resource.date_as_utc_iso()
        return base

    def visitAZUREResource(
        self, resource: 'AZUREResource', /, *args, **kwargs
    ) -> dict:
        base = self._get_base(resource, kwargs.get('report_fields', ()))
        base['id'] = resource.id
        base['name'] = resource.name
        return base

    def visitGOOGLEResource(
        self, resource: 'GOOGLEResource', /, *args, **kwargs
    ) -> dict:
        base = self._get_base(resource, kwargs.get('report_fields', ()))
        base['id'] = resource.id
        base['name'] = resource.name
        if resource.urn is not None:
            base['urn'] = resource.urn
        return base

    def visitK8SResource(
        self, resource: 'K8SResource', /, *args, **kwargs
    ) -> dict:
        base = self._get_base(resource, kwargs.get('report_fields', ()))
        base['id'] = resource.id
        base['name'] = resource.name
        if resource.namespace is not None:
            base['namespace'] = resource.namespace
        return base


def load_cc_providers():
    from c7n.resources import load_available

    _LOG.info('Going to load all available Cloud Custodian providers')
    load_available(resources=True)
    _LOG.info('Providers were loaded')


def prepare_resource_type(rt: str, cloud: Cloud) -> str:
    if '.' in rt:
        return rt
    match cloud:
        case Cloud.AWS:
            return 'aws.' + rt
        case Cloud.AZURE:
            return 'azure.' + rt
        case Cloud.GOOGLE | Cloud.GCP:
            return 'gcp.' + rt
        case Cloud.KUBERNETES | Cloud.K8S:
            return 'k8s.' + rt


def load_manager(rt: str) -> type['ResourceManager'] | None:
    from c7n.provider import get_resource_class

    _LOG.debug(f'Going to load CC manager: {rt}')
    try:
        return get_resource_class(rt)
    except (KeyError, AssertionError):
        _LOG.warning(f'Could not load resource type: {rt}')
        return


def _get_arn(
    res: dict, model: 'AWSTypeInfo', account_id: str, region: str
) -> str:
    """
    Tries to retrieve resource arn
    """
    arn_key = getattr(model, 'arn', None)
    assert arn_key is not False, 'Should be checked beforehand'
    if arn_key:
        return get_path(res, arn_key)
    # arn_key is None
    _id: str = get_path(res, cast(str, model.id))
    if _id.startswith('arn'):
        return _id
    from c7n.utils import generate_arn

    return generate_arn(
        service=model.arn_service or model.service,
        resource=_id,
        region=''
        if model.global_resource or region == GLOBAL_REGION
        else region,
        account_id=account_id,
        resource_type=model.arn_type,
        separator=model.arn_separator,
    )


def to_aws_resources(
    part: 'BaseShardPart', rt: str, metadata: 'RuleMetadata', account_id: str
) -> Generator[AWSResource, None, None]:
    if len(part.resources) == 0:
        return

    rt = prepare_resource_type(rt, Cloud.AWS)
    factory = load_manager(rt)
    if not factory:
        return
    m = getattr(factory, 'resource_type', None)
    if not m:
        _LOG.warning(f'{factory} has no resource_type')
        return

    has_arn = factory.has_arn()
    is_cloudtrail = rt == 'aws.cloudtrail'

    for res in part.resources:
        date = get_path(res, m.date) if m.date else None
        # needed to distinguish different resources with the same resource
        # type (aws.account) for example
        service = metadata.service
        region = part.location

        if is_cloudtrail and res.get('IsMultiRegionTrail'):
            _LOG.debug(
                'Found multiregional trail. Moving it to multiregional region'
            )
            region = GLOBAL_REGION

        yield AWSResource(
            region=region,
            arn=None if not has_arn else _get_arn(res, m, account_id, region),
            date=date,
            id=get_path(res, m.id),
            name=get_path(res, m.name),
            resource_type=rt,
            sync_date=part.timestamp,
            data=res,
            discriminators=(service,) if service else (),
        )


def to_azure_resources(
    part: 'BaseShardPart', rt: str
) -> Generator[AZUREResource, None, None]:
    if len(part.resources) == 0:
        return

    rt = prepare_resource_type(rt, Cloud.AZURE)
    factory = load_manager(rt)
    if not factory:
        return
    m = getattr(factory, 'resource_type', None)
    if not m:
        _LOG.warning(f'{factory} has no resource_type')
        return

    for res in part.resources:
        yield AZUREResource(
            id=get_path(res, m.id),
            name=get_path(res, m.name),
            location=part.location,
            resource_type=rt,
            sync_date=part.timestamp,
            data=res,
        )


def to_google_resources(
    part: 'BaseShardPart', rt: str, metadata: 'RuleMetadata', account_id: str
) -> Generator[GOOGLEResource, None, None]:
    if len(part.resources) == 0:
        return

    rt = prepare_resource_type(rt, Cloud.GOOGLE)
    factory = load_manager(rt)
    if not factory:
        return
    m: 'GCPTypeInfo | None' = getattr(factory, 'resource_type', None)
    if not m:
        _LOG.warning(f'{factory} has no resource_type')
        return

    for res in part.resources:
        service = metadata.service
        # TODO: some how test against breaking changes

        yield GOOGLEResource(
            urn=m._get_urn(res, account_id),
            id=get_path(res, m.id),
            name=get_path(res, m.name),
            location=m._get_location(res),
            resource_type=rt,
            sync_date=part.timestamp,
            data=res,
            discriminators=(service,) if service else (),
        )


def to_k8s_resources(
    part: 'BaseShardPart', rt: str
) -> Generator[K8SResource, None, None]:
    if len(part.resources) == 0:
        return

    rt = prepare_resource_type(rt, Cloud.KUBERNETES)
    factory = load_manager(rt)
    if not factory:
        return
    m = getattr(factory, 'resource_type', None)
    if not m:
        _LOG.warning(f'{factory} has no resource_type')
        return

    for res in part.resources:
        yield K8SResource(
            namespace=get_path(res, 'metadata.namespace'),
            id=get_path(res, m.id),
            name=get_path(res, m.name),
            resource_type=rt,
            sync_date=part.timestamp,
            data=res,
        )


def iter_rule_resources_iterator(
    collection: 'ShardsCollection',
    metadata: 'Metadata',
    cloud: Cloud,
    account_id: str = '',
    *,
    policies: tuple[str, ...] | set[str] = (),
    regions: tuple[str, ...] | set[str] = (),
    resource_types: tuple[str, ...] | set[str] = (),
) -> Generator[tuple[str, Iterator[CloudResource]], None, None]:
    load_cc_providers()

    if resource_types:
        typ = set if len(resource_types) > 128 else tuple
        resource_types = typ(
            prepare_resource_type(rt, cloud) for rt in resource_types
        )

    meta = collection.meta

    rule_to_iters = {}

    for part in collection.iter_parts():
        policy = part.policy
        if policies and policy not in policies:
            continue

        rt = prepare_resource_type(meta[policy]['resource'], cloud)
        if resource_types and rt not in resource_types:
            continue

        match cloud:
            case Cloud.AWS:
                assert account_id, 'Account id must be provided for AWS'
                it = to_aws_resources(
                    part, rt, metadata.rule(policy), account_id
                )
            case Cloud.AZURE:
                it = to_azure_resources(part, rt)
            case Cloud.GOOGLE | Cloud.GCP:
                assert account_id, 'Account id must be provided for GOOGLE'
                it = to_google_resources(
                    part, rt, metadata.rule(policy), account_id
                )
            case Cloud.KUBERNETES | Cloud.K8S:
                it = to_k8s_resources(part, rt)
            case _:
                raise  # never

        if regions:
            it = (item for item in it if item.location in regions)
        rule_to_iters.setdefault(policy, []).append(it)

    for rule, iters in rule_to_iters.items():
        yield rule, chain(*iters)


def iter_rule_resources(
    collection: 'ShardsCollection',
    metadata: 'Metadata',
    cloud: Cloud,
    account_id: str = '',
    *,
    policies: tuple[str, ...] | set[str] = (),
    regions: tuple[str, ...] | set[str] = (),
    resource_types: tuple[str, ...] | set[str] = (),
) -> Generator[tuple[str, CloudResource], None, None]:
    it = iter_rule_resources_iterator(
        collection=collection,
        metadata=metadata,
        cloud=cloud,
        account_id=account_id,
        policies=policies,
        regions=regions,
        resource_types=resource_types,
    )
    for rule, resources in it:
        for res in resources:
            yield rule, res
