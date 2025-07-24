from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Generator,
    Generic,
    Iterable,
    Iterator,
    TypeVar,
    cast,
)

from typing_extensions import Self

from helpers import get_path
from helpers.constants import GLOBAL_REGION, Cloud
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from services.metadata import EMPTY_METADATA, Metadata, RuleMetadata

if TYPE_CHECKING:
    from c7n.manager import ResourceManager
    from c7n.query import TypeInfo as AWSTypeInfo
    from c7n_gcp.query import TypeInfo as GCPTypeInfo  # noqa

    from services.sharding import ShardPart, ShardsCollection

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

    @property
    @abstractmethod
    def tags(self) -> dict: ...

    @property
    def labels(self) -> dict:
        return self.tags


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
    def tags(self) -> dict:
        return self.data.get('labels') or {}

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
    def tags(self) -> dict:
        return self.data.get('labels') or {}

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
    Converts resource to its dict representation. If full = True, it
    does not create a new dict object but uses the one inside the resource
    for performance purpose. So, be careful.
    """

    __slots__ = ('_full',)

    def __init__(self, full: bool = False):
        self._full = full

    def _get_base(
        self, resource: 'CloudResource', fields: tuple[str, ...] = ()
    ) -> dict:
        if self._full:
            return resource.data  # not a copy !!!
        if not fields:
            return {}
        return {k: resource.data[k] for k in fields if k in resource.data}

    def visitAWSResource(
        self, resource: 'AWSResource', /, *args, **kwargs
    ) -> dict:
        base = self._get_base(resource, kwargs.get('report_fields', ()))
        base['id'] = resource.id
        base['name'] = resource.name
        if resource.arn is not None:
            base['arn'] = resource.arn
        # if resource.date is not None:
        #     base['date'] = resource.date_as_utc_iso()
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


class MaestroReportResourceView(ResourceVisitor[dict]):
    @staticmethod
    def _extend_with_report_fields(
        dct: dict, fields: tuple[str, ...], resource: 'CloudResource'
    ) -> None:
        for field in fields:
            _LOG.debug(f'Checking {field}')
            val = get_path(resource.data, field)
            if not val:
                _LOG.warning(
                    f'Resource {resource} does not have field {field}'
                )
                continue
            if any([str(val) in str(existing) for existing in dct.values()]):
                _LOG.debug(f'Field {field} will not be added to {resource}')
                continue
            _LOG.debug(f'Adding field {field} with value {val} to {resource}')
            dct[field] = val

    def visitAWSResource(
        self,
        resource: 'AWSResource',
        /,
        report_fields: tuple[str, ...] = (),
        **kwargs,
    ) -> dict:
        dct = {'id': resource.id, 'name': resource.name}
        if arn := resource.arn:
            dct['arn'] = arn

        if report_fields:
            self._extend_with_report_fields(dct, report_fields, resource)

        dct['sre:date'] = resource.sync_date
        return dct

    def visitAZUREResource(
        self,
        resource: 'AZUREResource',
        /,
        report_fields: tuple[str, ...] = (),
        **kwargs,
    ) -> dict:
        dct = {'id': resource.id, 'name': resource.name}
        if rg := resource.group:
            dct['resourceGroup'] = rg
        if report_fields:
            self._extend_with_report_fields(dct, report_fields, resource)
        dct['sre:date'] = resource.sync_date
        return dct

    def visitGOOGLEResource(
        self,
        resource: 'GOOGLEResource',
        /,
        report_fields: tuple[str, ...] = (),
        **kwargs,
    ) -> dict:
        dct = {'id': resource.id, 'name': resource.name}
        if urn := resource.urn:
            dct['urn'] = urn
        if report_fields:
            self._extend_with_report_fields(dct, report_fields, resource)
        dct['sre:date'] = resource.sync_date
        return dct

    def visitK8SResource(
        self,
        resource: 'K8SResource',
        /,
        report_fields: tuple[str, ...] = (),
        **kwargs,
    ) -> dict:
        dct = {'id': resource.id, 'name': resource.name}
        if namespace := resource.namespace:
            dct['namespace'] = namespace
        if report_fields:
            self._extend_with_report_fields(dct, report_fields, resource)
        dct['sre:date'] = resource.sync_date
        return dct


_CC_PROVIDERS_LOADED = False


def load_cc_providers():
    global _CC_PROVIDERS_LOADED

    if not _CC_PROVIDERS_LOADED:
        from c7n.resources import load_available

        _LOG.info('Going to load all available Cloud Custodian providers')
        loaded = load_available(resources=True)
        _CC_PROVIDERS_LOADED = True
        _LOG.info('Loaded providers: ' + ', '.join(loaded))
    else:
        _LOG.info('Cloud Custodian providers were already loaded')


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


class _ExecutionContext:
    """
    Mocks one from CC
    """

    __slots__ = ('options',)

    def __init__(self, options):
        self.options = options

    def __getattr__(self, item):
        return None


def _get_arns(
    factory: type['ResourceManager'],
    resources: list[dict],
    region: str,
    account_id: str,
) -> list[str]:
    """
    Invokes get_arns from resource manager as a last resort. Requires us
    to initialize manager class so we set mocked context.
    factory.has_arns must be checked beforehand
    """

    from c7n.config import Config

    # TODO: make sure to check against breaking changes from CC
    config = Config.empty(region=region, account_id=account_id)
    manager = factory(ctx=_ExecutionContext(config), data={})
    return manager.get_arns(resources)


def _get_arn_fast(res: dict, model: 'AWSTypeInfo') -> str | None:
    """
    Tries to resolve arn from the resource without invoking generate_arn.
    factory.has_arns must be checked beforehand
    """
    arn_key = getattr(model, 'arn', None)
    assert arn_key is not False, 'Should be checked beforehand'
    if arn_key:
        return get_path(res, arn_key)
    # arn_key is None
    _id: str = get_path(res, cast(str, model.id))
    if _id.startswith('arn'):
        return _id


def _get_id_name(res: dict, rt: str, model) -> tuple[str | None, str | None]:
    _id = get_path(res, model.id)
    if not _id:
        _LOG.error(f'Resource of type {rt} does not have an id')
        # we can almost be sure that id exists
    name = get_path(res, model.name)
    if not name:
        _LOG.warning(f'Resource of type {rt} does not have name')
    return _id, name


def _resolve_s3_location(res: dict) -> str:
    # LocationConstraint is None if region is us-east-1
    return res.get('Location', {}).get('LocationConstraint') or 'us-east-1'


def _resolve_azure_location(res: dict) -> str:
    if 'location' in res:
        return res['location']
    return GLOBAL_REGION


def _resolve_google_location(res: dict, model: 'GCPTypeInfo') -> str:
    """
    All Google rules are global, but each individual resource can have its specific region
    """
    # TODO: check for breaking changes, implement tests
    loc = model._get_location(res)
    if loc == 'global' and 'locationId' in res:
        _LOG.warning('Resource contains locationId but CC returned global')
        return res['locationId']
    return loc


def _resolve_name_from_aws_tags(
    tags: Iterable[dict], allowed=('Name', 'name')
) -> str | None:
    for tag in tags:
        if tag['Key'] in allowed:
            return tag['Value']


def to_aws_resources(
    part: 'ShardPart', rt: str, metadata: 'RuleMetadata', account_id: str = ''
) -> Generator[AWSResource, None, None]:
    """
    If account_id is provided it will be used to generated arns where possible
    and where there is no arn provided by AWS
    """
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
    arns: list[str] | None = None  # initialized first when needed

    is_cloudtrail = rt == 'aws.cloudtrail'
    is_s3 = rt == 'aws.s3'
    disc = (metadata.service,) if metadata.service else ()
    timestamp = part.last_successful_timestamp()
    assert timestamp, 'Only parts that executed successfully allowed'

    for i, res in enumerate(part.resources):
        date = get_path(res, m.date) if m.date else None

        if is_cloudtrail and res.get('IsMultiRegionTrail'):
            _LOG.debug(
                'Found multiregional trail. Moving it to multiregional region'
            )
            region = GLOBAL_REGION
        elif is_s3:
            _LOG.debug(
                'Found S3 bucket. Resolving region from region constraints'
            )
            region = _resolve_s3_location(res)
        else:
            region = part.location

        # bear with me
        if has_arn and arns:
            arn = arns[i]
        elif has_arn:  # and not arns
            arn = _get_arn_fast(res, m)
            if arn is None and account_id and hasattr(factory, 'get_arns'):
                # - not trying to generate arn if account id is not provided
                # - hasattr checks against their bugs
                arns = _get_arns(
                    factory, part.resources, part.location, account_id
                )
                arn = arns[i]
        else:  # not has_arn
            arn = None

        _id, name = _get_id_name(res, rt, m)
        if not name and (tags := res.get('Tags')):
            # try to resolve name from tags
            name = _resolve_name_from_aws_tags(tags)
        if not name:
            name = _id

        yield AWSResource(
            region=region,
            arn=arn,
            date=date,
            id=_id,
            name=name,
            resource_type=rt,
            sync_date=timestamp,
            data=res,
            discriminators=disc,
        )


def to_azure_resources(
    part: 'ShardPart', rt: str
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
    timestamp = part.last_successful_timestamp()
    assert timestamp, 'Only parts that executed successfully allowed'

    for res in part.resources:
        _id, name = _get_id_name(res, rt, m)
        if not name:
            name = _id
        yield AZUREResource(
            id=_id,
            name=name,
            location=_resolve_azure_location(res),
            resource_type=rt,
            sync_date=timestamp,
            data=res,
        )


def to_google_resources(
    part: 'ShardPart', rt: str, metadata: 'RuleMetadata', account_id: str = ''
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

    urn_has_project = m.urn_has_project
    disc = (metadata.service,) if metadata.service else ()
    timestamp = part.last_successful_timestamp()
    assert timestamp, 'Only parts that executed successfully allowed'

    for res in part.resources:
        if account_id or not urn_has_project:
            urn = m._get_urn(res, account_id)
        else:
            urn = None

        _id, name = _get_id_name(res, rt, m)
        if not name:
            name = _id
        if 'id' in res and _id != res['id']:
            # for some reason CC defines id the same as name for many google
            # resources that do have a separate id. Maybe that's because
            # Google's id is just a number, and it's better to show the name.
            # Anyway, it's still an id
            _id = res['id']

        yield GOOGLEResource(
            urn=urn,
            id=_id,
            name=name,
            location=_resolve_google_location(res, m),
            resource_type=rt,
            sync_date=timestamp,
            data=res,
            discriminators=disc,
        )


def to_k8s_resources(
    part: 'ShardPart', rt: str
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
    timestamp = part.last_successful_timestamp()
    assert timestamp, 'Only parts that executed successfully allowed'

    for res in part.resources:
        _id, name = _get_id_name(res, rt, m)
        if not name:
            name = _id
        yield K8SResource(
            namespace=get_path(res, 'metadata.namespace'),
            id=_id,
            name=name,
            resource_type=rt,
            sync_date=timestamp,
            data=res,
        )


def iter_rule_region_resources(
    collection: 'ShardsCollection',
    cloud: Cloud,
    metadata: Metadata = EMPTY_METADATA,
    account_id: str = '',
    *,
    policies: list[str] | tuple[str, ...] | set[str] | None = None,
    regions: list[str] | tuple[str, ...] | set[str] | None = None,
    resource_types: list[str] | tuple[str, ...] | set[str] | None = None,
) -> Generator[tuple[str, str, Iterator[CloudResource]], None, None]:
    """
    Each rule & region pair is yielded only once. Yields a tuple of
    rule, region and resources that were found by executing that rule against
    that region (meaning that individual region of each
    exceptional resource is not resolved here.). Filtering by regions is
    performed based on that global region scope
    """

    load_cc_providers()

    if resource_types is not None:
        resource_types = resource_types.__class__(
            [prepare_resource_type(rt, cloud) for rt in resource_types]
        )

    meta = collection.meta

    # NOTE: here we iterate only over those rules that executed successfully
    # at least once even if their latest execution was failed
    for part in collection.iter_parts():
        policy = part.policy
        location = part.location
        if policies is not None and policy not in policies:
            continue

        if regions is not None and location not in regions:
            continue

        rt = prepare_resource_type(meta[policy]['resource'], cloud)
        if resource_types is not None and rt not in resource_types:
            continue

        if len(part.resources) == 0:
            yield policy, location, iter(())
            continue

        match cloud:
            case Cloud.AWS:
                it = to_aws_resources(
                    part, rt, metadata.rule(policy), account_id
                )
            case Cloud.AZURE:
                it = to_azure_resources(part, rt)
            case Cloud.GOOGLE | Cloud.GCP:
                it = to_google_resources(
                    part, rt, metadata.rule(policy), account_id
                )
            case Cloud.KUBERNETES | Cloud.K8S:
                it = to_k8s_resources(part, rt)
            case _:
                raise  # never
        yield policy, location, it


def iter_rule_resources(
    collection: 'ShardsCollection',
    cloud: Cloud,
    metadata: Metadata = EMPTY_METADATA,
    account_id: str = '',
    *,
    policies: list[str] | tuple[str, ...] | set[str] | None = None,
    regions: list[str] | tuple[str, ...] | set[str] | None = None,
    resource_types: list[str] | tuple[str, ...] | set[str] | None = None,
) -> Generator[tuple[str, Iterator[CloudResource]], None, None]:
    """
    Each rule is yielded only once. This method skips regions from shard parts
    assuming that you will prefer regions from individual resource.
    """
    it = iter_rule_region_resources(
        collection=collection,
        cloud=cloud,
        metadata=metadata,
        account_id=account_id,
        policies=policies,
        regions=None,  # important, we will filter based by regions here
        resource_types=resource_types,
    )

    rule_to_iters = defaultdict(list)
    for rule, _, resources in it:
        if regions is not None:
            resources = (
                item for item in resources if item.location in regions
            )
        rule_to_iters[rule].append(resources)

    for rule, iters in rule_to_iters.items():
        yield rule, chain(*iters)


def iter_rule_resource_region_resources(
    collection: 'ShardsCollection',
    cloud: Cloud,
    metadata: Metadata = EMPTY_METADATA,
    account_id: str = '',
    *,
    policies: list[str] | tuple[str, ...] | set[str] | None = None,
    regions: list[str] | tuple[str, ...] | set[str] | None = None,
    resource_types: list[str] | tuple[str, ...] | set[str] | None = None,
) -> Generator[tuple[str, str, list[CloudResource]], None, None]:
    """
    Groups by individual resource region
    """
    it = iter_rule_resources(
        collection=collection,
        cloud=cloud,
        metadata=metadata,
        account_id=account_id,
        policies=policies,
        regions=regions,
        resource_types=resource_types,
    )
    for rule, resources in it:
        mapped = defaultdict(list)
        for r in resources:
            mapped[r.location].append(r)
        for k, v in mapped.items():
            yield rule, k, v


def iter_rule_resource(
    collection: 'ShardsCollection',
    cloud: Cloud,
    metadata: 'Metadata' = EMPTY_METADATA,
    account_id: str = '',
    *,
    policies: list[str] | tuple[str, ...] | set[str] | None = None,
    regions: list[str] | tuple[str, ...] | set[str] | None = None,
    resource_types: list[str] | tuple[str, ...] | set[str] | None = None,
) -> Generator[tuple[str, CloudResource], None, None]:
    it = iter_rule_resources(
        collection=collection,
        cloud=cloud,
        metadata=metadata,
        account_id=account_id,
        policies=policies,
        regions=regions,
        resource_types=resource_types,
    )
    for rule, resources in it:
        for res in resources:
            yield rule, res
