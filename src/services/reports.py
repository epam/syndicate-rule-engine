import bisect
import gzip
import io
from abc import ABC
from datetime import date, datetime
from functools import cached_property, cmp_to_key
from itertools import chain
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Iterator,
    Literal,
    TypeVar,
    cast,
)

import msgspec
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers import deep_get, iter_key_values
from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    CAASEnv,
    Cloud,
    JobState,
    ReportType,
)
from helpers.log_helper import get_logger
from helpers.reports import (
    SeverityCmp,
    keep_highest,
    service_from_resource_type,
)
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services.ambiguous_job_service import AmbiguousJob
from services.base_data_service import BaseDataService
from services.clients.s3 import S3Client, S3Url
from services.metadata import Metadata
from services.platform_service import Platform
from services.report_service import ReportService
from services.reports_bucket import ReportMetricsBucketKeysBuilder
from services.resources import (
    CloudResource,
    iter_rule_resources,
)
from services.sharding import RuleMeta, ShardsCollection

_LOG = get_logger(__name__)


class JobMetricsDataSource:
    """
    Allows to retrieve data from jobs within one customer. Object is immutable
    """

    def __init__(self, jobs: Iterable[AmbiguousJob]):
        """
        Assumes that jobs will be already sorted in ascending order
        """
        self._jobs = tuple(jobs)

    def subset(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        tenant: str | set[str] | list[str] | tuple[str, ...] | None = None,
        platform: str | set[str] | list[str] | tuple[str, ...] | None = None,
        affiliation: Literal['tenant', 'platform', None] = None,
        job_state: JobState | None = None,
    ) -> 'JobMetricsDataSource':
        """
        Returns new object with jobs within the range. Including start but
        not including end
        """

        def _key(j: AmbiguousJob):
            return utc_datetime(j.submitted_at)

        st = bisect.bisect_left(self._jobs, start, key=_key) if start else 0
        en = (
            bisect.bisect_left(self._jobs, end, key=_key)
            if end
            else len(self._jobs)
        )
        jobs = self._jobs[st:en]

        if tenant:
            _items = (
                tenant if isinstance(tenant, (set, list, tuple)) else (tenant,)
            )
            jobs = filter(lambda j: j.tenant_name in _items, jobs)

        if platform:
            _items = (
                platform
                if isinstance(platform, (set, list, tuple))
                else (platform,)
            )
            jobs = filter(lambda j: j.platform_id in _items, jobs)
        match affiliation:
            case 'tenant':
                jobs = filter(lambda j: not j.is_platform_job, jobs)
            case 'platform':
                jobs = filter(lambda j: j.is_platform_job, jobs)
            case None:
                pass
        if job_state:
            jobs = filter(lambda j: j.status is job_state, jobs)
        return self.__class__(jobs)

    def __getitem__(self, key) -> 'JobMetricsDataSource':
        if isinstance(key, slice) and (
            isinstance(key.stop, datetime) or isinstance(key.start, datetime)
        ):
            if key.step:
                raise NotImplementedError(
                    'step is not implemented for datetime slicing'
                )  # TODO: maybe implement for timedelta step
            return self.subset(start=key.start, end=key.stop)
        return self.__class__(self._jobs[key])

    def __iter__(self) -> Iterator[AmbiguousJob]:
        return self._jobs.__iter__()

    def __len__(self) -> int:
        return self._jobs.__len__()

    @cached_property
    def n_standard(self) -> int:
        return sum(map(lambda j: not j.is_ed_job, self._jobs))

    @cached_property
    def n_event_driven(self) -> int:
        return len(self) - self.n_standard

    @cached_property
    def n_platform(self) -> int:
        return sum(map(lambda j: j.is_platform_job, self._jobs))

    @cached_property
    def n_succeeded(self) -> int:
        return sum(map(lambda j: j.is_succeeded, self._jobs))

    @cached_property
    def n_finished(self) -> int:
        return sum(map(lambda j: j.is_finished, self._jobs))

    @cached_property
    def n_failed(self) -> int:
        return sum(map(lambda j: j.is_failed, self._jobs))

    @cached_property
    def n_in_progress(self) -> int:
        return sum(map(lambda j: not j.is_finished, self._jobs))

    @cached_property
    def last_succeeded_scan_date(self) -> str | None:
        if not self._jobs:
            return
        succeeded = next(
            (item for item in reversed(self._jobs) if item.is_succeeded), None
        )
        if not succeeded:
            return
        return succeeded.submitted_at

    @property
    def customer(self) -> str | None:
        if not self._jobs:
            return
        return self._jobs[0].customer_name

    @cached_property
    def scanned_tenants(self) -> tuple[str, ...]:
        return tuple(set(j.tenant_name for j in self._jobs))

    @cached_property
    def scanned_platforms(self) -> tuple[str, ...]:
        return tuple(
            set(j.platform_id for j in self._jobs if j.is_platform_job)
        )


T = TypeVar('T')


class ReportVisitor(ABC, Generic[T]):
    def visitDefault(self, *args, **kwargs) -> T:
        _LOG.warning('Default visitor entered, can a bug')

    @classmethod
    def derive_visitor(cls, typ: ReportType, **kwargs) -> 'ReportVisitor':
        # NOTE: you should get necessary visitor and report yourself,
        match typ:
            case ReportType.OPERATIONAL_RESOURCES:
                return ResourcesReportGenerator(**kwargs)
            case ReportType.OPERATIONAL_DEPRECATION:
                return DeprecationReportGenerator(**kwargs)
            case ReportType.OPERATIONAL_ATTACKS:
                return AttacksReportGenerator(**kwargs)
            case ReportType.OPERATIONAL_FINOPS:
                return FinopsReportGenerator(**kwargs)
            case _:
                raise NotImplementedError('')


class Report(ABC):
    """
    TODO: write something here
    """

    __slots__ = ('typ',)

    def __init__(self, typ: ReportType, /):
        self.typ = typ

    def accept(self, visitor: ReportVisitor[T], /, **kwargs) -> T:
        method = getattr(
            visitor, f'visit{self.__class__.__name__}', visitor.visitDefault
        )
        return method(self, **kwargs)

    @classmethod
    def derive_report(cls, typ: ReportType, /) -> 'Report':
        match typ:
            case ReportType.OPERATIONAL_RESOURCES:
                return ResourcesReport(typ)
            case ReportType.OPERATIONAL_DEPRECATION:
                return DeprecationReport(typ)
            case ReportType.OPERATIONAL_ATTACKS:
                return AttacksReport(typ)
            case ReportType.OPERATIONAL_FINOPS:
                return FinopsReport(typ)
            case ReportType.OPERATIONAL_KUBERNETES:
                return KubernetesReport(typ)
            case _:
                raise NotImplementedError('')


class ResourcesReport(Report):
    pass


class DeprecationReport(Report):
    pass


class FinopsReport(Report):
    pass


class AttacksReport(Report):
    pass


class KubernetesReport(Report):
    pass


class ScopedRulesSelector(ReportVisitor[set[str]]):
    __slots__ = ('_metadata',)

    def __init__(self, metadata: Metadata):
        self._metadata = metadata

    def visitDefault(
        self, report: 'Report', /, rules: Iterable[str], **kwargs
    ) -> set[str]:
        return set(rules)

    def visitResourcesReport(
        self, report: 'ResourcesReport', /, rules: Iterable[str], **kwargs
    ) -> set[str]:
        """
        Resources report contains all rules
        """
        return set(rules)

    def visitDeprecationReport(
        self, report: 'DeprecationReport', /, rules: Iterable[str], **kwargs
    ) -> set[str]:
        meta = self._metadata
        return {rule for rule in rules if meta.rule(rule).is_deprecation()}

    def visitFinopsReport(
        self, report: 'FinopsReport', /, rules: Iterable[str], **kwargs
    ) -> set[str]:
        meta = self._metadata
        return {rule for rule in rules if meta.rule(rule).is_finops()}

    def visitAttacksReport(
        self, report: 'AttacksReport', /, rules: Iterable[str], **kwargs
    ) -> set[str]:
        meta = self._metadata
        return {rule for rule in rules if meta.rule(rule).mitre}


class ResourcesReportGenerator(ReportVisitor[Generator[dict, None, None]]):
    __slots__ = ('_metadata', '_view', 'scope')

    def __init__(
        self,
        metadata: Metadata,
        view: ReportVisitor[dict],
        scope: set[str] | None = None,
    ):
        self._metadata = metadata
        self._view = view
        self.scope = scope

    def visitDefault(
        self,
        report: 'ResourcesReport',
        /,
        rule_resources: dict[str, set[CloudResource]],
        meta: dict[str, RuleMeta],
        **kwargs,
    ) -> Generator[dict, None, None]:
        for rule in rule_resources:
            if self.scope is not None and rule not in self.scope:
                continue
            rm = meta.get(rule, {})
            rm2 = self._metadata.rule(rule)
            by_region = {}
            for r in rule_resources[rule]:
                by_region.setdefault(r.location, []).append(
                    r.accept(self._view, report_fields=rm2.report_fields)
                )
            yield {
                'policy': rule,
                'resource_type': service_from_resource_type(rm['resource']),
                'description': rm.get('description') or '',
                'severity': rm2.severity.value,
                'resources': by_region,
            }

    def visitKubernetesReport(
        self,
        report: 'KubernetesReport',
        /,
        rule_resources: dict[str, set[CloudResource]],
        meta: dict[str, RuleMeta],
        **kwargs,
    ) -> Generator[dict, None, None]:
        for res in self.visitDefault(report, rule_resources, meta, **kwargs):
            res['resources'] = list(
                chain.from_iterable(res['resources'].values())
            )
            yield res


class DeprecationReportGenerator(ReportVisitor[Generator[dict, None, None]]):
    """
    Defines methods to generate some operational reports payloads
    """

    __slots__ = ('_metadata', '_view', 'scope')

    def __init__(
        self,
        metadata: Metadata,
        view: ReportVisitor[dict],
        scope: set[str] | None = None,
    ):
        self._metadata = metadata
        self._view = view
        self.scope = scope

    def visitDefault(
        self,
        report: 'DeprecationReport',
        /,
        rule_resources: dict[str, set[CloudResource]],
        meta: dict[str, RuleMeta],
        **kwargs,
    ) -> Generator[dict, None, None]:
        for rule in rule_resources:
            if self.scope is not None and rule not in self.scope:
                continue
            rm = self._metadata.rule(rule)
            category = rm.deprecation_category()
            if not category:
                continue  # not a deprecation rule
            by_region = {}
            for r in rule_resources[rule]:
                by_region.setdefault(r.location, []).append(
                    r.accept(self._view, report_fields=rm.report_fields)
                )

            yield {
                'category': category,
                'deprecation_date': rm.deprecation.date.isoformat()
                if isinstance(rm.deprecation.date, date)
                else None,
                'is_deprecated': rm.deprecation.is_deprecated,
                'deprecation_severity': rm.deprecation.severity.value,
                'deprecation_link': rm.deprecation.link,
                'remediation_complexity': rm.remediation_complexity.value,
                'remediation': rm.remediation,
                'policy': rule,
                'description': meta[rule].get('description', ''),
                'resource_type': service_from_resource_type(
                    meta[rule]['resource']
                ),
                'resources': by_region,
            }


class FinopsReportGenerator(ReportVisitor[Generator[dict, None, None]]):
    """
    Defines methods to generate some operational reports payloads
    """

    __slots__ = ('_metadata', '_view', 'scope')

    def __init__(
        self,
        metadata: Metadata,
        view: ReportVisitor[dict],
        scope: set[str] | None = None,
    ):
        self._metadata = metadata
        self._view = view
        self.scope = scope

    def visitDefault(
        self,
        report: 'FinopsReport',
        /,
        rule_resources: dict[str, set[CloudResource]],
        meta: dict[str, RuleMeta],
        **kwargs,
    ) -> Generator[dict, None, None]:
        mapping = {}
        for rule in rule_resources:
            if self.scope is not None and rule not in self.scope:
                continue
            rm = self._metadata.rule(rule)
            finops_category = rm.finops_category()
            if not finops_category:
                continue
            ss = rm.service_section
            if not ss:
                _LOG.warning(f'Rule {rule} does not have service section')
                continue
            by_region = {}
            for r in rule_resources[rule]:
                by_region.setdefault(r.location, []).append(
                    r.accept(self._view, report_fields=rm.report_fields)
                )
            mapping.setdefault(ss, []).append(
                {
                    'rule': meta[rule].get('description', rule),
                    'service': rm.service
                    or service_from_resource_type(meta[rule]['resource']),
                    'category': finops_category,
                    'severity': rm.severity.value,
                    'resource_type': service_from_resource_type(
                        meta[rule]['resource']
                    ),
                    'resources': by_region,
                }
            )
        for ss, data in mapping.items():
            yield {'service_section': ss, 'rules_data': data}


class AttacksReportGenerator(ReportVisitor[Generator[dict, None, None]]):
    """
    Defines methods to generate some operational reports payloads
    """

    __slots__ = ('_metadata', '_view', 'scope')

    def __init__(
        self,
        metadata: Metadata,
        view: ReportVisitor[dict],
        scope: set[str] | None = None,
    ):
        self._metadata = metadata
        self._view = view
        self.scope = scope

    def visitDefault(
        self,
        report: 'AttacksReport',
        /,
        rule_resources: dict[str, set[CloudResource]],
        meta: dict[str, RuleMeta],
        **kwargs,
    ) -> Generator[dict, None, None]:
        """
        Yields a unique attack, a target resource and rules that can cause
        such an attack:
        region, service, resource_type, resource, dict[MitreAttack, list[str]]
        """

        unique_resource_to_attack_rules = {}
        for rule in rule_resources:
            if self.scope is not None and rule not in self.scope:
                continue
            rm = self._metadata.rule(rule)
            s = rm.service or service_from_resource_type(meta[rule]['resource'])
            rule_attacks = tuple(rm.iter_mitre_attacks())
            if not rule_attacks:
                _LOG.warning(f'No attacks found for rule: {rule}')
                continue
            for res in rule_resources[rule]:
                _, attacks = unique_resource_to_attack_rules.setdefault(
                    res, (s, {})
                )
                for attack in rule_attacks:
                    attacks.setdefault(attack, []).append(rule)
        for res, (s, attacks) in unique_resource_to_attack_rules.items():
            at = []
            report_fields = ()
            for attack, rules in attacks.items():
                inner = attack.to_dict()
                inner['severity'] = sorted(
                    (self._metadata.rule(r).severity.value for r in rules),
                    key=cmp_to_key(SeverityCmp()),
                )[-1]
                inner['violations'] = [
                    {
                        'description': meta[rule]['description'],
                        'remediation': self._metadata.rule(rule).remediation,
                        'remediation_complexity': self._metadata.rule(
                            rule
                        ).remediation_complexity.value,
                        'severity': self._metadata.rule(rule).severity.value,
                    }
                    for rule in rules
                ]
                at.append(inner)
                if not report_fields:
                    report_fields = self._metadata.rule(rules[0]).report_fields
            yield {
                'region': res.region,
                'service': s,
                'resource_type': res.resource_type,
                'resource': res.accept(self._view, report_fields=report_fields),
                'attacks': at,
            }


# TODO: remove
class ShardsCollectionDataSource:
    def __init__(
        self,
        collection: ShardsCollection,
        metadata: Metadata,
        cloud: Cloud,
        account_id: str = '',
    ):
        self._col = collection
        self._meta = metadata
        self._cloud = cloud
        self._aid = account_id

        self._rule_resources = None

    @property
    def _resources(self) -> dict[str, set[CloudResource]]:
        if self._rule_resources is not None:
            return self._rule_resources
        it = iter_rule_resources(
            collection=self._col,
            cloud=self._cloud,
            metadata=self._meta,
            account_id=self._aid,
        )
        dct = {}
        for k, v in it:
            resources = set(v)
            if not resources:
                continue
            dct[k] = resources
        self._rule_resources = dct
        # NOTE: Generally we should not expect duplicated resources within one
        #  rule. Something is definitely wrong if one rule returns multiple
        #  equal resources within one region. If one rule returns multiple
        #  equal resources within different regions that rule must be global.
        #  But, we perform some custom processing of resources which involves
        #  changing their regions. For example, the same multi-regional
        #  CloudTrail can be returns multiple times by executing the same rule
        #  against different regions. So, if we encounter a multi-regional
        #  trail during processing we manually change ist region to 'global'.
        #  So, there is a real point here to de-duplicate resources
        #  WITHIN ONE rule here.
        return self._rule_resources

    def clear(self):
        self._rule_resources = None

    @cached_property
    def n_unique(self) -> int:
        return len(set(chain.from_iterable(self._resources.values())))

    @cached_property
    def n_findings(self) -> int:
        n = 0
        for resources in self._resources.values():
            n += len(resources)
        return n

    def region_severities(
        self, unique: bool = True
    ) -> dict[str, dict[str, int]]:
        """
        Returns something like this:
        {
            "eu-central-1": {
                "High": 123,
                "Medium": 42
            },
            "eu-west-1": {
                "High": 123,
                "Medium": 42
            },
        }
        unique == True
        In case there is a resource which violates different rules with
        different severity, it will be added to the highest severity
        number.
        unique == False
        In case there is a resource which violates different rules with
        different severity, it will be to both severities. So, the total
        number of unique resources and sum of resources by severities
        can clash
        """
        region_severity = {}
        for rule in self._resources:
            sev = self._meta.rule(rule).severity.value
            for res in self._resources[rule]:
                _inner = region_severity.setdefault(res.region, {})
                _inner.setdefault(sev, set()).add(res)
        if unique:
            for region, data in region_severity.items():
                keep_highest(
                    *[
                        data.get(k)
                        for k in sorted(
                            data.keys(), key=cmp_to_key(SeverityCmp())
                        )
                    ]
                )
        result = {}
        for region, data in region_severity.items():
            for severity, resources in data.items():
                if not resources:
                    continue
                d = result.setdefault(region, {})
                d.setdefault(severity, 0)
                d[severity] += len(resources)
        return result

    def severities(self) -> dict[str, int]:
        res = {}
        for severities in self.region_severities(unique=True).values():
            for name, n in severities.items():
                res.setdefault(name, 0)
                res[name] += n
        return res

    def region_resource_types(self) -> dict[str, dict[str, int]]:
        region_resource = {}
        for rule in self._resources:
            rt = service_from_resource_type(self._col.meta[rule]['resource'])
            for res in self._resources[rule]:
                _inner = region_resource.setdefault(res.region, {})
                _inner.setdefault(rt, set()).add(res)
        result = {}
        for region, data in region_resource.items():
            for rt, resources in data.items():
                d = result.setdefault(region, {})
                d.setdefault(rt, 0)
                d[rt] += len(resources)
        return result

    def resource_types(self) -> dict[str, int]:
        res = {}
        for rt in self.region_resource_types().values():
            for name, n in rt.items():
                res.setdefault(name, 0)
                res[name] += n
        return res

    def region_services(self) -> dict[str, dict[str, int]]:
        region_service = {}
        for rule in self._resources:
            ser = self._meta.rule(rule).service or service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            for res in self._resources[rule]:
                _inner = region_service.setdefault(res.region, {})
                _inner.setdefault(ser, set()).add(res)

        result = {}
        for region, data in region_service.items():
            for ser, resources in data.items():
                d = result.setdefault(region, {})
                d.setdefault(ser, 0)
                d[ser] += len(resources)
        return result

    def services(self) -> dict[str, int]:
        res = {}
        for services in self.region_services().values():
            for name, n in services.items():
                res.setdefault(name, 0)
                res[name] += n
        return res

    def tactic_to_severities(self) -> dict[str, dict[str, int]]:
        """ """
        # not sure about its correctness because this kind of data mapping
        # is very gnarly
        tactic_severity = {}
        for rule in self._resources:
            rm = self._meta.rule(rule)
            sev = rm.severity.value
            for tactic in rm.mitre:
                tactic_severity.setdefault(tactic, {}).setdefault(
                    sev, set()
                ).update(self._resources[rule])
        for tactic, data in tactic_severity.items():
            keep_highest(
                *[
                    data.get(k)
                    for k in sorted(data.keys(), key=cmp_to_key(SeverityCmp()))
                ]
            )
        result = {}
        for tactic, data in tactic_severity.items():
            for severity, resources in data.items():
                if not resources:
                    continue
                d = result.setdefault(tactic, {})
                d.setdefault(severity, 0)
                d[severity] += len(resources)
        return result


class ShardsCollectionProvider:
    """
    Caches collections for tenant and date
    """

    __slots__ = '_rs', '_threshold', '_cache'

    def __init__(
        self, report_service: ReportService, threshold_seconds: int = 86400
    ):
        self._rs = report_service
        # TODO: adjust the threshold and sync with shards snapshots
        self._threshold = threshold_seconds
        self._cache = {}

    def clear(self):
        self._cache.clear()

    def _is_latest(self, date: datetime) -> bool:
        now = utc_datetime()
        assert now >= date, 'Cannot possibly request future data'
        return (now - date).seconds <= self._threshold

    def get_for_tenant(
        self, tenant: Tenant, date: datetime
    ) -> ShardsCollection | None:
        """
        Returns already fetched collection
        """
        is_latest = self._is_latest(date)
        if is_latest:
            key = (tenant.name, None)
        else:
            key = (tenant.name, date)

        if key in self._cache:
            return self._cache[key]

        if is_latest:
            _LOG.debug(f'Going to fetch latest collection for tenant {tenant.name}')
            col = self._rs.tenant_latest_collection(tenant)
        else:
            _LOG.debug(f'Going to fetch snapshot collection for tenant {tenant.name} and date {date}')
            # TODO: cache for actual nearest key instead of given date in case
            #  we can have slightly different incoming "date". If the parameter
            #  most likely to be the same, we better keep it as is.
            #  If we choose to cache using the nearest key we will still need
            #  to make one s3:list_objects call each time this method is called
            col = self._rs.tenant_snapshot_collection(tenant, date)

        if col is None:
            return

        col.fetch_all()
        col.fetch_meta()
        self._cache[key] = col
        return col

    def get_for_platform(
        self, platform: Platform, date: datetime
    ) -> ShardsCollection | None:
        is_latest = self._is_latest(date)
        if is_latest:
            key = (platform.id, None)
        else:
            key = (platform.id, date)
        if key in self._cache:
            return self._cache[key]
        if is_latest:
            col = self._rs.platform_latest_collection(platform)
        else:
            col = self._rs.platform_snapshot_collection(platform, date)
        if col is None:
            return
        col.fetch_all()
        col.fetch_meta()
        self._cache[key] = col
        return col


MT = TypeVar('MT', bound=type)


class ReportMetricsService(BaseDataService[ReportMetrics]):
    payload_size_threshold = 1 << 20
    enc = msgspec.msgpack.Encoder()

    def __init__(self, s3_client: S3Client):
        super().__init__()
        self._s3 = s3_client

    def create(
        self,
        key: str,
        end: datetime,
        start: datetime | None = None,
        tenants: Iterable[str] = (),
        created_at: datetime | None = None,
    ) -> ReportMetrics:
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        customer = key.split(COMPOUND_KEYS_SEPARATOR, 2)[1]
        return ReportMetrics(
            key=key,
            end=utc_iso(end),
            start=utc_iso(start) if start else None,
            customer=customer,
            tenants=list(tenants),
            _created_at=utc_iso(created_at) if created_at else None,
        )

    def query(
        self,
        key: str,
        since: datetime | None = None,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
        rate_limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        """
        Queries reports inside the given time scope
        """
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        rkc = None
        if since:
            rkc &= ReportMetrics.start >= utc_iso(since)
        if till:
            rkc &= ReportMetrics.end < utc_iso(till)
        return self.model_class.query(
            hash_key=key,
            range_key_condition=rkc,
            scan_index_forward=ascending,
            limit=limit,
            rate_limit=rate_limit,
        )

    def query_exactly(
        self,
        key: str,
        start: datetime | None = None,
        end: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
        rate_limit: int | None = None,
        attributes_to_get: tuple | None = None,
    ) -> Iterator[ReportMetrics]:
        """
        Queries reports with exactly matching time
        """
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        rkc = None
        if start:
            rkc &= ReportMetrics.start == utc_iso(start)
        if end:
            rkc &= ReportMetrics.end == utc_iso(end)
        return self.model_class.query(
            hash_key=key,
            range_key_condition=rkc,
            scan_index_forward=ascending,
            limit=limit,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get,
        )

    def query_by_tenant(
        self,
        tenant: Tenant,
        type_: ReportType,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        return self.query(
            key=ReportMetrics.build_key_for_tenant(type_, tenant),
            till=till,
            ascending=ascending,
            limit=limit,
        )
    
    def query_all_by_customer(
        self,
        customer: Customer | str,
        since: datetime | None = None,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
        rate_limit: int | None = None,
        attributes_to_get: list | None = None,
    ) -> Iterator[ReportMetrics]:
        key = customer.name if isinstance(customer, Customer) else customer
        rkc = None
        if since:
            rkc &= ReportMetrics.start >= utc_iso(since)
        if till:
            rkc &= ReportMetrics.end < utc_iso(till)
        return self.model_class.customer_end_index.query(
            hash_key=key,
            range_key_condition=rkc,
            scan_index_forward=ascending,
            limit=limit,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get
        )

    def query_by_platform(
        self,
        platform: Platform,
        type_: ReportType,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        return self.query(
            key=ReportMetrics.build_key_for_platform(type_, platform),
            till=till,
            ascending=ascending,
            limit=limit,
        )

    def get_latest_for_tenant(
        self, tenant: Tenant, type_: ReportType, till: datetime | None = None
    ) -> ReportMetrics | None:
        return next(
            self.query_by_tenant(
                tenant=tenant, type_=type_, till=till, ascending=False, limit=1
            ),
            None,
        )

    def get_latest_for_platform(
        self,
        platform: Platform,
        type_: ReportType,
        till: datetime | None = None,
    ) -> ReportMetrics | None:
        return next(
            self.query_by_platform(
                platform=platform,
                type_=type_,
                till=till,
                ascending=False,
                limit=1,
            ),
            None,
        )

    def get_exactly_for_customer(
        self,
        customer: Customer | str,
        type_: ReportType,
        start: datetime | None = None,
        end: datetime | None = None,
        attributes_to_get: tuple | None = None,
    ) -> ReportMetrics | None:
        name = customer.name if isinstance(customer, Customer) else customer
        return next(
            self.query_exactly(
                key=ReportMetrics.build_key_for_customer(type_, name),
                start=start,
                end=end,
                ascending=False,
                limit=1,
                attributes_to_get=attributes_to_get,
            ),
            None,
        )

    def get_latest_for_project(
        self,
        customer: str,
        project: str,
        type_: ReportType,
        till: datetime | None = None,
    ) -> ReportMetrics | None:
        return next(
            self.query(
                key=ReportMetrics.build_key_for_project(
                    type_, customer, project
                ),
                till=till,
                ascending=False,
                limit=1,
            ),
            None,
        )

    def was_collected_for_customer(
        self, customer: Customer | str, type_: ReportType, now: datetime
    ) -> bool:
        name = customer.name if isinstance(customer, Customer) else customer
        return bool(
            next(
                self.query_exactly(
                    key=ReportMetrics.build_key_for_customer(type_, name),
                    start=type_.start(now),
                    end=type_.end(now),
                    ascending=False,
                    limit=1,
                    attributes_to_get=(ReportMetrics.key,),
                ),
                None,
            )
        )

    def save(self, item: ReportMetrics, data: Any) -> None:
        # NOTE: data will always be something
        self.set_compressed_data(
            item=item,
            data=self.enc.encode(data),
            content_type='application/vnd.msgpack',
        )
        return super().save(item)

    def fetch_data(self, item: ReportMetrics, typ: MT = Any) -> MT:
        return msgspec.msgpack.decode(self.get_compressed_data(item), type=typ)

    def set_data(
        self,
        item: ReportMetrics,
        buf: io.BytesIO,
        content_type: str,
        content_encoding: str,
        length: int | None = None,
    ):
        if length is None:
            length = len(buf.getvalue())
        if length > self.payload_size_threshold:
            _LOG.info('Data exceeds threshold. Storing to S3')
            buf.seek(0)
            bucket = CAASEnv.REPORTS_BUCKET_NAME.as_str()
            key = ReportMetricsBucketKeysBuilder.metrics_key(item)
            self._s3.put_object(
                bucket=bucket,
                key=key,
                body=buf,
                content_type=content_type,
                content_encoding=content_encoding,
            )
            item.s3_url = str(S3Url.build(bucket, key))
        else:
            _LOG.info(
                'Compressed data size is within limits. Setting as DynamoDB Binary attribute'
            )
            item.data = buf.getvalue()
        item.content_type = content_type
        item.content_encoding = content_encoding

    def get_data(self, item: ReportMetrics) -> io.BytesIO | None:
        if item.s3_url:
            _LOG.info(f'Going to get item {item} data from s3')
            url = S3Url(item.s3_url)
            buf = self._s3.get_object(bucket=url.bucket, key=url.key)
            if not buf:
                return
            buf = cast(io.BytesIO, buf)
        else:
            d = item.data
            if not d:
                return
            buf = io.BytesIO(d)
        return buf

    def set_compressed_data(
        self, item: ReportMetrics, data: bytes, content_type: str
    ) -> None:
        """
        Changes the given data attribute and may write to s3
        """
        len_orig = len(data)
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as f:
            f.write(data)
        len_compressed = buf.tell()
        _log = _LOG.info if len_orig >= len_compressed else _LOG.warning
        _log(
            f'Original data size: {len_orig}. Compressed data size: {len_compressed}'
        )

        self.set_data(
            item=item,
            buf=buf,
            length=len_compressed,
            content_type=content_type,
            content_encoding='gzip',
        )

    def get_compressed_data(self, item: ReportMetrics) -> bytes | None:
        buf = self.get_data(item)
        if not buf:
            return
        with gzip.GzipFile(fileobj=buf, mode='rb') as f:
            return f.read()


def _default_diff_callback(key, new, old) -> dict:
    res = {'value': new}
    if isinstance(old, (int, float)) and not isinstance(old, bool):
        res['diff'] = new - old
    return res


def add_diff(
    current: dict,
    previous: dict,
    exclude: tuple[str | tuple[str, ...], ...] = (),
    callback: Callable[[str, Any, Any], Any] = _default_diff_callback,
) -> None:
    """
    Replaces numbers inside the current dict with {"diff": int, "value": int}.
    "diff" can be None in case there is not data for previous period
    Changes the first item in-place.
    """
    to_exclude = {i if isinstance(i, tuple) else (i,) for i in exclude}
    gen = iter_key_values(
        finding=current,
        hook=lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
    )
    try:
        keys, real = next(gen)
        while True:
            if keys in to_exclude:
                new = real
            else:
                new = callback(keys[-1], real, deep_get(previous, keys))
            keys, real = gen.send(new)
    except StopIteration:
        pass

    # TODO: we can possibly have values that exist in previous dict and
    #  do not exist in the current one. They are ignored for now
