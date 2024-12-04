import bisect
from datetime import datetime
from functools import cached_property, cmp_to_key
from itertools import chain
from typing import Generator, Iterable, Iterator, TypedDict, cast

from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers import filter_dict, hashable, iter_key_values, deep_get
from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    GLOBAL_REGION,
    REPORT_FIELDS,
    JobState,
    ReportType,
)
from helpers.log_helper import get_logger
from helpers.reports import Severity, SeverityCmp, keep_highest
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services.ambiguous_job_service import AmbiguousJob
from services.reports_bucket import ReportMetricsBucketKeysBuilder
from services.base_data_service import BaseDataService
from services.mappings_collector import (
    LazyLoadedMappingsCollector,
    MappingsCollector,
)
from services.report_service import ReportService
from services.sharding import ShardsCollection
from services.clients.s3 import S3Client, S3Url
from services.environment_service import EnvironmentService

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
    def n_failed(self) -> int:
        return sum(map(lambda j: j.is_failed, self._jobs))

    @cached_property
    def n_in_progress(self) -> int:
        return sum(map(lambda j: not j.is_finished, self._jobs))

    @property
    def last_scan_date(self) -> str | None:
        if not self._jobs:
            return
        return self._jobs[-1].submitted_at

    @property
    def customer(self) -> str | None:
        if not self._jobs:
            return
        return self._jobs[0].customer_name

    @cached_property
    def scanned_tenants(self) -> tuple[str, ...]:
        return tuple(set(j.tenant_name for j in self._jobs))


class ShardsCollectionDataSource:
    ResourcesGenerator = Generator[tuple[str, str, dict, float], None, None]

    class PrettifiedFinding(TypedDict):
        policy: str
        resource_type: str
        description: str
        severity: str
        resources: dict[str, list[dict]]

    def __init__(
        self,
        collection: ShardsCollection,
        mappings_collector: LazyLoadedMappingsCollector
        | MappingsCollector
        | None = None,
    ):
        """
        Assuming that collection is pulled and has meta
        """
        self._col = collection
        if mappings_collector:
            self._services = mappings_collector.service
            self._severities = mappings_collector.severity
            self._hd = mappings_collector.human_data
        else:
            self._services = {}
            self._severities = {}
            self._hd = {}

        self.__resources = None

    @staticmethod
    def adjust_resource_type(rt: str) -> str:
        """
        Removes cloud prefix from resource type
        """
        # TODO: replace usages with one from reports.py
        return rt.split('.', maxsplit=1)[-1]

    def _get_rule_service(self, rule: str) -> str:
        """
        Builds rule service from Cloud Custodian resource type. Last resort
        """
        if serv := self._services.get(rule):
            return serv
        rt = self.adjust_resource_type(self._col.meta[rule]['resource'])
        return rt.replace('-', ' ').replace('_', ' ').title()

    def iter_resources(self):
        for part in self._col.iter_parts():
            for res in part.resources:
                yield part.policy, part.location, res, part.timestamp

    @staticmethod
    def _custom_attr(name: str) -> str:
        """
        Adds prefix to the attribute name to mark that it's custom
        :param name:
        :return:
        """
        return f'sre:{name}'

    @staticmethod
    def _is_custom_attr(name: str) -> bool:
        return name.startswith('sre:')

    @staticmethod
    def _allow_only_regions(
        it: ResourcesGenerator, regions: set[str] | tuple[str, ...]
    ) -> ResourcesGenerator:
        for rule, region, dto, ts in it:
            if region in regions:
                yield rule, region, dto, ts

    def _allow_only_resource_type(
        self,
        it: ResourcesGenerator,
        meta: dict,
        resource_type: tuple[str, ...],
    ) -> ResourcesGenerator:
        to_check = tuple(map(self.adjust_resource_type, resource_type))
        for rule, region, dto, ts in it:
            rt = self.adjust_resource_type(meta.get(rule, {}).get('resource'))
            if rt in to_check:
                yield rule, region, dto, ts

    @staticmethod
    def _deduplicated(it: ResourcesGenerator) -> ResourcesGenerator:
        """
        This generator goes through resources and yields only unique ones
        within rule and region
        :param it:
        :return:
        """
        emitted = {}
        for rule, region, dto, ts in it:
            _emitted = emitted.setdefault((rule, region), set())
            _hashable = hashable(dto)
            if _hashable in _emitted:
                _LOG.debug(f'Duplicate found for {rule}:{region}')
                continue
            yield rule, region, dto, ts
            _emitted.add(_hashable)

    def _custom_modify(self, it: ResourcesGenerator) -> ResourcesGenerator:
        """
        Some resources require special treatment.
        - rules with resource type "aws.cloudtrail" are not multiregional,
        but the resource they look for can be either multiregional or not.
        So we must deduplicate them on the scope of whole account.
        252 and other glue-catalog rules are not multiregional, but they
        also do not return unique information within region.
        :param it:
        :param meta:
        :return:
        """
        # TODO: in case we need more business logic here, redesign this
        #  solution. Maybe move this logic to a separate class
        meta = self._col.meta
        for rule, region, dto, ts in it:
            rt = meta.get(rule, {}).get('resource')
            rt = self.adjust_resource_type(rt)
            if rt in ('glue-catalog', 'account'):
                _LOG.debug(
                    f'Rule with type {rt} found. Adding region '
                    f'attribute to make its dto differ from '
                    f'other regions'
                )
                dto[self._custom_attr('region')] = region
            elif rt == 'cloudtrail':
                if dto.get('IsMultiRegionTrail'):
                    _LOG.debug(
                        'Found multiregional trail. '
                        'Moving it to multiregional region'
                    )
                    region = GLOBAL_REGION
            yield rule, region, dto, ts

    def _report_fields(self, rule: str) -> set[str]:
        rf = set(self._hd.get(rule, {}).get('report_fields') or [])
        return rf | REPORT_FIELDS

    def _keep_report_fields(
        self, it: ResourcesGenerator
    ) -> ResourcesGenerator:
        """
        Keeps only report fields for each resource. Custom attributes are
        not removed because they are added purposefully
        :param it:
        :return:
        """
        for rule, region, dto, ts in it:
            filtered = filter_dict(dto, self._report_fields(rule))
            filtered.update(
                {k: v for k, v in dto.items() if self._is_custom_attr(k)}
            )
            yield rule, region, filtered, ts

    def create_resources_generator(
        self,
        only_report_fields: bool = True,
        deduplicated: bool = True,
        active_regions: tuple[str, ...] = (),
        resource_types: tuple[str, ...] = (),
    ) -> ResourcesGenerator:
        it = self.iter_resources()
        it = self._custom_modify(it)

        if only_report_fields:
            it = self._keep_report_fields(it)
        if deduplicated:
            it = self._deduplicated(it)

        if active_regions:
            it = self._allow_only_regions(it, (GLOBAL_REGION, *active_regions))
        if resource_types:
            it = self._allow_only_resource_type(
                it, self._col.meta, resource_types
            )
        return it

    @property
    def _resources(self) -> dict[str, dict[str, set]]:
        """
        Caching for private usage
        """
        if self.__resources is not None:
            return self.__resources
        # TODO: test how bad-performant is that..
        it = self.create_resources_generator()
        mapping = {}
        for rule, region, dto, _ in it:
            mapping.setdefault(rule, {}).setdefault(region, set()).add(
                hashable(dto)
            )
        self.__resources = mapping
        return mapping

    def clear(self):
        self.__resources = None

    @cached_property
    def n_unique(self) -> int:
        return len(
            set(
                chain.from_iterable(
                    chain.from_iterable(d.values())
                    for d in self._resources.values()
                )
            )
        )

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
        In case where is a resource which violates different rules with
        different severity, it will be to both severities. So, the total
        number of unique resources and sum of resources by severities
        can clash
        """
        region_severity = {}
        for rule in self._resources:
            severity = Severity.parse(self._severities.get(rule)).value
            for region, res in self._resources[rule].items():
                region_severity.setdefault(region, {}).setdefault(
                    severity, set()
                ).update(res)
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

    def resources(self) -> list[PrettifiedFinding]:
        result = []
        service = self._services
        severity = self._severities
        meta = self._col.meta
        for rule in self._resources:
            rm = meta.get(rule, {})
            item = {
                'policy': rule,
                'resource_type': service.get(rule)
                or self._get_rule_service(rule),
                'description': rm.get('description') or '',
                'severity': Severity.parse(severity.get(rule)).value,
                'resources': {
                    region: list(res)
                    for region, res in self._resources[rule].items()
                },
            }
            result.append(item)
        return result

    def resource_types(self) -> dict[str, int]:
        result = {}
        service = self._services
        for rule in self._resources:
            rt = service.get(rule) or self._get_rule_service(rule)
            result.setdefault(rt, 0)
            for res in self._resources[rule].values():
                result[rt] += len(res)
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
            col = self._rs.tenant_latest_collection(tenant)
        else:
            # TODO: cache for actual nearest key instead of given date in case
            #  we can have slightly different incoming "date". If the parameter
            #  most likely to be the same, we better keep it as is.
            col = self._rs.tenant_snapshot_collection(tenant, date)

        if col is None:
            return

        col.fetch_all()
        col.fetch_meta()
        self._cache[key] = col
        return col


class ReportMetricsService(BaseDataService[ReportMetrics]):
    def __init__(
        self, s3_client: S3Client, environment_service: EnvironmentService
    ):
        super().__init__()

        self._s3 = s3_client
        self._env = environment_service

    @staticmethod
    def build_key(
        type_: ReportType,
        customer: str,
        project: str = '',
        cloud: str = '',
        tenant: str = '',
        region: str = '',
    ) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(
            (type_.value, customer, project, cloud, tenant, region)
        )

    @classmethod
    def key_for_tenant(
        cls, type_: ReportType, tenant: Tenant, region: str = ''
    ) -> str:
        """
        For tenant bound reports
        """
        return cls.build_key(
            type_=type_,
            customer=tenant.customer_name,
            project=tenant.display_name_to_lower.lower(),  # TODO: maybe use lt attr
            cloud=tenant.cloud,
            tenant=tenant.name,
            region=region,
        )

    @classmethod
    def key_for_project(
        cls, type_: ReportType, customer: str, project: str, cloud: str = ''
    ) -> str:
        """
        For project reports
        """
        return cls.build_key(
            type_=type_, customer=customer, project=project, cloud=cloud
        )

    @classmethod
    def key_for_customer(cls, type_: ReportType, customer: str) -> str:
        """
        For c-level reports
        """
        return cls.build_key(type_=type_, customer=customer)

    def create(
        self,
        key: str,
        data: dict,
        end: datetime,
        start: datetime | None = None,
    ) -> ReportMetrics:
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        customer = key.split(COMPOUND_KEYS_SEPARATOR, 2)[1]
        return ReportMetrics(
            key=key,
            end=utc_iso(end),
            start=utc_iso(start) if start else None,
            data=data,
            customer=customer,
        )

    def query(
        self,
        key: str,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
        rate_limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        rkc = None
        if till:
            rkc = ReportMetrics.end <= utc_iso(till)
        return self.model_class.query(
            hash_key=key,
            range_key_condition=rkc,
            scan_index_forward=ascending,
            limit=limit,
            rate_limit=rate_limit,
        )

    def query_by_tenant(
        self,
        tenant: Tenant,
        type_: ReportType,
        till: datetime | None = None,
        region: str = '',
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        return self.query(
            key=self.key_for_tenant(type_, tenant, region),
            till=till,
            ascending=ascending,
            limit=limit,
        )

    def query_by_customer(
        self,
        customer: Customer | str,
        type_: ReportType,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        name = customer.name if isinstance(customer, Customer) else customer
        return self.query(
            key=self.key_for_customer(type_, name),
            till=till,
            ascending=ascending,
            limit=limit,
        )

    def get_latest_for_tenant(
        self,
        tenant: Tenant,
        type_: ReportType,
        till: datetime | None = None,
        region: str = '',
    ) -> ReportMetrics | None:
        return next(
            self.query_by_tenant(
                tenant=tenant,
                type_=type_,
                till=till,
                region=region,
                ascending=False,
                limit=1,
            ),
            None,
        )

    def get_latest_for_customer(
        self,
        customer: Customer | str,
        type_: ReportType,
        till: datetime | None = None,
    ) -> ReportMetrics | None:
        return next(
            self.query_by_customer(
                customer=customer,
                type_=type_,
                till=till,
                ascending=False,
                limit=1,
            ),
            None,
        )

    def save_data_to_s3(self, item: ReportMetrics) -> None:
        """
        These items are assumed to be immutable - generated only once. So
        currently no need to handle key overrides and remove old junk
        because they should not happen
        """
        if not item.data:
            _LOG.warning(
                f'{item.key} has empty data so will not be saved to s3'
            )
            return
        bucket = self._env.default_reports_bucket_name()
        key = ReportMetricsBucketKeysBuilder.metrics_key(item)
        self._s3.gz_put_json(bucket=bucket, key=key, obj=item.data.as_dict())
        item.data = {}
        item.s3_url = str(S3Url.build(bucket, key))

    def fetch_data_from_s3(self, item: ReportMetrics) -> None:
        if item.is_fetched:
            return
        if not item.s3_url:
            return
        url = S3Url(item.s3_url)
        item.data = cast(
            dict, self._s3.gz_get_json(bucket=url.bucket, key=url.key)
        )


def add_diff(
    current: dict,
    previous: dict,
    exclude: tuple[str | tuple[str, ...], ...] = (),
) -> None:
    """
    Replaces numbers inside the current dict with {"diff": int, "value": int}.
    "diff" can be None in case there is not data for previous period
    Changes the first item in-place.
    """
    to_exclude = {i if isinstance(i, tuple) else (i,) for i in exclude}
    gen = iter_key_values(
        finding=current,
        hook=lambda x: isinstance(x, (int, float)) and not isinstance(x, bool)
    )
    try:
        keys, real = next(gen)
        while True:
            if keys in to_exclude:
                new = real
            else:
                new = {'value': real}
                old = deep_get(previous, keys)
                if isinstance(old, (int, float)) and not isinstance(old, bool):
                    new['diff'] = real - old
            keys, real = gen.send(new)
    except StopIteration:
        pass

    # TODO: we can possibly have values that exist in previous dict and
    #  do not exist in the current one. They are ignored for now
