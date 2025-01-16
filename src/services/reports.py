import bisect
from datetime import datetime
from functools import cached_property, cmp_to_key
from itertools import chain
from typing import Generator, Iterable, Iterator, Literal, cast

from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers import deep_get, filter_dict, hashable, iter_key_values
from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    GLOBAL_REGION,
    REPORT_FIELDS,
    TACTICS_ID_MAPPING,
    Cloud,
    JobState,
    ReportType,
)
from helpers.log_helper import get_logger
from helpers.reports import (
    SeverityCmp,
    adjust_resource_type,
    keep_highest,
    service_from_resource_type,
)
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services.ambiguous_job_service import AmbiguousJob
from services.base_data_service import BaseDataService
from services.clients.s3 import S3Client, S3Url
from services.environment_service import EnvironmentService
from services.metadata import Metadata
from services.platform_service import Platform
from services.report_service import ReportService
from services.reports_bucket import ReportMetricsBucketKeysBuilder
from services.sharding import ShardsCollection

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

    @cached_property
    def scanned_platforms(self) -> tuple[str, ...]:
        return tuple(
            set(j.platform_id for j in self._jobs if j.is_platform_job)
        )


class ShardsCollectionDataSource:
    ResourcesGenerator = Generator[tuple[str, str, dict, float], None, None]

    def __init__(self, collection: ShardsCollection, metadata: Metadata):
        """
        Assuming that collection is pulled and has meta
        """
        self._col = collection
        self._meta = metadata

        self.__resources = None

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

    @staticmethod
    def _allow_only_resource_type(
        it: ResourcesGenerator, meta: dict, resource_type: tuple[str, ...]
    ) -> ResourcesGenerator:
        to_check = tuple(map(adjust_resource_type, resource_type))
        for rule, region, dto, ts in it:
            rt = adjust_resource_type(meta.get(rule, {})['resource'])
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
            rt = meta.get(rule, {})['resource']
            rt = adjust_resource_type(rt)
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
        return set(self._meta.rule(rule).report_fields) | REPORT_FIELDS

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
            severity = self._meta.rule(rule).severity
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

    def resources(self) -> Generator[dict, None, None]:
        meta = self._col.meta
        for rule in self._resources:
            rm = meta.get(rule, {})
            yield {
                'policy': rule,
                'resource_type': self._meta.rule(rule).service
                or service_from_resource_type(
                    self._col.meta[rule]['resource']
                ),
                'description': rm.get('description') or '',
                'severity': self._meta.rule(rule).severity.value,
                'resources': {
                    region: list(res)
                    for region, res in self._resources[rule].items()
                },
            }

    def resources_no_regions(self) -> Generator[dict, None, None]:
        for res in self.resources():
            res['resources'] = list(
                chain.from_iterable(res['resources'].values())
            )
            yield res

    def resource_types(self) -> dict[str, int]:
        result = {}
        for rule in self._resources:
            rt = self._meta.rule(rule).service or service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            result.setdefault(rt, 0)
            for res in self._resources[rule].values():
                result[rt] += len(res)
        return result

    def finops(self) -> dict[str, list[dict]]:
        """
        Produces finops data in its old format
        """
        res = {}
        for rule in self._resources:
            rule_meta = self._meta.rule(rule)
            finops_category = rule_meta.finops_category()
            if not finops_category:
                continue  # not a finops rule
            ss = rule_meta.service_section
            if not ss:
                _LOG.warning(f'Rule {rule} does not have service section')
                continue
            res.setdefault(ss, []).append(
                {
                    'rule': self._col.meta[rule].get('description', rule),
                    'service': rule_meta.service
                    or service_from_resource_type(
                        self._col.meta[rule]['resource']
                    ),
                    'category': finops_category,
                    'severity': rule_meta.severity.value,
                    'resource_type': self._col.meta[rule]['resource'],
                    'resources': {
                        region: list(res)
                        for region, res in self._resources[rule].items()
                    },
                }
            )
        return res

    def operational_attacks(self) -> list:
        # TODO: refactor this format and it that generates it.
        #  This code just generates mitre report the way it was generated
        pre_result = {}

        for rule in self._resources:
            meta = self._meta.rule(rule)
            if not meta.mitre:
                _LOG.warning(f'Mitre metadata not found for {rule}. Skipping')
                continue
            severity = meta.severity
            resource_type = meta.service or service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            description = self._col.meta[rule].get('description', '')

            for region, res in self._resources[rule].items():
                for tactic, data in meta.mitre.items():
                    for technique in data:
                        technique_name = technique.get('tn_name')
                        technique_id = technique.get('tn_id')
                        sub_techniques = [
                            st['st_name'] for st in technique.get('st', [])
                        ]
                        resources_data = [
                            {
                                'resource': r,
                                'resource_type': resource_type,
                                'rule': description,
                                'severity': severity.value,
                                'sub_techniques': sub_techniques,
                            }
                            for r in res
                        ]
                        tactics_data = pre_result.setdefault(
                            tactic,
                            {
                                'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                                'techniques_data': {},
                            },
                        )
                        techniques_data = tactics_data[
                            'techniques_data'
                        ].setdefault(
                            technique_name,
                            {'technique_id': technique_id, 'regions_data': {}},
                        )
                        regions_data = techniques_data[
                            'regions_data'
                        ].setdefault(region, {'resources': []})
                        regions_data['resources'].extend(resources_data)
        result = []
        for tactic, techniques in pre_result.items():
            item = {
                'tactic_id': techniques['tactic_id'],
                'tactic': tactic,
                'techniques_data': [],
            }
            for technique, data in techniques['techniques_data'].items():
                item['techniques_data'].append(
                    {**data, 'technique': technique}
                )
            result.append(item)
        return result

    def operational_k8s_attacks(self) -> list[dict]:
        # TODO REFACTOR IT
        pre_result = {}
        for rule in self._resources:
            meta = self._meta.rule(rule)
            if not meta.mitre:
                _LOG.warning(f'Mitre metadata not found for {rule}. Skipping')
                continue
            severity = meta.severity
            resource_type = meta.service or service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            description = self._col.meta[rule].get('description', '')

            for _, res in self._resources[rule].items():
                for tactic, data in meta.mitre.items():
                    for technique in data:
                        technique_name = technique.get('tn_name')
                        technique_id = technique.get('tn_id')
                        sub_techniques = [
                            st['st_name'] for st in technique.get('st', [])
                        ]
                        tactics_data = pre_result.setdefault(
                            tactic,
                            {
                                'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                                'techniques_data': {},
                            },
                        )
                        techniques_data = tactics_data[
                            'techniques_data'
                        ].setdefault(
                            technique_name, {'technique_id': technique_id}
                        )
                        resources_data = techniques_data.setdefault(
                            'resources', []
                        )
                        resources_data.extend(
                            [
                                {
                                    'resource': r,
                                    'resource_type': resource_type,
                                    'rule': description,
                                    'severity': severity.value,
                                    'sub_techniques': sub_techniques,
                                }
                                for r in res
                            ]
                        )
        result = []

        for tactic, techniques in pre_result.items():
            item = {
                'tactic_id': techniques['tactic_id'],
                'tactic': tactic,
                'techniques_data': [],
            }
            for technique, data in techniques['techniques_data'].items():
                item['techniques_data'].append(
                    {**data, 'technique': technique}
                )
            result.append(item)
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
        tenant_or_platform: str = '',
        region: str = '',
    ) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(
            (type_.value, customer, project, cloud, tenant_or_platform, region)
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
            cloud=tenant.cloud,
            tenant_or_platform=tenant.name,
            region=region,
        )

    @classmethod
    def key_for_platform(cls, type_: ReportType, platform: Platform):
        return cls.build_key(
            type_=type_,
            customer=platform.customer,
            cloud=Cloud.KUBERNETES.value,
            tenant_or_platform=platform.id,
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

    def query_by_platform(
        self,
        platform: Platform,
        type_: ReportType,
        till: datetime | None = None,
        ascending: bool = False,
        limit: int | None = None,
    ) -> Iterator[ReportMetrics]:
        return self.query(
            key=self.key_for_platform(type_, platform),
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
        hook=lambda x: isinstance(x, (int, float)) and not isinstance(x, bool),
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
