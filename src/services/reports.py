import bisect
from datetime import datetime
from enum import Enum
from functools import cached_property, cmp_to_key
from itertools import chain
from typing import Generator, Iterable, Iterator, Literal, cast, Callable, Any

from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant

from helpers import deep_get, hashable, iter_key_values
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


class CustomAttribute(str, Enum):
    REGION = 'sre:region'
    SERVICE = 'sre:service'

    @classmethod
    def pop_custom(cls, dct: dict) -> None:
        for attr in cls:
            dct.pop(attr.value, None)


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


class ShardsCollectionDataSource:
    ResourcesGenerator = Generator[tuple[str, str, dict, float], None, None]

    def __init__(
        self, collection: ShardsCollection, metadata: Metadata, cloud: Cloud
    ):
        """
        Assuming that collection is pulled and has meta
        """
        self._col = collection
        self._meta = metadata
        self._cloud = cloud

        self.__resources = None

    def iter_resources(self):
        for part in self._col.iter_parts():
            for res in part.resources:
                yield part.policy, part.location, res, part.timestamp

    @staticmethod
    def _allow_only_regions(
        it: ResourcesGenerator, regions: set[str] | tuple[str, ...]
    ) -> ResourcesGenerator:
        for rule, region, dto, ts in it:
            if region in regions:
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

    @staticmethod
    def _allow_only_resource_type(
        it: ResourcesGenerator, meta: dict, resource_type: tuple[str, ...]
    ) -> ResourcesGenerator:
        to_check = tuple(map(adjust_resource_type, resource_type))
        for rule, region, dto, ts in it:
            rt = adjust_resource_type(meta.get(rule, {})['resource'])
            if rt in to_check:
                yield rule, region, dto, ts

    def _add_custom_attributes_google(
        self, it: ResourcesGenerator
    ) -> ResourcesGenerator:
        for rule, region, dto, ts in it:
            dto_copy = dto.copy()
            if ser := self._meta.rule(rule).service:
                _LOG.debug('Adding service to google resource')
                dto[CustomAttribute.SERVICE.value] = ser
            yield rule, region, dto_copy, ts

    def _add_custom_attributes_aws(
        self, it: ResourcesGenerator
    ) -> ResourcesGenerator:
        """
        Some resources require special treatment.
        - rules with resource type "aws.cloudtrail" are not multiregional,
        but the resource they look for can be either multiregional or not.
        So we must deduplicate them on the scope of whole account.
        252 and other glue-catalog rules are not multiregional, but they
        do not return unique information within region.

        Also, most rules with aws.account resource type belong to different
        services, so we add some custom attributes to make deduplication more
        robust
        """
        meta = self._col.meta
        _need_region = ('glue-catalog', 'account')

        for rule, region, dto, ts in it:
            dto_copy = dto.copy()

            rt = meta.get(rule, {})['resource']
            rt = adjust_resource_type(rt)

            if rt in _need_region:
                _LOG.debug(
                    f'Rule with type {rt} found. Adding region '
                    f'attribute to make its dto differ from '
                    f'other regions'
                )
                dto_copy[CustomAttribute.REGION.value] = region

            # NOTE: rules with aws.account resource type can literally
            #  relate to different services, and we can know that only
            #  by using metadata.
            if rt == 'account' and (ser := self._meta.rule(rule).service):
                _LOG.debug('Adding service to rule with aws.account resource')
                dto_copy[CustomAttribute.SERVICE.value] = ser

            if rt == 'cloudtrail':
                if dto.get('IsMultiRegionTrail'):
                    _LOG.debug(
                        'Found multiregional trail. '
                        'Moving it to multiregional region'
                    )
                    region = GLOBAL_REGION
            yield rule, region, dto_copy, ts

    def _keep_only_report_fields(
        self, it: ResourcesGenerator
    ) -> ResourcesGenerator:
        """
        Keeps only report fields for each resource. Custom attributes are
        not removed because they are added purposefully
        :param it:
        :return:
        """
        base_to_keep = REPORT_FIELDS | set(CustomAttribute)

        for rule, region, dto, ts in it:
            fields = base_to_keep.union(self._meta.rule(rule).report_fields)
            for k in tuple(dto):
                if k not in fields:
                    dto.pop(k)
            yield rule, region, dto, ts

    def create_resources_generator(
        self,
        only_report_fields: bool = True,
        active_regions: tuple[str, ...] = (),
        resource_types: tuple[str, ...] = (),
    ) -> ResourcesGenerator:
        it = self.iter_resources()
        match self._cloud:
            case Cloud.AWS:
                it = self._add_custom_attributes_aws(it)
            case Cloud.GOOGLE:
                it = self._add_custom_attributes_google(it)

        if only_report_fields:
            it = self._keep_only_report_fields(it)

        # NOTE about deduplication: there is no sense in de-duplicating
        # resources within one policy and one region because it would be an
        # error if one policy invocation returned not unique resources.
        # The same thing about deduplication within one policy across
        # different regions. There should be no the same resources in different
        #  regions returned by one policy (otherwise this policy must be
        # global, right?). We can benefit from deduplication only if we do
        # that within one region across different policies: rule1 checks
        # whether ec2 instances have termination protection, rule2 checks
        # whether ec2 instances have metadataV2. Both rules find the same
        # ec2 instance with id 1 in eu-central-1 region. There are two
        # findings, but there is only one violating resource.
        # But this deduplication below is needed because it fixes resources
        # after self._add_custom_attributes_*() methods. After applying the
        # method the iterator can produce more than one unique resources within
        # one policy and region (which cannot happen under normal
        # circumstances). So we need this deduplication here for some corner
        # cases (As multiregional CloudTrail, for example)
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
        mapping = {}
        for rule, region, dto, _ in self.create_resources_generator():
            mapping.setdefault(region, {}).setdefault(rule, []).append(dto)
        self.__resources = mapping
        return mapping

    def clear(self):
        self.__resources = None

    @cached_property
    def n_unique(self) -> int:
        n = 0
        for rule_resources in self._resources.values():
            n += len(
                set(
                    map(hashable, chain.from_iterable(rule_resources.values()))
                )
            )
        return n

    @cached_property
    def n_findings(self) -> int:
        n = 0
        for rule_resources in self._resources.values():
            for res in rule_resources.values():
                n += len(res)
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
        for region in self._resources:
            _inner = region_severity.setdefault(region, {})
            for rule, res in self._resources[region].items():
                _inner.setdefault(
                    self._meta.rule(rule).severity.value, set()
                ).update(map(hashable, res))

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

    def resources(self) -> Generator[dict, None, None]:
        inverted = {}
        for region in self._resources:
            for rule, resources in self._resources[region].items():
                # we won't encounter the same region for one rule twice
                inverted.setdefault(rule, {})[region] = resources

        meta = self._col.meta
        for rule in inverted:
            rm = meta.get(rule, {})
            yield {
                'policy': rule,
                'resource_type': service_from_resource_type(
                    self._col.meta[rule]['resource']
                ),
                'description': rm.get('description') or '',
                'severity': self._meta.rule(rule).severity.value,
                'resources': inverted[rule],
            }

    def resources_no_regions(self) -> Generator[dict, None, None]:
        for res in self.resources():
            res['resources'] = list(
                chain.from_iterable(res['resources'].values())
            )
            yield res

    def region_resource_types(self) -> dict[str, dict[str, int]]:
        region_resource = {}
        for region in self._resources:
            _inner = region_resource.setdefault(region, {})
            for rule, res in self._resources[region].items():
                rt = service_from_resource_type(
                    self._col.meta[rule]['resource']
                )
                _inner.setdefault(rt, set()).update(map(hashable, res))

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
        for region in self._resources:
            _inner = region_service.setdefault(region, {})
            for rule, res in self._resources[region].items():
                ser = self._meta.rule(
                    rule
                ).service or service_from_resource_type(
                    self._col.meta[rule]['resource']
                )
                _inner.setdefault(ser, set()).update(map(hashable, res))

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

    def finops(self) -> dict[str, list[dict]]:
        """
        Produces finops data in its old format
        """
        inverted = {}
        for region in self._resources:
            for rule, resources in self._resources[region].items():
                # we won't encounter the same region for one rule twice
                inverted.setdefault(rule, {})[region] = resources

        res = {}
        for rule in inverted:
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
                    'resource_type': service_from_resource_type(
                        self._col.meta[rule]['resource']
                    ),
                    'resources': inverted[rule],
                }
            )
        return res

    def deprecation(self) -> Generator[dict, None, None]:
        """
        Produces deprecations data in the format required by maestro
        """
        inverted = {}
        for region in self._resources:
            for rule, resources in self._resources[region].items():
                # we won't encounter the same region for one rule twice
                inverted.setdefault(rule, {})[region] = resources

        for rule in inverted:
            rule_meta = self._meta.rule(rule)
            depr = rule_meta.deprecation_category_date()
            if depr is None:
                continue  # not a deprecation rule
            category, date = depr
            yield {
                'category': category,
                'deprecation_date': date,
                'remediation_complexity': rule_meta.remediation_complexity.value,
                'remediation': rule_meta.remediation,
                'policy': rule,
                'description': self._col.meta[rule].get('description', ''),
                'resource_type': service_from_resource_type(
                    self._col.meta[rule]['resource']
                ),
                'resources': inverted[rule],
            }

    def operational_attacks(self) -> list:
        # TODO: refactor this format and it that generates it.
        #  This code just generates mitre report the way it was generated

        inverted = {}
        for region in self._resources:
            for rule, resources in self._resources[region].items():
                # we won't encounter the same region for one rule twice
                inverted.setdefault(rule, {})[region] = resources

        pre_result = {}

        for rule in inverted:
            meta = self._meta.rule(rule)
            if not meta.mitre:
                _LOG.warning(f'Mitre metadata not found for {rule}. Skipping')
                continue
            severity = meta.severity
            resource_type = service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            description = self._col.meta[rule].get('description', '')

            for region, res in inverted[rule].items():
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
        inverted = {}
        for region in self._resources:
            for rule, resources in self._resources[region].items():
                # we won't encounter the same region for one rule twice
                inverted.setdefault(rule, {})[region] = resources

        pre_result = {}
        for rule in inverted:
            meta = self._meta.rule(rule)
            if not meta.mitre:
                _LOG.warning(f'Mitre metadata not found for {rule}. Skipping')
                continue
            severity = meta.severity
            resource_type = service_from_resource_type(
                self._col.meta[rule]['resource']
            )
            description = self._col.meta[rule].get('description', '')

            for _, res in inverted[rule].items():
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

    def tactic_to_severities(self) -> dict[str, dict[str, int]]:
        """ """
        # not sure about its correctness because this kind of data mapping
        # is very slick
        result = {}
        for region in self._resources:
            tactic_severity = {}
            for rule, resources in self._resources[region].items():
                sev = self._meta.rule(rule).severity.value
                for tactic in self._meta.rule(rule).mitre:
                    tactic_severity.setdefault(tactic, {}).setdefault(
                        sev, set()
                    ).update(map(hashable, resources))
            for tactic, data in tactic_severity.items():
                keep_highest(
                    *[
                        data.get(k)
                        for k in sorted(
                            data.keys(), key=cmp_to_key(SeverityCmp())
                        )
                    ]
                )
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
            col = self._rs.tenant_latest_collection(tenant)
        else:
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


class ReportMetricsService(BaseDataService[ReportMetrics]):
    def __init__(
        self, s3_client: S3Client, environment_service: EnvironmentService
    ):
        super().__init__()

        self._s3 = s3_client
        self._env = environment_service

    def create(
        self,
        key: str,
        end: datetime,
        start: datetime | None = None,
        tenants: Iterable[str] = (),
    ) -> ReportMetrics:
        assert key.count(COMPOUND_KEYS_SEPARATOR) == 5, 'Invalid key'
        customer = key.split(COMPOUND_KEYS_SEPARATOR, 2)[1]
        return ReportMetrics(
            key=key,
            end=utc_iso(end),
            start=utc_iso(start) if start else None,
            customer=customer,
            tenants=list(tenants),
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
        Queries reports with exaclty matching time
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

    def save(self, item: ReportMetrics, data: dict | None = None) -> None:
        if data is None:
            return super().save(item)

        # TODO: do that based on item size
        if item.type in (
            ReportType.OPERATIONAL_RULES,
            ReportType.OPERATIONAL_RESOURCES,
            ReportType.OPERATIONAL_ATTACKS,
            ReportType.OPERATIONAL_KUBERNETES,
        ):
            # large
            bucket = self._env.default_reports_bucket_name()
            key = ReportMetricsBucketKeysBuilder.metrics_key(item)
            self._s3.gz_put_json(bucket, key, data)
            item.data = {}
            item.s3_url = str(S3Url.build(bucket, key))
            return super().save(item)
        item.data = data
        return super().save(item)

    def fetch_data(self, item: ReportMetrics) -> dict:
        if item.is_fetched:
            return item.data.as_dict()
        if not item.s3_url:
            # should not happen. This means there no data in S3 and in DB,
            # so the report is corrupted
            return {}
        url = S3Url(item.s3_url)
        data = cast(dict, self._s3.gz_get_json(url.bucket, url.key))
        # item.data = data  # no need to create a lot of other objects
        return data

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
