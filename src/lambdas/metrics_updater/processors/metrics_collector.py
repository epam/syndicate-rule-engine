import copy
import heapq
import statistics
from datetime import datetime
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Generator,
    Iterable,
    Iterator,
    MutableMapping,
    Optional,
    cast,
)

import msgspec
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import ModularServiceProvider
from typing_extensions import Self

from helpers import RequestContext
from helpers.constants import (
    DEPRECATED_RULE_SUFFIX,
    GLOBAL_REGION,
    TACTICS_ID_MAPPING,
    TS_EXCLUDED_RULES_KEY,
    Cloud,
    JobState,
    PolicyErrorType,
    RemediationComplexity,
    ReportType,
    Severity,
)
from helpers.log_helper import get_logger
from helpers.reports import service_from_resource_type
from helpers.time_helper import utc_datetime, utc_iso
from lambdas.metrics_updater.processors.base import (
    BaseProcessor,
    NextLambdaEvent,
)
from models.metrics import ReportMetrics
from models.ruleset import Ruleset
from services import SP, modular_helpers
from services.coverage_service import MappingAverageCalculator
from services.job_service import JobService
from services.license_service import License, LicenseService
from services.metadata import Metadata, MitreAttack
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform, PlatformService
from services.report_service import ReportService
from services.reports import (
    ActivatedTenantsIterator,
    AttacksReportGenerator,
    EmptyOperationalReportGenerator,
    JobMetricsDataSource,
    KubernetesReport,
    Report,
    ReportMetricsService,
    ReportVisitor,
    ResourcesReportGenerator,
    ScopedRulesSelector,
    ShardsCollectionDataSource,
    ShardsCollectionProvider,
)
from services.resource_exception_service import ResourceExceptionsService
from services.resources import (
    CloudResource,
    MaestroReportResourceView,
    iter_rule_resources,
)
from services.resources_service import ResourcesService
from services.ruleset_service import RulesetName, RulesetService
from services.sharding import ShardsCollection


if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

    from services.report_service import AverageStatisticsItem

ReportsGen = Generator[tuple[ReportMetrics, dict], None, None]

_LOG = get_logger(__name__)

# NOTE: there is a lot of code repetition here that:
# 1. makes this file quite big and congested
# 2. sometimes not optimal performance-wise.
# But as far as i see it the repetition is not a problem whatsoever.
# Furthermore, i'm sure that is more an advantage here
# because these metrics and reports have some problems that are not easy to
# detect and fix. So I tried to pull all the "calculus" logic out but keep all
# the reports creation logic separated for each individual report type so that
# we could apply fixes and improvements individually even though most have
# the same boilerplate.
# The business logic itself is quite confusing so no need to make it
# more unclear by providing more abstraction

TOP_TENANT_LENGTH = 10
TOP_CLOUD_LENGTH = 5


class MetricsContext:
    """
    Keeps some common context and data for one task of collecting metrics
    """

    __slots__ = '_cst', '_l', '_dt', '_meta', '_reports'

    def __init__(
        self,
        cst: Customer,
        licenses: tuple[License, ...],
        metadata: Metadata,
        now: datetime | None = None,
    ):
        self._cst = cst
        self._l = licenses
        self._dt = now
        self._meta = metadata
        self._reports = {}

    def __enter__(self):
        if not self._dt:
            self._dt = utc_datetime()
        self._reports.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reports.clear()

    @property
    def now(self) -> datetime:
        assert self._dt, 'now can be accessed only within the context'
        return self._dt

    @property
    def customer(self) -> Customer:
        return self._cst

    @property
    def licenses(self) -> tuple[License, ...]:
        return self._l

    @property
    def metadata(self) -> Metadata:
        return self._meta

    @property
    def n_reports(self) -> int:
        return self._reports.__len__()

    def has_reports(self) -> bool:
        return bool(self._reports)

    def add_report(self, report: ReportMetrics, data: dict):
        key = report.entity, report.type
        assert key not in self._reports, (
            'adding the same report twice within one context, smt is wrong'
        )
        self._reports[key] = (report, data)

    def add_reports(self, reports: Iterator[tuple[ReportMetrics, dict]]):
        for report in reports:
            self.add_report(*report)

    def iter_reports(
        self, entity: str | None = None, typ: ReportType | None = None
    ) -> ReportsGen:
        for (en, t), rep in self._reports.items():
            if entity and en != entity:
                continue
            if typ and t != typ:
                continue
            yield rep

    def get_report(
        self, entity: str, typ: ReportType
    ) -> tuple[ReportMetrics, dict] | None:
        return self._reports.get((entity, typ))

    def pop_report(self) -> tuple[ReportMetrics, dict]:
        return self._reports.popitem()[1]


class RuleCheck(msgspec.Struct, kw_only=True, frozen=True):
    id: str
    description: str | msgspec.UnsetType = msgspec.UNSET
    remediation: str | msgspec.UnsetType = msgspec.UNSET
    remediation_complexity: RemediationComplexity | msgspec.UnsetType = (
        msgspec.UNSET
    )
    severity: Severity | msgspec.UnsetType = msgspec.UNSET
    service: str | msgspec.UnsetType = msgspec.UNSET
    resource_type: str | msgspec.UnsetType = msgspec.UNSET

    region: str | msgspec.UnsetType = msgspec.UNSET
    when: float | msgspec.UnsetType = msgspec.UNSET
    error_type: PolicyErrorType | msgspec.UnsetType = msgspec.UNSET
    error: str | msgspec.UnsetType = msgspec.UNSET
    found: int | msgspec.UnsetType = msgspec.UNSET


class LicenseMetadata(msgspec.Struct, kw_only=True, frozen=True):
    id: str
    rulesets: tuple[str, ...] = ()
    total_rules: int
    jobs: int
    per: str
    description: str
    valid_until: str
    valid_from: str


class ReportRulesMetadata(msgspec.Struct, kw_only=True, frozen=True):
    total: int = 0
    disabled: tuple[str, ...] = ()
    deprecated: tuple[str, ...] = ()
    passed: tuple[RuleCheck, ...] = ()
    failed: tuple[RuleCheck, ...] = ()
    violated: tuple[RuleCheck, ...] | msgspec.UnsetType = msgspec.UNSET

    not_executed: tuple[str, ...] | msgspec.UnsetType = msgspec.UNSET


class TenantReportMetadata(msgspec.Struct, kw_only=True, frozen=True):
    licenses: tuple[LicenseMetadata, ...] = ()
    is_automatic_scans_enabled: bool = True
    in_progress_scans: int = 0
    finished_scans: int = 0
    succeeded_scans: int = 0
    last_scan_date: str | None = None
    activated_regions: tuple[str, ...] = ()
    rules: ReportRulesMetadata = msgspec.field(
        default_factory=ReportRulesMetadata
    )


class MetricsCollector(BaseProcessor):
    """
    Here when i say "collect data for reports", "collect metrics",
    "collect reports" i generally mean the same thing.

    We have multiple types of metrics that we must collect (multiple types
    of reports that we can send). Sources of data that we can use to
    collect metrics:
    - all standard and event-driven jobs, their results and statistics for each
    - latest state of resources for each tenant
    - snapshots of resources state for each tenant with configured snapshot
      period. Default is 4 hours for onprem.
    - customers and tenants, applications, parents, etc. - models from DB
    - rules index and some meta that is available locally - description,
      resource type
    - data from Cloud Custodian, only onprem because lambdas do not have
      Cloud Custodian package
    - private rule metadata, (currently not clear)

    What you should understand is that each item of ReportType enum represents
    a separate type of reports. That type has its own reporting period,
    data format and logic behind it. So, generally we should be able to
    collect data for each that report individually for any specified date.
    Such a way of collecting metrics would probably be easy to understand and
    reason about but it would be inefficient because we would probably need to
    query the same data sources and aggregate the same data multiple times
    especially if we collect all data for the same period. And.. that is
    actually the case. So this MetricsCollector class and MetricsContext
    created to allow reusing some data and optimizing the metrics collection
    process assuming that most our reports share a lot.

    Example to think about: Operational Overview report contains some
    overview information as of its reporting date, specifically it has
    total number of violated resources. Operational Rules report historically
    must also contain total number of violated resources as of its reporting
    date. This number is quite expensive to calculate because currently it
    requires iterating over all rules and their resources,
    making deduplication, etc. Generally we could not just steal that value
    from Op. Overview to Op. Rules because they can be just collected for
    different periods. But here i do that anyway because i know reports
    are collected for the same period here.
    """

    processor_name = "metrics"

    def __init__(
        self,
        modular_client: ModularServiceProvider,
        job_service: JobService,
        report_service: ReportService,
        report_metrics_service: ReportMetricsService,
        license_service: LicenseService,
        ruleset_service: RulesetService,
        platform_service: PlatformService,
        resource_service: ResourcesService,
        resource_exception_service: ResourceExceptionsService,
        tenant_settings_service: 'TenantSettingsService',
    ):
        self._mc = modular_client
        self._js = job_service
        self._rs = report_service
        self._rms = report_metrics_service
        self._ls = license_service
        self._rss = ruleset_service
        self._ps = platform_service
        self._res_ser = resource_service
        self._res_exp_ser = resource_exception_service
        self._tss = tenant_settings_service

        self._tenants_cache = {}
        self._platforms_cache = {}
        self._rulesets_cache = {}

        self._entities_it = ActivatedTenantsIterator(mc=self._mc, ls=self._ls)

    @staticmethod
    def nlargest(
        items: list[dict], n: int, reverse: bool = False
    ) -> list[dict]:
        def key(i: dict):
            return i.get('sort_by', 0)

        if reverse:
            return heapq.nlargest(n, items, key=key)
        else:
            return heapq.nsmallest(n, items, key=key)

    @staticmethod
    def yield_one_per_cloud(
        it: Iterable[Tenant],
    ) -> Generator[tuple[Cloud, Tenant], None, None]:
        """
        Maestro has so-called tenant groups. They call them "tenants" so
        here we have a confusion. "Tenant" as a model is one AWS account or
        one AZURE subscription or one GOOGLE project, etc.
        "Tenant" as a group is a number of "Tenant" models where
        each cloud can be found only once. So, a number of tenants in a
        tenant group cannot exceed the total number of supported clouds
        because a tenant of specific cloud can be added only once.

        This method iterates over the given tenants and yields a cloud and a
        tenant. If it founds a second tenant with already yielded cloud,
        it just skips it with warning.
        """
        yielded = set()
        for tenant in it:
            cloud = tenant_cloud(tenant)
            if cloud in yielded:
                _LOG.warning(
                    f'Found another tenant with the same cloud: {tenant.name}'
                )
                continue
            yield cloud, tenant
            yielded.add(cloud)

    @staticmethod
    def base_clouds_payload() -> dict[str, list]:
        return {
            Cloud.AWS.value: [],
            Cloud.AZURE.value: [],
            Cloud.GOOGLE.value: [],
        }

    @staticmethod
    def base_cloud_payload_dict() -> dict[str, dict]:
        return {
            Cloud.AWS.value: {},
            Cloud.AZURE.value: {},
            Cloud.GOOGLE.value: {},
        }

    @classmethod
    def build(cls) -> Self:
        return cls(
            modular_client=SP.modular_client,
            job_service=SP.job_service,
            report_service=SP.report_service,
            report_metrics_service=SP.report_metrics_service,
            license_service=SP.license_service,
            ruleset_service=SP.ruleset_service,
            platform_service=SP.platform_service,
            resource_service=SP.resources_service,
            resource_exception_service=SP.resource_exception_service,
            tenant_settings_service=\
                SP.modular_client.tenant_settings_service(),
        )

    @staticmethod
    def whole_period(
        now: datetime, /, *reports: ReportType
    ) -> tuple[datetime | None, datetime]:
        """
        Accepts the date where reports are collected and number of reports
        that should be collected. Returns the biggest whole period (start, end)
        that we need to collect these reports. Example:
        say today (date when metrics are collected) is 2024-11-15,
        we want to collect tenant overview metrics (their period is one week
        from Sunday to Sunday: [2024-11-10, 2024-11-17]) and c-level metrics
        for customer (their period is one previous month:
        [2024-10-01, 2024-11-01]).
        So the whole period for which we need data is [2024-10-01, 2024-11-17].
        But since future data does not exist we can set upper bound to now.
        """
        if not reports:
            return None, now
        start = None
        end = None
        for report in reports:
            t_end = now + report.r_end
            if end is None or t_end > end:
                end = t_end
            if report.r_start is None:
                continue
            t_start = now + report.r_start
            if start is None or t_start < start:
                start = t_start
        if end is None or end > now:  # actually end never None here
            end = now
        return start, end

    @staticmethod
    def _update_dict_values(target: dict, from_: dict) -> None:
        for k, v in from_.items():
            target.setdefault(k, 0)
            target[k] += v

    def _get_licensed_ruleset(self, name: str) -> Ruleset | None:
        if name in self._rulesets_cache:
            return self._rulesets_cache[name]
        item = self._rss.get_licensed(name)
        if not item:
            return
        self._rulesets_cache[name] = item
        return item

    def _get_tenant(self, name: str) -> Tenant | None:
        # TODO: support is_active
        if name in self._tenants_cache:
            return self._tenants_cache[name]
        item = self._mc.tenant_service().get(name)
        if not item:
            return
        self._tenants_cache[name] = item
        return item

    def _get_platform(self, platform_id: str) -> Platform | None:
        if platform_id in self._platforms_cache:
            return self._platforms_cache[platform_id]
        platform = self._ps.get_nullable(platform_id)
        if not platform:
            return
        self._ps.fetch_application(platform)
        self._platforms_cache[platform_id] = platform
        return platform

    @staticmethod
    def _complete_rules_report(
        it: ReportsGen, ctx: MetricsContext
    ) -> ReportsGen:
        for rep, data in it:
            ov = ctx.get_report(rep.entity, ReportType.OPERATIONAL_OVERVIEW)
            if ov is None:
                _LOG.warning(
                    'Cannot complete rules report because correspond operational is not found'
                )
            elif rep.type is ReportType.OPERATIONAL_RULES:
                data['resources_violated'] = ov[1]['data'][
                    'resources_violated'
                ]
            yield rep, data

    def _get_license_cloud_metadata(
        self, lic: License, cloud: Cloud
    ) -> tuple[LicenseMetadata, set[str]] | None:
        """
        Builds license metadata for the given cloud and also returns
        a set of all rules
        """
        rulesets = []
        for name in lic.ruleset_ids:
            rs = self._get_licensed_ruleset(name)
            if not rs:
                _LOG.warning(f'Somehow licensed ruleset {name} does not exist')
                continue
            cl = rs.cloud
            if cl == 'GCP':
                cl = 'GOOGLE'
            if cl == cloud:
                rulesets.append(rs)
        if not rulesets:
            return
        rules = set(chain.from_iterable(r.rules for r in rulesets))
        meta = LicenseMetadata(
            id=lic.license_key,
            rulesets=tuple([r.name for r in rulesets]),
            total_rules=len(rules),
            jobs=lic.allowance['job_balance'],
            per=lic.allowance['time_range'],
            description=lic.description,
            valid_until=utc_iso(lic.expiration),
            valid_from=utc_iso(lic.valid_from),
        )
        return meta, rules

    def _get_tenant_disabled_rules(self, tenant: Tenant) -> set[str]:
        """
        Takes into consideration rules that are excluded for that specific tenant
        and for its customer
        """
        _LOG.info(f'Querying disabled rules for {tenant.name} rules')
        excluded = set()
        ts = SP.modular_client.tenant_settings_service().get(
            tenant_name=tenant.name, key=TS_EXCLUDED_RULES_KEY
        )
        if ts:
            _LOG.info('Tenant setting with excluded rules is found')
            excluded.update(ts.value.as_dict().get('rules') or ())
        cs = SP.modular_client.customer_settings_service().get_nullable(
            customer_name=tenant.customer_name, key=TS_EXCLUDED_RULES_KEY
        )
        if cs:
            _LOG.info('Customer setting with excluded rules is found')
            excluded.update(cs.value.get('rules') or ())
        return excluded

    def _get_tenant_exceptions_data(
        self,
        tenant: Tenant,
        collection: ShardsCollection | None,
        cloud: Cloud,
        metadata: Metadata,
    ) -> list[dict]:
        """
        Returns exceptions data for tenant if collection exists.
        """
        if not collection:
            return []
        exceptions = self._res_exp_ser.get_resource_exceptions_collection_by_tenant(tenant)
        exceptions_data, _ = exceptions.filter_exception_resources(
            collection, cloud, metadata, tenant.project
        )
        return exceptions_data

    @staticmethod
    def _iter_failed_checks(
        collection: 'ShardsCollection', scope: set[str]
    ) -> Generator[RuleCheck, None, None]:
        meta = collection.meta
        for part in collection.iter_error_parts():
            if part.policy not in scope:
                continue
            assert part.has_error(), 'part must have an error'
            yield RuleCheck(
                id=part.policy,
                description=meta.get(part.policy, {}).get('description') or '',
                region=part.location,
                when=part.timestamp,
                error_type=part.error_type,
                error=part.error_message,
            )

    @staticmethod
    def _iter_passed_checks(
        collection: 'ShardsCollection', scope: set[str]
    ) -> Generator[RuleCheck, None, None]:
        # NOTE: currently it skips failed checks even though we can include
        # them, and it will not violate any logic. It is more a matter of
        # what is better to display for the user (rule could pass and then
        # fail, maybe due to missing credentials and it will mean that data
        # is slightly outdated, but this rule still passed before)
        meta = collection.meta
        for part in collection.iter_all_parts():
            if (
                part.has_error()
                or len(part.resources) > 0
                or part.policy not in scope
            ):
                continue
            yield RuleCheck(
                id=part.policy,
                description=meta.get(part.policy, {}).get('description') or '',
                region=part.location,
                when=part.timestamp,
            )

    @staticmethod
    def _iter_violated_checks(
        collection: 'ShardsCollection', metadata: Metadata, scope: set[str]
    ) -> Generator[RuleCheck, None, None]:
        """
        These duplicate the rules inside reports payload, but contains rules
        metadata without duplicates
        """
        yielded: set[str] = set()
        meta = collection.meta
        for part in collection.iter_parts():
            policy = part.policy
            if (
                (policy not in scope)
                or len(part.resources) == 0
                or (policy in yielded)
            ):
                continue
            yielded.add(policy)
            rm = metadata.rule(policy)
            rt = meta[policy]['resource']
            yield RuleCheck(
                id=policy,
                description=meta[policy].get('description') or '',
                remediation=rm.remediation,
                remediation_complexity=rm.remediation_complexity,
                severity=rm.severity,
                service=rm.service or service_from_resource_type(rt),
                resource_type=rt,
                when=part.timestamp,
            )

    @staticmethod
    def _get_rule_resources(
        collection: 'ShardsCollection',
        cloud: Cloud,
        metadata: Metadata,
        account_id: str = '',
    ) -> dict[str, set[CloudResource]]:
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
        it = iter_rule_resources(
            collection=collection,
            cloud=cloud,
            metadata=metadata,
            account_id=account_id,
        )
        dct = {}
        for k, v in it:
            resources = set(v)
            if not resources:
                continue
            dct[k] = resources
        return dct

    def _save_all(self, ctx: MetricsContext, msg: str):
        _LOG.info(msg)
        while ctx.has_reports():
            self._rms.save(*ctx.pop_report())

    def collect_operational_for_tenant(
        self,
        ctx: MetricsContext,
        scp: ShardsCollectionProvider,
        js: JobMetricsDataSource,
        tenant: Tenant,
        licenses: tuple[License, ...],
    ) -> ReportsGen:
        """
        Collects some operational reports for a tenant. Since they all
        have a lot similar we can reuse the data and make it here all at once
        sacrificing code readability
        """
        types = (
            ReportType.OPERATIONAL_RESOURCES,
            ReportType.OPERATIONAL_FINOPS,
            ReportType.OPERATIONAL_ATTACKS,
            ReportType.OPERATIONAL_DEPRECATION,
            ReportType.OPERATIONAL_OVERVIEW,
        )
        cloud = tenant_cloud(tenant)
        _licenses_meta = []
        cloud_rules = set()
        for lic in licenses:
            pair = self._get_license_cloud_metadata(lic, cloud)
            if pair is None:
                continue  # should not happen
            _licenses_meta.append(pair[0])
            cloud_rules.update(pair[1])

        licenses = tuple(_licenses_meta)
        disabled = tuple(self._get_tenant_disabled_rules(tenant))
        active_regions = tuple(modular_helpers.get_tenant_regions(
            tenant,
            self._tss
        ))

        ls = self._js.get_tenant_last_job_date(tenant.name)
        if not ls:
            # case when tenant had no scans, so we yield empty reports
            _LOG.warning(f'No jobs for tenant {tenant} found')
            empty_meta = TenantReportMetadata(
                licenses=licenses,
                is_automatic_scans_enabled=True,  # because maestro is active for tenant
                activated_regions=active_regions,
            )
            selector = ScopedRulesSelector(ctx.metadata)
            empty_report_generator = EmptyOperationalReportGenerator()
            for typ in types:
                report = Report.derive_report(typ)
                scope = report.accept(selector, rules=cloud_rules)

                meta = msgspec.structs.replace(
                    empty_meta,
                    rules=ReportRulesMetadata(
                        total=len(scope),
                        disabled=disabled,
                        violated=(),
                        # not_executed=tuple(report_rules)
                    ),
                )

                item = self._rms.create(
                    key=ReportMetrics.build_key_for_tenant(typ, tenant),
                    end=typ.end(ctx.now),
                    start=typ.start(ctx.now),
                    tenants=(tenant.name,),
                )
                yield (
                    item,
                    {
                        'metadata': meta,
                        'data': report.accept(empty_report_generator),
                        'id': tenant.project,
                    },
                )
            return
        _LOG.info(f'Last scan date for tenant {tenant.name} is {ls}')

        selector = ScopedRulesSelector(ctx.metadata)
        view = MaestroReportResourceView()

        # NOTE, actually we should retrieve a separate collection for
        # each report type, because they could have different dates. But here
        # we can afford it because we assert that each has the same date
        _LOG.info(
            f'Going to retrieve shards collection for operation reports '
            f'for tenant {tenant.name} and date {ReportType.OPERATIONAL_RESOURCES.end(ctx.now)}'
        )
        collection = scp.get_for_tenant(
            tenant, ReportType.OPERATIONAL_RESOURCES.end(ctx.now)
        )
        if not collection:
            _LOG.warning(
                'Somehow collection for operational reports is not found or empty even though the tenant has at least one successful jobs'
            )
            return
        exceptions = (
            self._res_exp_ser.get_resource_exceptions_collection_by_tenant(
                tenant
            )
        )
        exceptions_data, collection = exceptions.filter_exception_resources(
            collection, cloud, ctx.metadata, tenant.project
        )

        rule_resources = self._get_rule_resources(
            collection, cloud, ctx.metadata, tenant.project
        )

        type_resources = self._res_ser.get_type_resources_for_tenant(
            tenant, collection.meta
        )

        deprecated = tuple(self._iter_deprecated_rules(collection))

        for typ in types:
            start = typ.start(ctx.now)
            end = typ.end(ctx.now)
            _LOG.info(
                f'Going to collect operational report {typ} for tenant {tenant.name}: {start} - {end}'
            )
            job_source = js.subset(
                start=start, end=end, tenant=tenant.name, affiliation='tenant'
            )
            _LOG.info(f'Tenant had {len(job_source)} jobs in the period')
            report = Report.derive_report(typ)
            scope = report.accept(selector, rules=cloud_rules)
            total = len(scope)

            # Some selectors can filter disabled and deprecated rules,
            # so we don't want to include them in that report
            disabled_loc = tuple(scope.intersection(disabled))
            deprecated_loc = tuple(scope.intersection(deprecated))

            scope.difference_update(disabled)
            scope.difference_update(deprecated)

            meta = TenantReportMetadata(
                licenses=licenses,
                is_automatic_scans_enabled=True,
                in_progress_scans=job_source.n_in_progress,
                finished_scans=job_source.n_finished,
                succeeded_scans=job_source.n_succeeded,
                last_scan_date=ls,
                activated_regions=active_regions,
                rules=ReportRulesMetadata(
                    total=total,
                    disabled=disabled_loc,
                    deprecated=deprecated_loc,
                    passed=tuple(self._iter_passed_checks(collection, scope)),
                    failed=tuple(self._iter_failed_checks(collection, scope)),
                    violated=tuple(
                        self._iter_violated_checks(
                            collection, ctx.metadata, scope
                        )
                    ),
                ),
            )
            generator = ReportVisitor.derive_visitor(
                typ,
                metadata=ctx.metadata,
                view=view,
                scope=scope,
                report_service=self._rs,
            )
            data = report.accept(
                generator,
                rule_resources=rule_resources,
                collection=collection,
                type_resources=type_resources,
                start=start,
                end=end,
                meta=collection.meta,
                cloud=cloud,
            )
            if not isinstance(data, dict):
                # TODO: somehow move this info to visitors abstraction
                data = tuple(data)

            item = self._rms.create(
                key=ReportMetrics.build_key_for_tenant(typ, tenant),
                end=end,
                start=start,
                tenants=(tenant.name,),
            )
            yield (
                item,
                {
                    'metadata': meta,
                    'data': data,
                    'id': tenant.project,
                    'exceptions_data': exceptions_data,
                },
            )

    def collect_metrics(self, ctx: MetricsContext):
        # TODO: make here some assertions about report types
        #  about dates,

        start, end = self.whole_period(ctx.now, *ReportType)
        jobs = self._js.get_by_customer_name(
            customer_name=ctx.customer.name,
            start=start,
            end=end,
            ascending=True,  # important to have jobs in order
        )
        js = JobMetricsDataSource(jobs)
        if not js:
            _LOG.warning('No jobs for customer found')

        scp = ShardsCollectionProvider(self._rs)

        _LOG.info(
            f'Going to collect operational reports for customer {ctx.customer.name}'
        )

        for tenant, licenses in self._entities_it.iter_tenant_licenses(
            ctx.customer, ctx.licenses
        ):
            _LOG.info(
                f'Going to collect operational reports for tenant {tenant.name}'
            )
            ctx.add_reports(
                self.collect_operational_for_tenant(
                    ctx=ctx, scp=scp, js=js, tenant=tenant, licenses=licenses
                )
            )

        # TODO: refactor old metrics
        self._collect_old(
            ctx=ctx, start=start, end=end, job_source=js, sc_provider=scp
        )

    def _collect_old(
        self,
        *,
        ctx: MetricsContext,
        start: datetime,
        end: datetime,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
    ):
        """
        Collects predefined hard-coded set of reports. Here we definitely know
        that all the reports are collected as of "now" date, so we can cache
        some sources of data and use lower-level metrics to calculate higher
        level
        """
        _LOG.info(f'Need to collect jobs data from {start} to {end}')

        _LOG.info('Generating operational rules for all tenants')
        ctx.add_reports(
            self._complete_rules_report(
                self.operational_rules(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.OPERATIONAL_RULES,
                ),
                ctx,
            )
        )
        _LOG.info('Generating operational compliance for all tenants')
        ctx.add_reports(
            self.operational_compliance(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_COMPLIANCE,
            )
        )
        _LOG.info('Generating operational k8s report for all platforms')
        ctx.add_reports(
            self.operational_k8s(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_KUBERNETES,
            )
        )

        # TODO: save reports to db and s3 immideately when they're not needed anymore
        _LOG.info('Generating project overview reports for all tenant groups')
        ctx.add_reports(
            self.project_overview(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_OVERVIEW)
                ),
                report_type=ReportType.PROJECT_OVERVIEW,
                scp=sc_provider,
            )
        )

        _LOG.info(
            'Generating project compliance reports for all tenant groups'
        )
        ctx.add_reports(
            self.project_compliance(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_COMPLIANCE)
                ),
                report_type=ReportType.PROJECT_COMPLIANCE,
                scp=sc_provider,
            )
        )

        _LOG.info('Generating project resources reports for all tenant groups')
        ctx.add_reports(
            self.project_resources_with_new_schema(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_RESOURCES)
                ),
                report_type=ReportType.PROJECT_RESOURCES,
                scp=sc_provider,
            )
        )
        _LOG.info('Generating project attacks reports for all tenant groups')
        ctx.add_reports(
            self.project_attacks(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_ATTACKS)
                ),
                report_type=ReportType.PROJECT_ATTACKS,
                scp=sc_provider,
            )
        )
        _LOG.info('Generating project finops reports for all tenant groups')
        ctx.add_reports(
            self.project_finops(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_FINOPS)
                ),
                report_type=ReportType.PROJECT_FINOPS,
                scp=sc_provider,
            )
        )
        self._save_all(ctx, 'saving operational and project')

        # Department
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD, ctx.now
        ):
            _LOG.info('Generating department top resources by cloud')
            ctx.add_reports(
                self.top_resources_by_cloud(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD,
                )
            )

        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES, ctx.now
        ):
            _LOG.info('Generating department top tenants resources')
            ctx.add_reports(
                self.top_tenants_resources(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES,
                )
            )
        if not self._rms.was_collected_for_customer(
            ctx.customer,
            ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD,
            ctx.now,
        ):
            _LOG.info('Generating department top compliance by cloud')
            ctx.add_reports(
                self.top_compliance_by_cloud(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD,
                )
            )
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE, ctx.now
        ):
            _LOG.info('Generating department top tenants compliance')
            ctx.add_reports(
                self.top_tenants_compliance(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE,
                )
            )
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD, ctx.now
        ):
            _LOG.info('Generating department top attacks by cloud')
            ctx.add_reports(
                self.top_attacks_by_cloud(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD,
                )
            )
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS, ctx.now
        ):
            _LOG.info('Generating department top tenant attacks')
            ctx.add_reports(
                self.top_tenants_attacks(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS,
                )
            )

        # C-level
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.C_LEVEL_OVERVIEW, ctx.now
        ):
            _LOG.info(
                'Generating c-level overview for all tenants because '
                'it has not be collected yet'
            )
            ctx.add_reports(
                self.c_level_overview(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.C_LEVEL_OVERVIEW,
                )
            )

        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.C_LEVEL_COMPLIANCE, ctx.now
        ):
            _LOG.info(
                'Generating c-level compliance for all tenants because '
                'it has not be collected yet'
            )
            ctx.add_reports(
                self.c_level_compliance(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.C_LEVEL_COMPLIANCE,
                )
            )
        if not self._rms.was_collected_for_customer(
            ctx.customer, ReportType.C_LEVEL_ATTACKS, ctx.now
        ):
            _LOG.info(
                'Generating c-level attacks for all tenants because '
                'it has not be collected yet'
            )
            ctx.add_reports(
                self.c_level_attacks(
                    ctx=ctx,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.C_LEVEL_ATTACKS,
                )
            )

        self._save_all(ctx, 'saving department and c-level')

    def _expand_rules_statistics(
        self,
        it: Iterable['AverageStatisticsItem'],
        ctx: MetricsContext,
        meta: dict | None = None,
        type_resources: dict | None = None,
    ) -> Generator['AverageStatisticsItem', None, None]:
        meta = meta or {}
        for item in it:
            p = item.policy
            # Exclude deprecated rules
            if p.endswith(DEPRECATED_RULE_SUFFIX):
                continue
            item.id = item.policy
            item.policy = meta.get(p, {}).get('description', p)
            item.resource_type = meta.get(p, {}).get('resource', '')

            rm = ctx.metadata.rule(p)
            item.service = rm.service or service_from_resource_type(
                item.resource_type
            )
            item.severity = rm.severity

            # TODO: implement average resources scanned
            if type_resources:
                resources = [
                    res for res in type_resources.get(meta[p]['resource'], [])
                    if res.location == item.region or res.location == GLOBAL_REGION
                ]
                item.resources_scanned = len(resources)
                item.average_resources_scanned = item.resources_scanned

            yield item

    def operational_rules(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            tjs = js.subset(tenant=tenant.name, job_state=JobState.SUCCEEDED)
            col = sc_provider.get_for_tenant(tenant, end)
            exceptions = (
                self._res_exp_ser.get_resource_exceptions_collection_by_tenant(
                    tenant
                )
            )
            _, col = exceptions.filter_exception_resources(
                col, tenant_cloud(tenant), ctx.metadata, tenant.project
            )
            type_resources = self._res_ser.get_type_resources_for_tenant(tenant, col.meta)

            outdated = []
            lsd = tjs.last_succeeded_scan_date
            if not lsd:
                outdated.append(tenant.name)
                lsd = job_source.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date
            data = {
                'succeeded_scans': len(tjs),
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant, self._tss)
                ),
                'last_scan_date': lsd,
                'id': tenant.project,
                'data': list(
                    self._expand_rules_statistics(
                        it=self._rs.average_statistics(
                            *map(self._rs.job_statistics, tjs)
                        ),
                        ctx=ctx,
                        meta=col.meta if col else {},
                        type_resources=type_resources if type_resources else {},
                    )
                ),
                'outdated_tenants': outdated,
            }
            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_tenant(
                        report_type, tenant
                    ),
                    end=end,
                    start=start,
                    tenants=[tenant.name],
                ),
                data,
            )

    def operational_compliance(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            col = sc_provider.get_for_tenant(tenant, end)
            if not col:
                _LOG.warning(
                    f'Shards collection for {tenant.name} for {end} is empty'
                )
                continue
            if tenant.cloud == Cloud.AWS:
                mapping = self._rs.group_parts_iterator_by_location(
                    self._rs.iter_successful_parts(col)
                )
            else:
                mapping = {
                    GLOBAL_REGION: list(self._rs.iter_successful_parts(col))
                }

            region_coverages = {}
            for location, parts in mapping.items():
                region_coverages[location] = self._rs.calculate_coverages(
                    successful=self._rs.get_standard_to_controls_to_rules(
                        it=parts, metadata=ctx.metadata
                    ),
                    full=ctx.metadata.domain(tenant.cloud).full_cov,
                )

            # calculating total across whole account. For all except AWS those
            # are the same
            total = self._rs.calculate_tenant_full_coverage(
                col, ctx.metadata, tenant_cloud(tenant)
            )

            outdated = []
            lsd = js.subset(tenant=tenant.name).last_succeeded_scan_date
            if not lsd:
                outdated.append(tenant.name)
                lsd = job_source.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date
            data = {
                'id': tenant.project,
                'last_scan_date': lsd,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant, self._tss)
                ),
                'data': {
                    'regions': {
                        location: {
                            st.full_name: cov for st, cov in standards.items()
                        }
                        for location, standards in region_coverages.items()
                    },
                    'total': {st.full_name: cov for st, cov in total.items()},
                },
                'outdated_tenants': outdated,
            }
            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_tenant(
                        report_type, tenant
                    ),
                    end=end,
                    start=start,
                    tenants=[tenant.name],
                ),
                data,
            )

    def operational_k8s(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='platform')
        for platform_id in js.scanned_platforms:
            platform = self._get_platform(platform_id)
            if not platform:
                _LOG.warning(f'Platform with id {platform_id} not found!')
                continue
            col = sc_provider.get_for_platform(platform, end)

            tenant = self._get_tenant(platform.tenant_name)
            if not tenant:
                _LOG.warning(
                    f'Tenant with name {platform.tenant_name} for platform {platform_id} not found!'
                )
                continue
            exceptions = (
                self._res_exp_ser.get_resource_exceptions_collection_by_tenant(
                    tenant
                )
            )
            _, col = exceptions.filter_exception_resources(
                col, Cloud.KUBERNETES, ctx.metadata, tenant.project
            )
            if not col:
                _LOG.warning(
                    f'Shards collection for {platform_id} for {end} is empty'
                )
                continue

            coverages = self._rs.calculate_coverages(
                successful=self._rs.get_standard_to_controls_to_rules(
                    it=self._rs.iter_successful_parts(col),
                    metadata=ctx.metadata,
                ),
                full=ctx.metadata.domain(Cloud.KUBERNETES).full_cov,
            )

            outdated = []
            lsd = js.subset(platform=platform_id).last_succeeded_scan_date
            if not lsd:
                outdated.append(platform.tenant_name)
                lsd = job_source.subset(
                    platform=platform_id
                ).last_succeeded_scan_date

            rule_resources = self._get_rule_resources(
                collection=col, cloud=Cloud.KUBERNETES, metadata=ctx.metadata
            )

            view = MaestroReportResourceView()
            attacks_gen = AttacksReportGenerator(
                metadata=ctx.metadata, view=view
            )
            resources_gen = ResourcesReportGenerator(
                metadata=ctx.metadata, view=view
            )
            resources = tuple(
                KubernetesReport(report_type).accept(
                    resources_gen, rule_resources=rule_resources, meta=col.meta
                )
            )
            attacks = tuple(
                KubernetesReport(report_type).accept(
                    attacks_gen, rule_resources=rule_resources, meta=col.meta
                )
            )

            # Collect violated rules for cluster metadata
            k8s_rules_scope = {p.policy for p in col.iter_parts()}
            violated_rules = tuple(
                self._iter_violated_checks(col, ctx.metadata, k8s_rules_scope)
            )

            data = {
                'tenant_name': platform.tenant_name,
                'last_scan_date': lsd,
                'region': platform.region,
                'name': platform.name,
                'type': platform.type.value,
                'resources': resources,
                'compliance': {
                    st.full_name: cov for st, cov in coverages.items()
                },
                'mitre': attacks,
                'outdated_tenants': outdated,
                'violated_rules': violated_rules,
            }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_platform(
                        report_type, platform
                    ),
                    end=end,
                    start=start,
                    tenants=[platform.tenant_name],
                ),
                data,
            )

    def project_overview(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_OVERVIEW
            for r in operational_reports
        )

        # they are totally the same as operational overview
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)

        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)
        for dn, reports in dn_to_reports.items():
            if len(reports) > 3:
                _LOG.warning(
                    'Something is wrong: one project contains more than one tenant per cloud'
                )
            data = self.base_cloud_payload_dict()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                cloud = tenant_cloud(tenant)
                collection = scp.get_for_tenant(tenant, end)
                
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant, collection, cloud, ctx.metadata
                )

                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['metadata'].last_scan_date,
                    'activated_regions': item[1]['metadata'].activated_regions,
                    'total_scans': item[1]['metadata'].finished_scans,
                    'failed_scans': item[1]['metadata'].finished_scans
                    - item[1]['metadata'].succeeded_scans,
                    'succeeded_scans': item[1]['metadata'].succeeded_scans,
                    'resources_violated': item[1]['data'][
                        'resources_violated'
                    ],
                    'regions_data': {
                        r: {
                            'severity_data': d['resources'],
                            'resource_types_data': d['resource_types'],
                        }
                        for r, d in item[1]['data']['regions'].items()
                    },
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': (), 'data': data},
            )

    def project_compliance(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_COMPLIANCE
            for r in operational_reports
        )

        # they are totally the same as operational compliance
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)

        for dn, reports in dn_to_reports.items():
            data = self.base_cloud_payload_dict()
            outdated_tenants = set()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                outdated_tenants.update(item[1]['outdated_tenants'])
                cloud = tenant_cloud(tenant)

                collection = scp.get_for_tenant(
                    tenant=tenant, 
                    date=end,
                )
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant=tenant, 
                    collection=collection, 
                    cloud=cloud, 
                    metadata=ctx.metadata,
                )

                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['last_scan_date'],
                    'activated_regions': item[1]['activated_regions'],
                    'data': item[1]['data'],
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': list(outdated_tenants), 'data': data},
            )

    # TODO: rewrite to not rely on operational resources
    def project_resources(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_RESOURCES
            for r in operational_reports
        )

        # they are totally the same as operational compliance
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)

        for dn, reports in dn_to_reports.items():
            data = self.base_cloud_payload_dict()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                cloud = tenant_cloud(tenant)

                collection = scp.get_for_tenant(
                    tenant=tenant,
                    date=end,
                )
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant=tenant,
                    collection=collection,
                    cloud=cloud,
                    metadata=ctx.metadata,
                )

                policies_data = []
                for policy in item[1].get('data', []):
                    policies_data.append(
                        {
                            'policy': policy['policy'],
                            'description': policy['description'],
                            'severity': policy['severity'],
                            'resource_type': policy['resource_type'],
                            'regions_data': {
                                region: {'total_violated_resources': len(res)}
                                for region, res in policy['resources'].items()
                            },
                        }
                    )
                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['metadata'].last_scan_date,
                    'activated_regions': item[1]['metadata'].activated_regions,
                    'data': policies_data,
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': [], 'data': data},
            )

    def project_attacks(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_ATTACKS
            for r in operational_reports
        )

        # they are totally the same as operational compliance
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)

        for dn, reports in dn_to_reports.items():
            data = self.base_cloud_payload_dict()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                cloud = tenant_cloud(tenant)

                collection = scp.get_for_tenant(
                    tenant=tenant,
                    date=end,
                )
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant=tenant,
                    collection=collection,
                    cloud=cloud,
                    metadata=ctx.metadata,
                )

                new_mitre_data = {}
                for res in item[1].get('data', ()):
                    for attack in res['attacks']:
                        inner = new_mitre_data.setdefault(
                            msgspec.convert(attack, type=MitreAttack), {}
                        )
                        inner.setdefault(res['region'], {}).setdefault(
                            attack['severity'], 0
                        )
                        inner[res['region']][attack['severity']] += 1

                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['metadata'].last_scan_date,
                    'activated_regions': item[1]['metadata'].activated_regions,
                    'attacks': [
                        {**attack.to_dict(), 'regions': regions_data}
                        for attack, regions_data in new_mitre_data.items()
                    ],
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': [], 'data': data},
            )

    def project_finops(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_FINOPS
            for r in operational_reports
        )

        # they are totally the same as operational compliance
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)

        for dn, reports in dn_to_reports.items():
            data = self.base_cloud_payload_dict()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                cloud = tenant_cloud(tenant)

                collection = scp.get_for_tenant(
                    tenant=tenant,
                    date=end,
                )
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant=tenant, 
                    collection=collection, 
                    cloud=cloud, 
                    metadata=ctx.metadata,
                )


                service_data = []
                for finops_data in item[1].get('data', ()):
                    ss = finops_data['service_section']
                    ss_data = finops_data.get('rules_data', [])

                    rules_data = copy.deepcopy(ss_data)
                    for rule in rules_data:
                        rule['regions_data'] = {
                            region: {'total_violated_resources': len(res)}
                            for region, res in rule.pop(
                                'resources', {}
                            ).items()
                        }

                    service_data.append(
                        {'service_section': ss, 'rules_data': rules_data}
                    )

                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['metadata'].last_scan_date,
                    'activated_regions': item[1]['metadata'].activated_regions,
                    'service_data': service_data,
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': [], 'data': data},
            )

    def top_resources_by_cloud(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        cloud_tenant = self.base_clouds_payload()
        outdated = set()
        all_tenants = set()
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if not col:
                _LOG.warning(
                    f'Shards collection for {tenant.name} for {end} is empty'
                )
                continue
            tjs = js.subset(tenant=tenant.name)

            lsd = tjs.last_succeeded_scan_date
            if not lsd:
                outdated.add(tenant.name)
                lsd = job_source.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date
            all_tenants.add(tenant.name)

            sdc = ShardsCollectionDataSource(
                col, ctx.metadata, cloud, tenant.project
            )

            cloud_tenant.setdefault(cloud.value, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'sort_by': (n_unique := sdc.n_unique),
                    'data': {
                        'activated_regions': sorted(
                            modular_helpers.get_tenant_regions(
                                tenant,
                                self._tss,
                            )
                        ),
                        'tenant_name': tenant_name,
                        'last_scan_date': lsd,
                        'total_scans': tjs.n_succeeded + tjs.n_failed,
                        'failed_scans': tjs.n_failed,
                        'succeeded_scans': tjs.n_succeeded,
                        'resources_violated': n_unique,
                        'resource_types_data': sdc.resource_types(),
                        'severity_data': sdc.severities(),
                    },
                }
            )
        for cloud in cloud_tenant:
            cloud_tenant[cloud] = self.nlargest(
                cloud_tenant[cloud], TOP_CLOUD_LENGTH, True
            )

        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'outdated_tenants': list(outdated), 'data': cloud_tenant},
        )

    def top_tenants_resources(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        outdated = set()
        all_tenants = set()
        dn_tenants = {}
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            dn_tenants.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(tenant)
        data = []
        for dn, tenants in dn_tenants.items():
            clouds_data = {}
            sort_by = 0
            for cloud, tenant in self.yield_one_per_cloud(tenants):
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                tjs = js.subset(tenant=tenant.name)
                # TODO: cache shards collection data source for the same dates
                sdc = ShardsCollectionDataSource(
                    col, ctx.metadata, cloud, tenant.project
                )

                lsd = tjs.last_succeeded_scan_date
                if not lsd:
                    outdated.add(tenant.name)
                    lsd = job_source.subset(
                        tenant=tenant.name
                    ).last_succeeded_scan_date
                all_tenants.add(tenant.name)

                n_unique = sdc.n_unique
                clouds_data[cloud.value] = {
                    'last_scan_date': lsd,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant, self._tss)
                    ),
                    'tenant_name': tenant.name,
                    'account_id': tenant.project,
                    'total_scans': tjs.n_succeeded + tjs.n_failed,
                    'succeeded_scans': tjs.n_succeeded,
                    'failed_scans': tjs.n_failed,
                    'resources_violated': n_unique,
                    'resource_types_data': sdc.resource_types(),
                    'severity_data': sdc.severities(),
                }
                sort_by += n_unique

            data.append(
                {
                    'tenant_display_name': dn,
                    'sort_by': sort_by,
                    'data': clouds_data,
                }
            )
        data = self.nlargest(data, TOP_TENANT_LENGTH, True)
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': list(outdated)},
        )

    def top_compliance_by_cloud(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        cloud_tenant = self.base_clouds_payload()
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        all_tenants = set()
        outdated = set()
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if not col:
                _LOG.warning(
                    f'Shards collection for {tenant.name} for {end} is empty'
                )
                continue
            if not js.subset(tenant=tenant.name).last_succeeded_scan_date:
                outdated.add(tenant.name)
            all_tenants.add(tenant.name)

            total = self._rs.calculate_tenant_full_coverage(
                col, ctx.metadata, cloud
            )

            cloud_tenant.setdefault(cloud.value, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'sort_by': statistics.mean(total.values()) if total else 0,
                    'data': {st.full_name: cov for st, cov in total.items()},
                }
            )

        for cloud in cloud_tenant:
            cloud_tenant[cloud] = self.nlargest(
                cloud_tenant[cloud], TOP_CLOUD_LENGTH, False
            )

        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': cloud_tenant, 'outdated_tenants': list(outdated)},
        )

    def top_tenants_compliance(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        all_tenants = set()
        outdated = set()
        dn_tenants = {}
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            dn_tenants.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(tenant)
        data = []
        for dn, tenants in dn_tenants.items():
            clouds_data = {}
            percents = []
            for cloud, tenant in self.yield_one_per_cloud(tenants):
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                tjs = js.subset(tenant=tenant.name)
                lsd = tjs.last_succeeded_scan_date
                if not lsd:
                    outdated.add(tenant.name)
                    lsd = job_source.subset(
                        tenant=tenant.name
                    ).last_succeeded_scan_date
                all_tenants.add(tenant.name)

                total = self._rs.calculate_tenant_full_coverage(
                    col, ctx.metadata, cloud
                )
                clouds_data[cloud.value] = {
                    'last_scan_date': lsd,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant, self._tss)
                    ),
                    'tenant_name': tenant.name,
                    'average_data': {
                        st.full_name: cov for st, cov in total.items()
                    },
                }
                percents.extend(total.values())
            data.append(
                {
                    'tenant_display_name': dn,
                    'sort_by': statistics.mean(percents) if percents else 0,
                    'data': clouds_data,
                }
            )
        data = self.nlargest(data, TOP_TENANT_LENGTH, False)
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': list(outdated)},
        )

    def top_attacks_by_cloud(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        cloud_tenant = self.base_clouds_payload()
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        all_tenants = set()
        outdated = set()
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if not col:
                _LOG.warning(
                    f'Shards collection for {tenant.name} for {end} is empty'
                )
                continue
            tjs = js.subset(tenant=tenant.name)

            lsd = tjs.last_succeeded_scan_date
            if not lsd:
                outdated.add(tenant.name)
                lsd = job_source.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date
            all_tenants.add(tenant.name)

            scd = ShardsCollectionDataSource(
                col, ctx.metadata, cloud, tenant.project
            )

            tactic_severity = scd.tactic_to_severities()

            cloud_tenant.setdefault(cloud.value, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'last_scan_date': lsd,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant, self._tss)
                    ),
                    'tenant_name': tenant.name,
                    'account_id': tenant.project,
                    'sort_by': scd.n_unique,  # TODO: sort by what?
                    'data': [
                        {
                            'tactic_id': TACTICS_ID_MAPPING.get(
                                tactic_name, ''
                            ),
                            'tactic': tactic_name,
                            'severity_data': sev_data,
                        }
                        for tactic_name, sev_data in tactic_severity.items()
                    ],
                }
            )

        for cloud in cloud_tenant:
            cloud_tenant[cloud] = self.nlargest(
                cloud_tenant[cloud], TOP_CLOUD_LENGTH, True
            )
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': cloud_tenant, 'outdated_tenants': list(outdated)},
        )

    def top_tenants_attacks(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        all_tenants = set()
        outdated = set()
        dn_tenants = {}
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            dn_tenants.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(tenant)
        data = []
        for dn, tenants in dn_tenants.items():
            clouds_data = {}
            sort_by = 0
            for cloud, tenant in self.yield_one_per_cloud(tenants):
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                sdc = ShardsCollectionDataSource(
                    col, ctx.metadata, cloud, tenant.project
                )
                if not js.subset(tenant=tenant.name).last_succeeded_scan_date:
                    outdated.add(tenant.name)
                all_tenants.add(tenant.name)

                tactic_severity = sdc.tactic_to_severities()

                clouds_data[cloud.value] = [
                    {
                        'tactic_id': TACTICS_ID_MAPPING.get(tactic_name, ''),
                        'tactic': tactic_name,
                        'severity_data': sev_data,
                    }
                    for tactic_name, sev_data in tactic_severity.items()
                ]
                sort_by += sdc.n_unique

            data.append(
                {
                    'tenant_display_name': dn,
                    'sort_by': sort_by,
                    'data': clouds_data,
                }
            )
        data = self.nlargest(data, TOP_TENANT_LENGTH, True)
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': list(outdated)},
        )

    def c_level_overview(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        cloud_tenant = self.base_clouds_payload()
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud_tenant.setdefault(tenant.cloud, []).append(tenant)
        _LOG.info('Collecting licenses data')
        # TODO: in case these metrics are collected as of some past date the
        #  license information will not correspond to date
        licenses, rulesets = self._collect_licenses_data(ctx)
        data = {}
        all_tenants = set()
        outdated = set()
        for cloud, tenants in cloud_tenant.items():
            rt_data = {}
            sev_data = {}
            total = 0
            used_tenants = []
            for tenant in tenants:
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                all_tenants.add(tenant.name)
                if not js.subset(tenant=tenant.name).last_succeeded_scan_date:
                    outdated.add(tenant.name)

                sdc = ShardsCollectionDataSource(
                    col, ctx.metadata, tenant_cloud(tenant), tenant.project
                )
                self._update_dict_values(rt_data, sdc.resource_types())

                self._update_dict_values(sev_data, sdc.severities())
                total += sdc.n_unique
                used_tenants.append(tenant.name)

            tjs = js.subset(tenant=used_tenants)
            data[cloud] = {
                'failed_scans': tjs.n_failed,
                'last_scan_date': tjs.last_succeeded_scan_date,
                'resources_violated': total,
                'resource_types_data': rt_data,
                'severity_data': sev_data,
                'succeeded_scans': tjs.n_succeeded,
                'total_scanned_tenants': len(used_tenants),
                'total_scans': len(tjs),
                **self._cloud_licenses_info(cloud, licenses, rulesets),
            }
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': list(outdated)},
        )

    def c_level_compliance(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        cloud_tenant = self.base_clouds_payload()
        all_tenants = set()
        outdated = set()
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud_tenant.setdefault(tenant.cloud, []).append(tenant)
        _LOG.info('Collecting licenses data')
        # TODO: in case these metrics are collected as of some past date the
        #  license information will not correspond to date
        licenses, rulesets = self._collect_licenses_data(ctx)
        data = {}
        for cloud, tenants in cloud_tenant.items():
            calc = MappingAverageCalculator()
            for tenant in tenants:
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                all_tenants.add(tenant.name)
                if not js.subset(tenant=tenant.name).last_succeeded_scan_date:
                    outdated.add(tenant.name)
                total = self._rs.calculate_tenant_full_coverage(
                    col, ctx.metadata, tenant_cloud(tenant)
                )
                calc.update(total)

            data[cloud] = {
                **self._cloud_licenses_info(cloud, licenses, rulesets),
                'total_scanned_tenants': len(tenants),
                'last_scan_date': js.subset(
                    tenant={t.name for t in tenants}
                ).last_succeeded_scan_date,
                'average_data': {
                    st.full_name: cov for st, cov in calc.produce()
                },
            }
        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': list(outdated)},
        )

    def c_level_attacks(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        cloud_tenant = self.base_clouds_payload()
        all_tenants = set()
        outdated = set()
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud_tenant.setdefault(tenant.cloud, []).append(tenant)

        data = {}
        for cloud, tenants in cloud_tenant.items():
            tactic_severities = {}
            for tenant in tenants:
                col = sc_provider.get_for_tenant(tenant, end)
                if not col:
                    _LOG.warning(
                        f'Shards collection for {tenant.name} for {end} is empty'
                    )
                    continue
                all_tenants.add(tenant.name)
                if not js.subset(tenant=tenant.name).last_succeeded_scan_date:
                    outdated.add(tenant.name)
                sdc = ShardsCollectionDataSource(
                    col, ctx.metadata, tenant_cloud(tenant)
                )
                for (
                    tactic_name,
                    severities,
                ) in sdc.tactic_to_severities().items():
                    self._update_dict_values(
                        target=tactic_severities.setdefault(tactic_name, {}),
                        from_=severities,
                    )

            data[cloud] = [
                {
                    'tactic': tactic,
                    'tactic_id': TACTICS_ID_MAPPING.get(tactic, ''),
                    'severity_data': severity_data,
                }
                for tactic, severity_data in tactic_severities.items()
            ]

        yield (
            self._rms.create(
                key=ReportMetrics.build_key_for_customer(
                    report_type, ctx.customer.name
                ),
                start=start,
                end=end,
                tenants=all_tenants,
            ),
            {'data': data, 'outdated_tenants': outdated},
        )

    def _collect_licenses_data(
        self, ctx: MetricsContext
    ) -> tuple[tuple[License, ...], dict[str, Ruleset]]:
        """
        Collects customer licenses data and their rulesets
        """
        licenses = tuple(self._ls.iter_customer_licenses(ctx.customer.name))
        rulesets = {}  # cache
        for lic in licenses:
            for ruleset_id in lic.ruleset_ids:
                if ruleset_id not in rulesets:
                    rulesets[ruleset_id] = self._rss.get_licensed(ruleset_id)
        return licenses, rulesets

    @staticmethod
    def _cloud_licenses_info(
        cloud: str,  # Cloud
        licenses: tuple[License, ...],
        rulesets: dict[str, Ruleset],
    ) -> dict:
        """
        C level reports currently display license information as if only one
        license can ever exist for a cloud. Actually, number of licenses is
        not limited per cloud.

        :param cloud: cloud we need to collect data for
        :param licenses: list of all licenses within a customer
        :param rulesets: mapping ids to rulesets, all rulesets across
        customer licenses
        :return: data that will be stored in report and sent to Maestro
        """
        cloud_rulesets = []
        cloud_licenses = []
        for lic in licenses:
            for rid in lic.ruleset_ids:
                rs = rulesets.get(rid)
                if not rs:
                    _LOG.warning(
                        f'Somehow ruleset item {rid} does not exist in DB'
                    )
                    continue
                cl = rs.cloud.upper()
                if cl == 'GCP':
                    cl = 'GOOGLE'
                if cl == cloud:
                    cloud_licenses.append(lic)
                    if rs.versions:
                        cloud_rulesets.append(
                            RulesetName(
                                rs.name, sorted(rs.versions)[-1]
                            ).to_human_readable_str()
                        )
                    else:
                        cloud_rulesets.append(
                            RulesetName(
                                rs.name, rs.version
                            ).to_human_readable_str()
                        )
        if not cloud_licenses:
            return {'activated': False, 'license_properties': {}}

        # TODO: change this on Maestro side to allow multiple licenses
        main_l = cloud_licenses[0]
        expiration = None
        if exp := main_l.expiration:
            # the returned object is displayed directly, so we make
            # human-formatting here
            expiration = exp.strftime('%b %d, %Y %H:%M:%S %Z')

        # TODO: just pass raw data to Maestro: they must format it
        balance = main_l.allowance.get('job_balance')
        time_range = main_l.allowance.get('time_range')
        scan_frequency = (
            f'{balance} scan{"" if balance == 1 else "s"} per {time_range}'
        )

        return {
            'activated': True,
            'license_properties': {
                'Rulesets': sorted(cloud_rulesets),
                'Number of licenses': len(cloud_licenses),
                'Event-Driven mode': 'On'
                if any([lic.event_driven for lic in cloud_licenses])
                else 'Off',
                'Scans frequency': scan_frequency,
                'Expiration': expiration,
            },
        }

    def __call__(
        self, 
        event: Optional[MutableMapping] = None, 
        context: Optional[RequestContext] = None,
    ) -> Optional[NextLambdaEvent]:
        _LOG.info('Starting metrics collector')
        now = utc_datetime()
        for customer, licenses in self._entities_it.iter_customer_licenses(
            True
        ):
            _LOG.info(f'Collecting metrics for customer: {customer.name}')
            if not licenses:
                _LOG.warning(f'Customer {customer.name} has no licenses')
            metadata = self._ls.get_metadata_for_licenses(licenses)
            ctx = MetricsContext(
                cst=customer, licenses=licenses, metadata=metadata, now=now
            )

            with ctx:
                try:
                    self.collect_metrics(ctx)
                except Exception:
                    _LOG.exception(
                        f'Unexpected error occurred collecting metrics for {customer.name}'
                    )
                    raise
            self._tenants_cache.clear()
            self._platforms_cache.clear()
            self._rulesets_cache.clear()

    # NOTE: This function is just temporary solution and very inefficient
    # TODO: rewrite previous project_resources function without operational report
    def project_resources_with_new_schema(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
        scp: ShardsCollectionProvider,
    ) -> ReportsGen:
        assert all(
            r[0].type is ReportType.OPERATIONAL_RESOURCES
            for r in operational_reports
        )

        # they are totally the same as operational compliance
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        dn_to_reports = {}
        for item in operational_reports:
            tenant = cast(Tenant, self._get_tenant(item[0].tenant))
            dn_to_reports.setdefault(
                tenant.display_name_to_lower.lower(), []
            ).append(item)

        for dn, reports in dn_to_reports.items():
            data = self.base_cloud_payload_dict()
            tenants = set()
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))
                tenants.add(tenant.name)
                cloud = tenant_cloud(tenant)

                collection = scp.get_for_tenant(
                    tenant=tenant, 
                    date=end,
                )
                exceptions_data = self._get_tenant_exceptions_data(
                    tenant=tenant, 
                    collection=collection, 
                    cloud=cloud, 
                    metadata=ctx.metadata,
                )

                policies_dict = {}
                for policy in item[1]['metadata'].rules.violated:
                    policies_dict[policy.id] = {
                        'description': policy.description,
                        'severity': policy.severity.value,
                    }
                policies_data = {}
                for resource in item[1]['data']:
                    for policy in resource['violations']:
                        if policy['policy'] in policies_data:
                            region = policies_data[policy['policy']][
                                'regions_data'
                            ]
                            if resource['region'] not in region:
                                region[resource['region']] = {
                                    'total_violated_resources': 1
                                }
                            else:
                                region[resource['region']][
                                    'total_violated_resources'
                                ] += 1
                        else:
                            policies_data[policy['policy']] = {
                                'policy': policy['policy'],
                                'description': policies_dict[policy['policy']][
                                    'description'
                                ],
                                'severity': policies_dict[policy['policy']][
                                    'severity'
                                ],
                                'resource_type': resource['resource_type'],
                                'regions_data': {
                                    resource['region']: {
                                        'total_violated_resources': 1
                                    }
                                },
                            }
                data[cloud.value] = {
                    'account_id': item[1]['id'],
                    'tenant_name': tenant.name,
                    'last_scan_date': item[1]['metadata'].last_scan_date,
                    'activated_regions': item[1]['metadata'].activated_regions,
                    'data': list(policies_data.values()),
                    'exceptions_data': exceptions_data,
                }

            yield (
                self._rms.create(
                    key=ReportMetrics.build_key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                    tenants=tenants,
                ),
                {'outdated_tenants': [], 'data': data},
            )

    def _iter_deprecated_rules(
        self, collection: ShardsCollection
    ) -> Iterable[str]:
        """
        Iterates over deprecated rules in the collection.
        """
        for policy in collection.meta:
            if policy.endswith(DEPRECATED_RULE_SUFFIX):
                yield policy
