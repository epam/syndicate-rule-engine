import copy
import statistics
from datetime import datetime
from typing import Generator, Iterator, cast, Iterable

from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular

from helpers.constants import (
    GLOBAL_REGION,
    Cloud,
    JobState,
    ReportType,
    TACTICS_ID_MAPPING,
)
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.metrics import ReportMetrics
from models.ruleset import Ruleset
from services import SP, modular_helpers
from services.ambiguous_job_service import AmbiguousJobService
from services.license_service import License, LicenseService
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform, PlatformService
from services.report_service import ReportService
from services.reports import (
    JobMetricsDataSource,
    ReportMetricsService,
    ShardsCollectionDataSource,
    ShardsCollectionProvider,
)
from services.ruleset_service import RulesetName, RulesetService

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


class MetricsContext:
    """
    Keeps some common context and data for one task of collecting metrics
    """

    __slots__ = '_cst', '_dt', '_meta', '_reports'

    def __init__(
        self, cst: Customer, metadata: Metadata, now: datetime | None = None
    ):
        self._cst = cst
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
        if not self._dt:
            # not a use case actually
            return utc_datetime()
        return self._dt

    @property
    def customer(self) -> Customer:
        return self._cst

    @property
    def metadata(self) -> Metadata:
        return self._meta

    @property
    def n_reports(self) -> int:
        return self._reports.__len__()

    def add_report(self, report: ReportMetrics, data: dict):
        key = report.entity, report.type
        assert (
            key not in self._reports
        ), 'adding the same report twice within one context, smt is definitelly wrong'
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


class MetricsCollector:
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

    def __init__(
        self,
        modular_client: Modular,
        ambiguous_job_service: AmbiguousJobService,
        report_service: ReportService,
        report_metrics_service: ReportMetricsService,
        license_service: LicenseService,
        ruleset_service: RulesetService,
        platform_service: PlatformService,
    ):
        self._mc = modular_client
        self._ajs = ambiguous_job_service
        self._rs = report_service
        self._rms = report_metrics_service
        self._ls = license_service
        self._rss = ruleset_service
        self._ps = platform_service

        self._tenants_cache = {}
        self._platforms_cache = {}

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
    def base_clouds_payload() -> dict[Cloud, list]:
        return {Cloud.AWS: [], Cloud.AZURE: [], Cloud.GOOGLE: []}

    @classmethod
    def build(cls) -> 'MetricsCollector':
        return cls(
            modular_client=SP.modular_client,
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            report_metrics_service=SP.report_metrics_service,
            license_service=SP.license_service,
            ruleset_service=SP.ruleset_service,
            platform_service=SP.platform_service,
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
                data['resources_violated'] = ov[1]['resources_violated']
            yield rep, data

    def collect_metrics_for_customer(self, ctx: MetricsContext):
        """
        Collects predefined hard-coded set of reports. Here we definitely know
        that all the reports are collected as of "now" date, so we can cache
        some sources of data and use lower-level metrics to calculate higher
        level
        """
        start, end = self.whole_period(ctx.now, *ReportType)
        _LOG.info(f'Need to collect jobs data from {start} to {end}')
        jobs = self._ajs.to_ambiguous(
            self._ajs.get_by_customer_name(
                customer_name=ctx.customer.name,
                start=start,
                end=end,  # todo here end must be not including
                ascending=True,  # important
            )
        )
        job_source = JobMetricsDataSource(jobs)
        if not job_source:
            _LOG.warning('No jobs for customer found')

        sc_provider = ShardsCollectionProvider(self._rs)

        _LOG.info('Generating operational overview for all tenants')
        ctx.add_reports(
            self.operational_overview(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_OVERVIEW,
            )
        )
        _LOG.info('Generating operational resources for all tenants')
        ctx.add_reports(
            self.operational_resources(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_RESOURCES,
            )
        )

        _LOG.info('Generating operational rules for all tenants')
        ctx.add_reports(
            self._complete_rules_report(
                self.operational_rules(
                    now=ctx.now,
                    job_source=job_source,
                    report_type=ReportType.OPERATIONAL_RULES,
                ),
                ctx,
            )
        )
        _LOG.info('Generating operational finops for all tenants')
        ctx.add_reports(
            self.operational_finops(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_FINOPS,
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
        _LOG.info('Generating operational attacks for all tenants')
        ctx.add_reports(
            self.operational_attacks(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_ATTACKS,
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
        _LOG.info('Generating operational deprecations report for all tenants')
        ctx.add_reports(
            self.operational_deprecation(
                ctx=ctx,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_DEPRECATION,
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
            )
        )

        _LOG.info('Generating project resources reports for all tenant groups')
        ctx.add_reports(
            self.project_resources(
                ctx=ctx,
                operational_reports=list(
                    ctx.iter_reports(typ=ReportType.OPERATIONAL_RESOURCES)
                ),
                report_type=ReportType.PROJECT_RESOURCES,
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
            )
        )

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

        _LOG.info(f'Saving all reports items: {ctx.n_reports}')
        for rep, data in ctx.iter_reports():
            self._rms.save(rep, data)

    def operational_overview(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        # TODO: maybe collect for all tenants
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            col = sc_provider.get_for_tenant(tenant, end)
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            tjs = js.subset(tenant=tenant.name)
            sdc = ShardsCollectionDataSource(
                col, ctx.metadata, tenant_cloud(tenant)
            )
            # NOTE: ignoring jobs that are not finished
            succeeded, failed = tjs.n_succeeded, tjs.n_failed

            region_data = {}
            for region, data in sdc.region_severities(unique=True).items():
                region_data.setdefault(region, {})['severity'] = data
            for region, data in sdc.region_services().items():
                region_data.setdefault(region, {})['service'] = data

            data = {
                'total_scans': succeeded + failed,
                'failed_scans': failed,
                'succeeded_scans': succeeded,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
                'last_scan_date': tjs.last_succeeded_scan_date,
                'id': tenant.project,
                'resources_violated': sdc.n_unique,
                'total_findings': sdc.n_findings,
                'regions_data': region_data,
            }
            item = self._rms.create(
                key=self._rms.key_for_tenant(report_type, tenant),
                end=end,
                start=start,
            )
            yield item, data

    def operational_resources(
        self,
        ctx: MetricsContext,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ) -> ReportsGen:
        # TODO: how to determine whether to include tenant in reporting.
        #  Currently it's based on job_source.scanned_tenants and since
        #  this reports does not have start bound job_source contains jobs
        #  for a bigger period of time
        start = report_type.start(ctx.now)
        end = report_type.end(ctx.now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            col = sc_provider.get_for_tenant(tenant, end)
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            data = {
                'id': tenant.project,
                'data': list(
                    ShardsCollectionDataSource(
                        col, ctx.metadata, tenant_cloud(tenant)
                    ).resources()
                ),
                'last_scan_date': js.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
                ),
                data,
            )

    def operational_rules(
        self,
        now: datetime,
        job_source: JobMetricsDataSource,
        report_type: ReportType,
    ) -> ReportsGen:
        start = report_type.start(now)
        end = report_type.end(now)
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            tjs = js.subset(tenant=tenant.name, job_state=JobState.SUCCEEDED)

            data = {
                'succeeded_scans': len(tjs),
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
                'last_scan_date': tjs.last_succeeded_scan_date,
                'id': tenant.project,
                'data': list(
                    self._rs.average_statistics(
                        *map(self._rs.job_statistics, tjs)
                    )
                ),
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
                ),
                data,
            )

    def operational_finops(
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
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            data = {
                'id': tenant.project,
                'data': ShardsCollectionDataSource(
                    col, ctx.metadata, tenant_cloud(tenant)
                ).finops(),
                'last_scan_date': js.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
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
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
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
            data = {
                'id': tenant.project,
                'last_scan_date': js.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
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
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
                ),
                data,
            )

    def operational_attacks(
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
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            data = ShardsCollectionDataSource(
                col, ctx.metadata, tenant_cloud(tenant)
            ).operational_attacks()

            data = {
                'id': tenant.project,
                'last_scan_date': js.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
                'data': data,
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
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
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{platform.platform_id} for {end}'
                )
                continue
            ds = ShardsCollectionDataSource(
                col, ctx.metadata, Cloud.KUBERNETES
            )

            coverages = self._rs.calculate_coverages(
                successful=self._rs.get_standard_to_controls_to_rules(
                    it=self._rs.iter_successful_parts(col),
                    metadata=ctx.metadata,
                ),
                full=ctx.metadata.domain(Cloud.KUBERNETES).full_cov,
            )

            data = {
                'tenant_name': platform.tenant_name,
                'last_scan_date': js.subset(
                    platform=platform_id
                ).last_succeeded_scan_date,
                'region': platform.region,
                'name': platform.name,
                'type': platform.type.value,
                'resources': list(ds.resources_no_regions()),
                'compliance': {
                    st.full_name: cov for st, cov in coverages.items()
                },
                'mitre': ds.operational_k8s_attacks(),
            }

            yield (
                self._rms.create(
                    key=self._rms.key_for_platform(report_type, platform),
                    end=end,
                    start=start,
                ),
                data,
            )

    def operational_deprecation(
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
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            data = {
                'id': tenant.project,
                'last_scan_date': js.subset(
                    tenant=tenant.name
                ).last_succeeded_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
                'data': list(
                    ShardsCollectionDataSource(
                        col, ctx.metadata, tenant_cloud(tenant)
                    ).deprecation()
                ),
            }
            yield (
                self._rms.create(
                    key=self._rms.key_for_tenant(report_type, tenant),
                    end=end,
                    start=start,
                ),
                data,
            )

    def project_overview(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
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
            data = {
                Cloud.AWS.value: [],
                Cloud.AZURE.value: [],
                Cloud.GOOGLE.value: [],
            }
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))

                data.setdefault(tenant_cloud(tenant).value, []).append(
                    {
                        'account_id': item[1]['id'],
                        'tenant_name': tenant.name,
                        'last_scan_date': item[1]['last_scan_date'],
                        'activated_regions': item[1]['activated_regions'],
                        'total_scans': item[1]['total_scans'],
                        'failed_scans': item[1]['failed_scans'],
                        'succeeded_scans': item[1]['succeeded_scans'],
                        'regions_data': {
                            r: {
                                'severity_data': d['severity'],
                                'resource_types_data': d[
                                    'service'
                                ],  # TODO: maybe use resource types here instead of services
                            }
                            for r, d in item[1]['regions_data'].items()
                        },
                    }
                )

            yield (
                self._rms.create(
                    key=self._rms.key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                ),
                data,
            )

    def project_compliance(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
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
            data = {
                Cloud.AWS.value: [],
                Cloud.AZURE.value: [],
                Cloud.GOOGLE.value: [],
            }
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))

                data.setdefault(tenant_cloud(tenant).value, []).append(
                    {
                        'account_id': item[1]['id'],
                        'tenant_name': tenant.name,
                        'last_scan_date': item[1]['last_scan_date'],
                        'activated_regions': item[1]['activated_regions'],
                        'data': item[1]['data'],
                    }
                )

            yield (
                self._rms.create(
                    key=self._rms.key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                ),
                data,
            )

    def project_resources(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
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
            data = {
                Cloud.AWS.value: [],
                Cloud.AZURE.value: [],
                Cloud.GOOGLE.value: [],
            }
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))

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

                data.setdefault(tenant_cloud(tenant).value, []).append(
                    {
                        'account_id': item[1]['id'],
                        'tenant_name': tenant.name,
                        'last_scan_date': item[1]['last_scan_date'],
                        'activated_regions': item[1]['activated_regions'],
                        'data': policies_data,
                    }
                )

            yield (
                self._rms.create(
                    key=self._rms.key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                ),
                data,
            )

    def project_attacks(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
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
            data = {
                Cloud.AWS.value: [],
                Cloud.AZURE.value: [],
                Cloud.GOOGLE.value: [],
            }
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))

                mitre_data = copy.deepcopy(item[1].get('data', []))
                for mitre in mitre_data:
                    for technique in mitre.get('techniques_data', []):
                        for region_data in technique.get(
                            'regions_data', {}
                        ).values():
                            severity_data = {}
                            for r in region_data.pop('resources', {}):
                                severity_data.setdefault(r['severity'], 0)
                                severity_data[r['severity']] += 1

                            region_data['severity_data'] = severity_data

                data.setdefault(tenant_cloud(tenant).value, []).append(
                    {
                        'account_id': item[1]['id'],
                        'tenant_name': tenant.name,
                        'last_scan_date': item[1]['last_scan_date'],
                        'activated_regions': item[1]['activated_regions'],
                        'mitre_data': mitre_data,
                    }
                )

            yield (
                self._rms.create(
                    key=self._rms.key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                ),
                data,
            )

    def project_finops(
        self,
        ctx: MetricsContext,
        operational_reports: list[tuple[ReportMetrics, dict]],
        report_type: ReportType,
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
            data = {
                Cloud.AWS.value: [],
                Cloud.AZURE.value: [],
                Cloud.GOOGLE.value: [],
            }
            for item in reports:
                tenant = cast(Tenant, self._get_tenant(item[0].tenant))

                service_data = []
                for ss, ss_data in item[1].get('data', {}).items():
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

                data.setdefault(tenant_cloud(tenant).value, []).append(
                    {
                        'account_id': item[1]['id'],
                        'tenant_name': tenant.name,
                        'last_scan_date': item[1]['last_scan_date'],
                        'activated_regions': item[1]['activated_regions'],
                        'service_data': service_data,
                    }
                )

            yield (
                self._rms.create(
                    key=self._rms.key_for_project(
                        report_type, ctx.customer.name, dn
                    ),
                    end=end,
                    start=start,
                ),
                data,
            )

    def top_resources_by_cloud(
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
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            tjs = js.subset(tenant=tenant.name)
            sdc = ShardsCollectionDataSource(col, ctx.metadata, cloud)

            cloud_tenant.setdefault(cloud, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'sort_by': (n_unique := sdc.n_unique),
                    'data': {
                        'activated_regions': sorted(
                            modular_helpers.get_tenant_regions(tenant)
                        ),
                        'tenant_name': tenant_name,
                        'last_scan_date': tjs.last_succeeded_scan_date,
                        'total_scans': tjs.n_succeeded + tjs.n_failed,
                        'failed_scans': tjs.n_failed,
                        'succeeded_scans': tjs.n_succeeded,
                        'resources_violated': n_unique,
                        'resource_types_data': sdc.resource_types(),
                        'severity_data': sdc.severities(),
                    },
                }
            )
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            cloud_tenant,
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
                if col is None:
                    _LOG.warning(
                        f'Cannot get shards collection for '
                        f'{tenant.name} for {end}'
                    )
                    continue
                tjs = js.subset(tenant=tenant.name)
                # TODO: cache shards collection data source for the same dates
                sdc = ShardsCollectionDataSource(col, ctx.metadata, cloud)

                n_unique = sdc.n_unique
                clouds_data[cloud.value] = {
                    'last_scan_date': tjs.last_succeeded_scan_date,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant)
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
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            {'data': data},
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
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            total = self._rs.calculate_tenant_full_coverage(
                col, ctx.metadata, cloud
            )

            cloud_tenant.setdefault(cloud, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'sort_by': statistics.mean(total.values()) if total else 0,
                    'data': {st.full_name: cov for st, cov in total.items()},
                }
            )
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            cloud_tenant,
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
                if col is None:
                    _LOG.warning(
                        f'Cannot get shards collection for '
                        f'{tenant.name} for {end}'
                    )
                    continue
                tjs = js.subset(tenant=tenant.name)
                total = self._rs.calculate_tenant_full_coverage(
                    col, ctx.metadata, cloud
                )
                clouds_data[cloud.value] = {
                    'last_scan_date': tjs.last_succeeded_scan_date,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant)
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
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            {'data': data},
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
        js = job_source.subset(start=start, end=end, affiliation='tenant')
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud = tenant_cloud(tenant)
            col = sc_provider.get_for_tenant(tenant, end)
            if col is None:
                _LOG.warning(
                    f'Cannot get shards collection for '
                    f'{tenant.name} for {end}'
                )
                continue
            tjs = js.subset(tenant=tenant.name)
            scd = ShardsCollectionDataSource(col, ctx.metadata, cloud)

            tactic_severity = scd.tactic_to_severities()

            cloud_tenant.setdefault(cloud, []).append(
                {
                    'tenant_display_name': tenant.display_name_to_lower.lower(),
                    'last_scan_date': tjs.last_succeeded_scan_date,
                    'activated_regions': sorted(
                        modular_helpers.get_tenant_regions(tenant)
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
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            cloud_tenant,
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
                if col is None:
                    _LOG.warning(
                        f'Cannot get shards collection for '
                        f'{tenant.name} for {end}'
                    )
                    continue
                sdc = ShardsCollectionDataSource(col, ctx.metadata, cloud)

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
        yield (
            self._rms.create(
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            {'data': data},
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
        for cloud, tenants in cloud_tenant.items():
            rt_data = {}
            sev_data = {}
            total = 0
            used_tenants = []
            for tenant in tenants:
                col = sc_provider.get_for_tenant(tenant, end)
                if col is None:
                    _LOG.warning(
                        f'Cannot get shards collection for '
                        f'{tenant.name} for {end}'
                    )
                    continue
                sdc = ShardsCollectionDataSource(
                    col, ctx.metadata, tenant_cloud(tenant)
                )
                self._update_dict_values(rt_data, sdc.services())

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
                key=self._rms.key_for_customer(report_type, ctx.customer.name),
                start=start,
                end=end,
            ),
            data,
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
                    rulesets[ruleset_id] = self._rss.by_lm_id(ruleset_id)
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
                            ).to_str()
                        )
                    else:
                        cloud_rulesets.append(
                            RulesetName(rs.name, rs.version).to_str()
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

    def __call__(self):
        _LOG.info('Starting metrics collector')
        now = utc_datetime()  # TODO: allow to get from somewhere

        for customer in self._mc.customer_service().i_get_customer(
            is_active=True
        ):
            _LOG.info(f'Collecting metrics for customer: {customer.name}')
            # TODO: merge metadata if there are multiple licenses
            metadata = self._ls.get_customer_metadata(customer.name)
            with MetricsContext(customer, metadata, now) as ctx:
                self.collect_metrics_for_customer(ctx)
            self._tenants_cache.clear()
            self._platforms_cache.clear()
        return {}
