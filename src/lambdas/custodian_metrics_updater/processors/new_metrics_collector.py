from datetime import datetime

from typing import Generator
from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant
from models.metrics import ReportMetrics
from modular_sdk.modular import Modular

from helpers.constants import Cloud, JobState, ReportType
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services import SP, modular_helpers
from services.ambiguous_job_service import AmbiguousJobService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.report_service import ReportService
from services.reports import (
    JobMetricsDataSource,
    ReportMetricsService,
    ShardsCollectionDataSource,
    ShardsCollectionProvider,
)

_LOG = get_logger(__name__)


class MetricsCollector:
    """
    We have multiple types of metrics that we must collect. Data that we can
    use to collect metrics:
    - all standard and event-driven jobs, their results and statistics for each
    - latest state of resources for each tenant
    - snapshots of resources state for each tenant with configured snapshot
      period. Default is 4 hours for onprem.
    - customers and tenants, applications, parents, etc. - models
    - rules index and some meta that is available locally - description,
      resource type
    - data from Cloud Custodian, only onprem
    - private rule metadata, (currently not clear)
    """

    def __init__(
        self,
        modular_client: Modular,
        ambiguous_job_service: AmbiguousJobService,
        mappings_collector: LazyLoadedMappingsCollector,
        report_service: ReportService,
        report_metrics_service: ReportMetricsService,
    ):
        self._mc = modular_client
        self._ajs = ambiguous_job_service
        self._mp = mappings_collector
        self._rs = report_service
        self._rms = report_metrics_service

        self._tenants_cache = {}
        self._reports = {}

    @classmethod
    def build(cls) -> 'MetricsCollector':
        return cls(
            modular_client=SP.modular_client,
            ambiguous_job_service=SP.ambiguous_job_service,
            mappings_collector=SP.mappings_collector,
            report_service=SP.report_service,
            report_metrics_service=SP.report_metrics_service,
        )

    def reset(self):
        self._tenants_cache.clear()
        self._reports.clear()

    @staticmethod
    def whole_period(
        now: datetime, /, *reports: ReportType
    ) -> tuple[datetime | None, datetime]:
        """
        Accepts the date where reports are collected and number of reports
        that should be collected. Returns the biggest whole period (start, end)
        that we need to collect these reports. Example:
        say today (date when which metrics are collected) is 2024-11-15,
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

    def collect_metrics_for_customer(self, customer: Customer, now: datetime):
        """
        Collects predefined hard-coded set of reports. Here we definitely know
        that all the reports are collected as of "now" date, so we can cache
        some sources of data and use lower-level metrics to calculate higher
        level
        """
        reports = (
            ReportType.OPERATIONAL_OVERVIEW,
            ReportType.OPERATIONAL_RESOURCES,
            ReportType.OPERATIONAL_RULES,
            ReportType.C_LEVEL_OVERVIEW,
        )

        start, end = self.whole_period(now, *reports)
        _LOG.info(f'Need to collect data from {start} to {end}')
        jobs = self._ajs.to_ambiguous(
            self._ajs.get_by_customer_name(
                customer_name=customer.name,
                start=start,
                end=end,  # todo here end must be not including
                ascending=True,  # important
            )
        )
        job_source = JobMetricsDataSource(jobs)
        if not job_source:
            _LOG.warning('No jobs for customer found')

        sc_provider = ShardsCollectionProvider(self._rs)
        all_reports = []

        _LOG.info('Generating operational overview for all tenants')
        all_reports.extend(
            self.operational_overview(
                now=now,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_OVERVIEW,
            )
        )

        _LOG.info('Generating operational resources for all tenants')
        all_reports.extend(
            self.operational_resources(
                now=now,
                job_source=job_source,
                sc_provider=sc_provider,
                report_type=ReportType.OPERATIONAL_RESOURCES,
            )
        )

        _LOG.info('Generating operational rules for all tenants')
        all_reports.extend(
            self.operational_rules(
                now=now,
                job_source=job_source,
                report_type=ReportType.OPERATIONAL_RULES,
            )
        )

        # todo operational compliance, attacks, rules, finops, kubernetes
        # todo project reports
        # todo tops
        # todo clevel

        if now.day == 1:  # assuming that we collect metrics once a day
            _LOG.info('Generating c-level overview for all tenants')
            all_reports.extend(
                self.c_level_overview(
                    customer=customer,
                    now=now,
                    job_source=job_source,
                    sc_provider=sc_provider,
                    report_type=ReportType.C_LEVEL_OVERVIEW,
                )
            )

        _LOG.info(f'Saving all reports items: {len(all_reports)}')
        self._rms.batch_save(all_reports)

    def operational_overview(
        self,
        now: datetime,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ):
        start = report_type.start(now)
        end = report_type.end(now)
        js = job_source.subset(start=start, end=end)
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
            sdc = ShardsCollectionDataSource(col, self._mp)
            data = {
                'total_scans': len(tjs),
                'failed_scans': tjs.n_failed,
                'succeeded_scans': tjs.n_succeeded,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
                'last_scan_date': tjs.last_scan_date,
                'id': tenant.project,
                'resources_violated': sdc.n_unique,
                'regions_severity': sdc.region_severities(unique=True),
            }
            item = self._rms.create(
                key=self._rms.key_for_tenant(report_type, tenant),
                data=data,
                end=end,
                start=start,
            )
            yield item

    def operational_resources(
        self,
        now: datetime,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ):
        # TODO: how to determine whether to include tenant in reporting.
        #  Currently it's based on job_source.scanned_tenants and since
        #  this reports does not have start bound job_source contains jobs
        #  for a bigger period of time
        start = report_type.start(now)
        end = report_type.end(now)
        js = job_source.subset(start=start, end=end)
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
                'data': ShardsCollectionDataSource(col, self._mp).resources(),
                'last_scan_date': js.subset(tenant=tenant.name).last_scan_date,
                'activated_regions': sorted(
                    modular_helpers.get_tenant_regions(tenant)
                ),
            }
            item = self._rms.create(
                key=self._rms.key_for_tenant(report_type, tenant),
                data=data,  # TODO: test whether it's ok to assign large objects to PynamoDB's MapAttribute
                end=end,
                start=start,
            )
            self._rms.save_data_to_s3(
                item
            )  # TODO: define whether to same based on some parameter or data size
            yield item

    def operational_rules(
        self,
        now: datetime,
        job_source: JobMetricsDataSource,
        report_type: ReportType,
    ) -> Generator[ReportMetrics, None, None]:
        start = report_type.start(now)
        end = report_type.end(now)
        js = job_source.subset(start=start, end=end)
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
                'last_scan_date': tjs.last_scan_date,
                'id': tenant.project,
                'data': list(
                    self._rs.average_statistics(
                        *map(self._rs.job_statistics, tjs)
                    )
                ),
            }
            item = self._rms.create(
                key=self._rms.key_for_tenant(report_type, tenant),
                data=data,
                end=end,
                start=start,
            )
            self._rms.save_data_to_s3(item)
            yield item

    def c_level_overview(
        self,
        customer: Customer,
        now: datetime,
        job_source: JobMetricsDataSource,
        sc_provider: ShardsCollectionProvider,
        report_type: ReportType,
    ):
        start = report_type.start(now)
        end = report_type.end(now)
        js = job_source.subset(start=start, end=end)
        cloud_tenant = {
            Cloud.AWS.value: [],
            Cloud.AZURE.value: [],
            Cloud.GOOGLE.value: [],
        }
        for tenant_name in js.scanned_tenants:
            tenant = self._get_tenant(tenant_name)
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            cloud_tenant.setdefault(tenant.cloud, []).append(tenant)
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
                sdc = ShardsCollectionDataSource(col, self._mp)
                self._update_dict_values(rt_data, sdc.resource_types())
                self._update_dict_values(sev_data, sdc.severities())
                total += sdc.n_unique
                used_tenants.append(tenant.name)

            tjs = js.subset(tenant=used_tenants)
            data[cloud] = {
                'failed_scans': tjs.n_failed,
                'last_scan_date': tjs.last_scan_date,
                'resources_violated': total,
                'resource_types_data': rt_data,
                'severity_data': sev_data,
                'succeeded_scans': tjs.n_succeeded,
                'total_scanned_tenants': len(used_tenants),
                'total_scans': len(tjs),
            }
        yield self._rms.create(
            key=self._rms.key_for_customer(report_type, customer.name),
            data=data,
            start=start,
            end=end,
        )

    def __call__(self):
        _LOG.info('Starting metrics collector')
        now = utc_datetime()  # todo get from somewhere

        for customer in self._mc.customer_service().i_get_customer(
            is_active=True
        ):
            _LOG.info(f'Collecting metrics for customer: {customer.name}')
            self.collect_metrics_for_customer(customer=customer, now=now)
        return {}
