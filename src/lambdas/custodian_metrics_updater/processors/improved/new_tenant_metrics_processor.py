from datetime import datetime, timezone, timedelta
import calendar
from functools import cmp_to_key
from pathlib import PurePosixPath
from typing import TypedDict, Generator
from dateutil.relativedelta import SU, relativedelta

from modular_sdk.models.customer import Customer
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular

from helpers import hashable
from helpers.constants import JobState, Severity, TACTICS_ID_MAPPING, Cloud, \
    GLOBAL_REGION
from helpers.log_helper import get_logger
from helpers.reports import keep_highest, SeverityCmp
from helpers.time_helper import utc_iso, utc_datetime, week_number
from models.batch_results import BatchResults
from models.rule import RuleIndex
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJobService, AmbiguousJob
from services.clients.s3 import S3Client
from services.coverage_service import CoverageService
from services.environment_service import EnvironmentService
from services.job_statistics_service import JobStatisticsService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.metrics_service import MetricsService
from services.platform_service import PlatformService
from services.report_service import ReportService
from services.reports_bucket import TenantReportsBucketKeysBuilder, \
    PlatformReportsBucketKeysBuilder, MetricsBucketKeysBuilder, StatisticsBucketKeysBuilder
from services.sharding import (ShardsCollectionFactory, ShardsS3IO,
                               ShardsCollection)

_LOG = get_logger(__name__)

CLOUDS = ['aws', 'azure', 'google']  # todo organize


class RegionData(TypedDict):
    resources: list[dict]


class PrettifiedFinding(TypedDict):
    policy: str
    resource_type: str
    description: str
    severity: str
    regions_data: dict[str, RegionData]


class ResourcesAndOverviewCollector:
    __slots__ = '_meta', '_mappings_collector', '_ms', '_resources', '_unique'

    def __init__(self, meta: dict,
                 mappings_collector: LazyLoadedMappingsCollector,
                 metrics_service: MetricsService):
        self._meta = meta  # raw meta from rules
        self._mappings_collector = mappings_collector
        self._ms = metrics_service

        self._resources = {}  # rule & region to list of unique resources
        self._unique = set()

    def reset(self):
        self._resources.clear()
        self._unique.clear()

    def add_resource(self, rule: str, region: str, resource: dict):
        """
        Assuming that there will be no duplicates within rule-region,
        because we handle it in the generator
        :param rule:
        :param region:
        :param resource:
        :return:
        """
        res = hashable(resource)
        self._unique.add(res)
        self._resources.setdefault(rule, {}).setdefault(
            region, set()).add(res)

    def _get_rule_service(self, rule: str) -> str:
        """
        Builds rule service from Cloud Custodian resource type. Last resort
        """
        return self._ms.adjust_resource_type(self._meta[rule]['resource']).capitalize()

    def resources(self) -> list[PrettifiedFinding]:
        result = []
        service = self._mappings_collector.service
        severity = self._mappings_collector.severity
        meta = self._meta
        for rule in self._resources:
            rm = meta.get(rule, {})
            item = {
                'policy': rule,
                'resource_type': service.get(rule) or self._get_rule_service(rule),
                'description': rm.get('description') or '',
                'severity': Severity.parse(severity.get(rule)).value,
                'regions_data': {}
            }
            for region, res in self._resources[rule].items():
                item['regions_data'][region] = {'resources': list(res)}
            result.append(item)
        return result

    def k8s_resources(self) -> list:
        result = []
        service = self._mappings_collector.service
        severity = self._mappings_collector.severity
        meta = self._meta
        for rule in self._resources:
            rm = meta.get(rule, {})
            item = {
                'policy': rule,
                'resource_type': service.get(rule) or self._get_rule_service(rule),
                'description': rm.get('description') or '',
                'severity': Severity.parse(severity.get(rule)).value,
                'resources': []
            }
            for region, res in self._resources[rule].items():
                item['resources'].extend(res)
            result.append(item)
        return result

    def region_severity(self, unique: bool = True) -> dict:
        """
        Returns something like this:
        {
            "eu-central-1": {
                "severity_data": {
                    "High": 123,
                    "Medium": 42
                }
            },
            "eu-west-1": {
                "severity_data": {
                    "High": 123,
                    "Medium": 42
                }
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
        :return:
        """
        region_severity = {}
        for rule in self._resources:
            severity = Severity.parse(self._mappings_collector.severity.get(rule)).value
            for region, res in self._resources[rule].items():
                region_severity.setdefault(region, {}).setdefault(
                    severity, set()).update(res)
        if unique:
            for region, data in region_severity.items():
                keep_highest(*[
                    data.get(k) for k in
                    sorted(data.keys(), key=cmp_to_key(SeverityCmp()))
                ])
        result = {}
        for region, data in region_severity.items():
            for severity, resources in data.items():
                d = result.setdefault(region, {'severity_data': {}})
                d['severity_data'].setdefault(severity, 0)
                d['severity_data'][severity] += len(resources)
        return result

    def attack_vector(self) -> list[dict]:
        # TODO REFACTOR IT
        temp = {}
        for rule in self._resources:
            attack_vector = self._mappings_collector.mitre.get(rule)
            if not attack_vector:
                _LOG.debug(f'Attack vector not found for {rule}. Skipping')
                continue
            severity = Severity.parse(self._mappings_collector.severity.get(rule)).value
            resource_type = self._mappings_collector.service.get(rule) or self._get_rule_service(rule)
            description = self._meta.get(rule, {}).get('description') or ''

            for region, res in self._resources[rule].items():
                for tactic, data in attack_vector.items():
                    for technique in data:
                        technique_name = technique.get('tn_name')
                        technique_id = technique.get('tn_id')
                        sub_techniques = list(
                            st['st_name'] for st in technique.get('st', [])
                        )
                        resources_data = [{
                            'resource': r, 'resource_type': resource_type,
                            'rule': description,
                            'severity': severity,
                            'sub_techniques': sub_techniques
                        } for r in res]
                        tactics_data = temp.setdefault(tactic, {
                            'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                            'techniques_data': {}
                        })
                        techniques_data = tactics_data[
                            'techniques_data'].setdefault(
                            technique_name, {
                                'technique_id': technique_id,
                                'regions_data': {}
                            }
                        )
                        regions_data = techniques_data[
                            'regions_data'].setdefault(
                            region, {'resources': []}
                        )
                        regions_data['resources'].extend(resources_data)
        resulting_dict = []

        for tactic, techniques in temp.items():
            item = {"tactic_id": techniques['tactic_id'], "tactic": tactic,
                    "techniques_data": []}
            for technique, data in techniques['techniques_data'].items():
                item['techniques_data'].append(
                    {**data, 'technique': technique})
            resulting_dict.append(item)

        return resulting_dict

    def k8s_attack_vector(self) -> list[dict]:
        # TODO REFACTOR IT
        temp = {}
        for rule in self._resources:
            attack_vector = self._mappings_collector.mitre.get(rule)
            if not attack_vector:
                _LOG.debug(f'Attack vector not found for {rule}. Skipping')
                continue
            severity = Severity.parse(self._mappings_collector.severity.get(rule)).value
            resource_type = self._mappings_collector.service.get(rule) or self._get_rule_service(rule)
            description = self._meta.get(rule, {}).get('description') or ''

            for region, res in self._resources[rule].items():
                for tactic, data in attack_vector.items():
                    for technique in data:
                        technique_name = technique.get('tn_name')
                        technique_id = technique.get('tn_id')
                        sub_techniques = list(
                            st['st_name'] for st in technique.get('st', [])
                        )
                        tactics_data = temp.setdefault(tactic, {
                            'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                            'techniques_data': {}
                        })
                        techniques_data = tactics_data[
                            'techniques_data'].setdefault(
                            technique_name, {
                                'technique_id': technique_id
                            }
                        )
                        resources_data = techniques_data.setdefault(
                            'resources', [])
                        resources_data.extend([{
                            'resource': r, 'resource_type': resource_type,
                            'rule': description,
                            'severity': severity,
                            'sub_techniques': sub_techniques
                        } for r in res])
        resulting_dict = []

        for tactic, techniques in temp.items():
            item = {"tactic_id": techniques['tactic_id'], "tactic": tactic,
                    "techniques_data": []}
            for technique, data in techniques['techniques_data'].items():
                item['techniques_data'].append(
                    {**data, 'technique': technique})
            resulting_dict.append(item)

        return resulting_dict

    def finops(self) -> list[dict]:
        # todo REFACTOR it
        service_resource_mapping = {}
        meta = self._meta
        for rule in self._resources:
            rm = meta.get(rule, {})
            comment = RuleIndex(rm.get('comment', ''))
            if comment.category and 'finops' not in comment.category.lower():
                # kind of fast skip. Anyway we lack data in rule index
                continue
            category = self._mappings_collector.category.get(rule, '')
            if 'FinOps' not in category:
                continue
            else:
                category = category.split('>')[-1].strip()

            service_section = comment.service_section or self._mappings_collector.service_section.get(rule)
            service_resource_mapping.setdefault(service_section,
                                                {'rules_data': []})

            severity = Severity.parse(self._mappings_collector.severity.get(rule)).value
            service = self._mappings_collector.service.get(rule) or self._get_rule_service(rule)
            rule_item = {"rule": rm.get('description', ''),
                         "service": service,
                         "category": category,
                         "severity": severity,
                         "resource_type": rm['resource'],
                         "regions_data": {}}
            # todo service, resource type?????

            for region, resources in self._resources[rule].items():
                rule_item['regions_data'].setdefault(
                    region, {'resources': []})
                for res in resources:
                    rule_item['regions_data'][region]['resources'].append(
                        res)

            service_resource_mapping[service_section]['rules_data'].append(
                rule_item)

        return [{'service_section': service_section, **data} for
                service_section, data in service_resource_mapping.items()]

    def len_of_unique(self) -> int:
        return len(self._unique)


class TenantMetrics:
    def __init__(self, modular: Modular,
                 ambiguous_job_service: AmbiguousJobService,
                 environment_service: EnvironmentService,
                 s3_client: S3Client,
                 metrics_service: MetricsService,
                 mappings_collector: LazyLoadedMappingsCollector,
                 coverage_service: CoverageService,
                 report_service: ReportService,
                 platform_service: PlatformService,
                 job_statistics_service: JobStatisticsService):
        self._mc = modular
        self._ajs = ambiguous_job_service
        self._env = environment_service
        self._s3 = s3_client
        self._ms = metrics_service
        self._mappings_collector = mappings_collector
        self._cs = coverage_service
        self._rs = report_service
        self._ps = platform_service
        self._jss = job_statistics_service

    @classmethod
    def build(cls) -> 'TenantMetrics':
        return cls(
            modular=SP.modular_client,
            ambiguous_job_service=SP.ambiguous_job_service,
            environment_service=SP.environment_service,
            s3_client=SP.s3,
            metrics_service=SP.metrics_service,
            mappings_collector=SP.mappings_collector,
            coverage_service=SP.coverage_service,
            report_service=SP.report_service,
            platform_service=SP.platform_service,
            job_statistics_service=SP.job_statistics_service
        )

    @staticmethod
    def _tenant_cloud(tenant: Tenant) -> str:
        if tenant.cloud.lower() == 'gcp': return 'google'
        return tenant.cloud.lower()

    def _build_base_tenant_data(self, tenant: Tenant, start: datetime,
                                end: datetime) -> dict:
        return {
            'overview': {
                'total_scans': 0,
                'failed_scans': 0,
                'succeeded_scans': 0,
                'resources_violated': 0,
                'regions_data': {}
            },  # tenant and platforms
            'customer': tenant.customer_name,
            'tenant_name': tenant.name,
            'id': str(tenant.project),
            'cloud': self._tenant_cloud(tenant),
            'activated_regions': sorted(modular_helpers.get_tenant_regions(tenant)),
            'from': utc_iso(start),
            'to': utc_iso(end),
            'outdated_tenants': {},
            'last_scan_date': None,
            'compliance': {  # tenant
                'regions_data': [],
                'average_data': []
            },
            'resources': [],  # tenant
            'attack_vector': [],  # tenant
            'rule': {},  # tenant and platforms
            'finops': [],  # tenant
            'kubernetes': {}  # platforms
        }

    def _initiate_resource_collector(self, collection: ShardsCollection,
                                     regions) -> ResourcesAndOverviewCollector:
        it = self._ms.create_resources_generator(
            collection, regions
        )
        col = ResourcesAndOverviewCollector(collection.meta, self._mappings_collector,
                                            metrics_service=self._ms)
        for rule, region, dto, _ in it:
            col.add_resource(rule, region, dto)
        return col

    def _get_tenant_compliance(self, cloud: Cloud,
                               collection: ShardsCollection) -> dict[str, list]:
        # todo implement new coverages with questionnaires
        coverage = self._cs.coverage_from_collection(
            collection, cloud=cloud
        )
        result_coverage = []
        summarized_coverage = {}
        for region, item in coverage.items():
            region_item = {'region': region, 'standards_data': []}
            for n, v in item.items():
                region_item['standards_data'].append({'name': n, 'value': v})
                if len(coverage) > 1:
                    summarized_coverage.setdefault(n, []).append(v)
            result_coverage.append(region_item)
        average_coverage = [{'name': k, 'value': round(sum(v) / len(v), 2)}
                            for k, v in summarized_coverage.items()]
        return {'regions_data': result_coverage,
                'average_data': average_coverage}

    def _get_tenant_ids_for(self, customer_name: str,
                            date: datetime) -> Generator[str, None, None]:
        it = self._s3.list_dir(
            bucket_name=self._env.get_metrics_bucket_name(),
            key=MetricsBucketKeysBuilder.list_customer_accounts_metrics_prefix(customer_name, date)
        )
        for key in it:
            yield self.tenant_id_from_key(key)

    def _process_customer_jobs(self, customer: Customer,
                               start: datetime, end: datetime,
                               jobs: tuple[AmbiguousJob, ...]):
        tenant_jobs: dict[str, list[AmbiguousJob]] = {}
        for job in jobs:
            tenant_jobs.setdefault(job.tenant_name, []).append(job)
        all_tenants = []
        for tenant_name, jobs in tenant_jobs.items():
            tenant = self._mc.tenant_service().get(tenant_name)
            # ignoring whether it's active or not here
            if not tenant:
                _LOG.warning(f'Tenant with name {tenant_name} not found!')
                continue
            all_tenants.append(tenant)
            base = self._build_base_tenant_data(tenant, start, end)
            base['last_scan_date'] = jobs[0].submitted_at  # including platforms

            for job in jobs:
                base['overview']['total_scans'] += 1
                match job.status:
                    case JobState.FAILED:
                        base['overview']['failed_scans'] += 1
                    case JobState.SUCCEEDED:
                        base['overview']['succeeded_scans'] += 1
                    case _:
                        _LOG.error('Never happen')  # because only finished jobs are passed here
            builder = TenantReportsBucketKeysBuilder(tenant)
            key = builder.latest_key()
            collection = ShardsCollectionFactory.from_tenant(tenant)
            collection.io = ShardsS3IO(
                bucket=self._env.default_reports_bucket_name(),
                key=key,
                client=self._s3
            )
            collection.fetch_all()
            collection.fetch_meta()

            collector = self._initiate_resource_collector(
                collection, modular_helpers.get_tenant_regions(tenant)  # todo use oll regions?
            )
            _LOG.info('Collecting resources')
            base['resources'] = collector.resources()
            _LOG.info('Collecting attacks vectors')
            base['attack_vector'] = collector.attack_vector()
            _LOG.info('Expanding overview')
            base['overview']['resources_violated'] = collector.len_of_unique()
            base['overview']['regions_data'] = collector.region_severity()
            _LOG.info('collecting coverages')
            base['compliance'] = self._get_tenant_compliance(
                cloud=modular_helpers.tenant_cloud(tenant),
                collection=collection
            )
            _LOG.info('Collecting finops')
            base['finops'] = collector.finops()

            _LOG.info('Collecting rules metrics')
            average = self._rs.average_statistics(*map(
                self._rs.job_statistics, filter(lambda x: x.status == JobState.SUCCEEDED, jobs)
            ))  # todo download in threads?
            base['rule'] = {
                'rules_data': list(average),
                'violated_resources_length': collector.len_of_unique()
            }

            _LOG.info('Collecting kubernetes data')
            platform_jobs: dict[str, list[AmbiguousJob]] = {}
            for job in jobs:
                if not job.is_platform_job:
                    continue
                platform_jobs.setdefault(job.platform_id, []).append(job)
            for platform_id, pl_jobs in platform_jobs.items():
                platform = self._ps.get_nullable(platform_id)
                if not platform:
                    _LOG.warning(f'Platform with id {platform_id} not found')
                    continue
                builder = PlatformReportsBucketKeysBuilder(platform)
                k8s_key = builder.latest_key()
                k8s_collection = ShardsCollectionFactory.from_cloud(
                    Cloud.KUBERNETES)
                k8s_collection.io = ShardsS3IO(
                    bucket=self._env.default_reports_bucket_name(),
                    key=k8s_key,
                    client=self._s3
                )
                k8s_collection.fetch_all()
                k8s_collection.fetch_meta()

                k8s_collector = self._initiate_resource_collector(collection, [])
                compliance = self._get_tenant_compliance(
                    Cloud.KUBERNETES, k8s_collection).get('regions_data', [])
                if not compliance:
                    k8s_compliance_data = []
                else:
                    k8s_compliance_data = compliance[0].get('standards_data', [])
                # resources
                base['kubernetes'][platform_id] = {
                    'region': platform.region,
                    'last_scan_date': pl_jobs[0].submitted_at,
                    'policy_data': k8s_collector.k8s_resources(),
                    'mitre_data': k8s_collector.k8s_attack_vector(),
                    'compliance_data': k8s_compliance_data
                }

            _LOG.info('Saving tenant metrics to s3')
            self._s3.gz_put_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenant).account_metrics(end),  # todo date of last day?
                obj=base
            )
            _LOG.info('Saving monthly metrics to s3')  # todo monthly seems not monthly, must be fixed
            self._s3.gz_put_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenant).account_monthly_metrics(end),
                obj=base
            )

            if utc_datetime().day == 1:
                _LOG.info('Saving monthly rule statistics')
                self._save_monthly_rule_statistics(
                    start=start,
                    end=end,
                    tenant_obj=tenant,
                    rule_data=base['rule'].get('rules_data', [])
                )

        _LOG.info('Saving weekly job stats')
        # TODO these stats MUST be refactored. I just moved that code and actually cannot copletely comprehend its logic
        if utc_datetime().day == 1:
            self.save_weekly_job_stats(
                customer=customer,
                tenant_objects={t.name: t for t in all_tenants},
                start=start,
                end=end,
                end_date=utc_datetime().date().isoformat()
            )
        else:
            self.save_weekly_job_stats(
                customer=customer.name,
                tenant_objects={t.name: t for t in all_tenants},
                start=start,
                end=end,
            )

        _LOG.info('Copy metrics for tenants that have not been scaneed this week')  # todo is it ok?
        current_ids = {tenant.project for tenant in all_tenants}
        previous_ids = set(self._get_tenant_ids_for(customer.name, start))
        for tenant_id in previous_ids - current_ids:
            tenant = next(self._mc.tenant_service().i_get_by_acc(
                acc=tenant_id,
                limit=1
            ), None)
            if not tenant:
                _LOG.warning(f'Cannot find tenant with id {tenant_id}')
                continue
            previous_data = self._s3.gz_get_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenant).account_metrics(start)
            )
            # todo old metrics updated this data with latest tenant compliance, resources, finops and attacks in case old is empty. I don't do that
            if not previous_data.setdefault('outdated_tenants', {}):
                previous_data['outdated_tenants'][previous_data['cloud']] = {
                    previous_data['tenant_name']: previous_data['to']
                }
            previous_data['from'] = utc_iso(start)
            previous_data['to'] = utc_iso(end)
            previous_data['overview'] = {
                'total_scans': 0,
                'failed_scans': 0,
                'succeeded_scans': 0
            }
            if not previous_data['overview'].get('regions_data'):
                previous_data['overview']['regions_data'] = {
                    GLOBAL_REGION: {
                        'severity_data': {}
                    }
                }
            self._s3.gz_put_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenant).account_metrics(end),
                obj=previous_data
            )

    @staticmethod
    def get_current_week_boundaries() -> tuple[datetime, datetime]:
        """
        Use Sundays 00:00:00 for now
        """
        now = utc_datetime()
        start = now + relativedelta(hour=0, minute=0, second=0, microsecond=0, weekday=SU(-1))
        end = now + relativedelta(hour=0, minute=0, second=0, microsecond=0, weekday=SU(+1))
        return start, end

    @staticmethod
    def tenant_id_from_key(key: str) -> str:
        return PurePosixPath(key).name.split('.')[0]

    def process_data(self, event):
        start, end = self.get_current_week_boundaries()

        self.weekly_scan_statistics = {}  # todo refactor

        for customer in self._mc.customer_service().i_get_customer(is_active=True):
            _LOG.info(f'Collecting jobs for customer {customer.name}')
            jobs = self._ajs.to_ambiguous(self._ajs.get_by_customer_name(
                customer_name=customer.name,
                start=start,
                end=end
            ))
            jobs = filter(lambda j: j.is_finished(), jobs)

            self._process_customer_jobs(
                customer=customer,
                jobs=tuple(jobs),
                start=start,
                end=end
            )
        for cid, data in self.weekly_scan_statistics.items():  # todo refactor
            _LOG.debug(f'Saving weekly statistics for customer {cid}')
            for c in CLOUDS:
                dict_to_save = data.get(c)
                if not dict_to_save:
                    continue
                dict_to_save['customer_name'] = cid
                dict_to_save['cloud'] = c
                dict_to_save['tenants'] = dict_to_save.get('tenants', {})
                self._jss.save(dict_to_save)

        return {
            'data_type': 'tenant_groups',
            # todo end_date,
            # todo pass period which we use to collect data
            'continuously': event.get('continuously')  # todo for what
        }

    # todo refactor each method below this line

    def save_weekly_job_stats(self, customer, tenant_objects,
                              start: datetime, end: datetime,
                              end_date: str = None):
        def append_time_period(new_start_date, new_end_date):
            nonlocal time_periods
            time_periods.append((new_start_date, new_end_date))

        if not end_date:
            start_date = (start - timedelta(days=7)).date()  # todo???
        else:
            start_date = start.date()
        time_periods = []
        end_date = utc_datetime(end_date).date() if end_date else start.date()

        if end_date.month != start_date.month:
            # split data from previous month and current
            # if the week is at the junction of months
            append_time_period(start_date, start_date.replace(
                day=calendar.monthrange(start_date.year, start_date.month)[1]))
            start_date = start_date.replace(month=end_date.month, day=1)

        append_time_period(start_date, end_date)
        _LOG.warning(
            f'Saving weekly scans statistics for period {time_periods}')

        for start_date, end_date in time_periods:
            _LOG.warning(f'Start date: {start_date} {type(start_date)}')
            _LOG.warning(f'End date: {end_date} {type(end_date)}')
            start_date = start_date.isoformat()
            end_date = end_date.isoformat()
            if self._jss.get_by_customer_and_date(
                    customer, start_date, end_date):
                continue

            scans = self._ajs.get_by_customer_name(
                customer_name=customer,
                start=utc_datetime(start_date),
                end=utc_datetime(end_date)
            )

            self.weekly_scan_statistics.setdefault(customer, {}). \
                setdefault('customer_name', customer)
            weekly_stats = self.weekly_scan_statistics[customer]
            for c in CLOUDS:
                weekly_stats.setdefault(c, {})
                weekly_stats[c]['from_date'] = start_date
                weekly_stats[c]['to_date'] = end_date
                weekly_stats[c]['failed'] = 0
                weekly_stats[c]['succeeded'] = 0

            for scan in scans:
                name = scan.tenant_name
                if not (tenant_obj := tenant_objects.get(name)):
                    tenant_obj = self._mc.tenant_service().get(name)
                    if not tenant_obj or not tenant_obj.project:
                        _LOG.warning(f'Cannot find tenant {name}. Skipping...')
                        continue

                cloud = tenant_obj.cloud.lower()
                weekly_stats[cloud].setdefault('scanned_regions', {}). \
                    setdefault(tenant_obj.project, {})
                weekly_stats[cloud].setdefault('tenants', {}).setdefault(
                    tenant_obj.project, {'failed_scans': 0,
                                         'succeeded_scans': 0})
                if scan.status == JobState.FAILED.value:
                    weekly_stats[cloud]['failed'] += 1
                    weekly_stats[cloud]['tenants'][tenant_obj.project][
                        'failed_scans'] += 1
                    reason = getattr(scan, 'reason', None)
                    if reason:
                        weekly_stats[cloud].setdefault('reason', {}).setdefault(
                            tenant_obj.project, {}).setdefault(reason, 0)
                        weekly_stats[cloud]['reason'][tenant_obj.project][
                            reason] += 1
                elif scan.status == JobState.SUCCEEDED.value:
                    weekly_stats[cloud]['tenants'][tenant_obj.project][
                        'succeeded_scans'] += 1
                    weekly_stats[cloud]['succeeded'] += 1

                if scan.Meta.table_name == BatchResults.Meta.table_name:
                    regions = list(scan.regions_to_rules().keys()) or []
                else:
                    regions = scan.regions or []
                for region in regions:
                    weekly_stats[cloud]['scanned_regions'][tenant_obj.project].setdefault(region, 0)
                    weekly_stats[cloud]['scanned_regions'][tenant_obj.project][region] += 1
                weekly_stats[cloud]['last_scan_date'] = self._get_last_scan_date(
                    scan.submitted_at, weekly_stats[cloud].get('last_scan_date'))

    @staticmethod
    def _get_last_scan_date(new_scan_date: str, last_scan_date: str = None):
        if not last_scan_date:
            return new_scan_date
        last_scan_datetime = utc_datetime(last_scan_date, utc=False)
        scan_datetime = utc_datetime(new_scan_date, utc=False)
        if last_scan_datetime < scan_datetime:
            return new_scan_date
        return last_scan_date

    def _save_monthly_rule_statistics(self, start: datetime, end: datetime,
                                      tenant_obj: Tenant, rule_data):
        today_date = utc_datetime()
        date_to_process = end.date()
        if today_date.date().day == 1:
            self._s3.gz_put_json(
                bucket=self._env.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    today_date.date() - timedelta(days=1),
                    tenant=tenant_obj),
                obj=rule_data)
        elif week_number(date_to_process) != 1:
            self._s3.gz_put_json(
                bucket=self._env.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    date_to_process, tenant=tenant_obj),
                obj=rule_data)
        else:  # if week does not fully belong to the current month
            jobs = self._ajs.get_by_tenant_name(
                tenant_name=tenant_obj.name,
                start=today_date.replace(day=1),
                end=end,
                status=JobState.SUCCEEDED,
            )
            average = self._rs.average_statistics(*map(
                self._rs.job_statistics, jobs
            ))
            self._s3.gz_put_json(
                bucket=self._env.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    date_to_process, tenant=tenant_obj), obj=list(average))


TENANT_METRICS = TenantMetrics.build()
