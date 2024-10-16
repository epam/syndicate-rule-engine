import calendar
import json
from datetime import datetime, timedelta
from functools import cmp_to_key
from typing import List, Dict, TypedDict, Optional

from dateutil.relativedelta import relativedelta, SU
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular

from helpers import get_logger, hashable
from helpers.constants import \
    RULE_TYPE, OVERVIEW_TYPE, RESOURCES_TYPE, CLOUD_ATTR, CUSTOMER_ATTR, \
    SUCCEEDED_SCANS_ATTR, FAILED_SCANS_ATTR, JobState, \
    TOTAL_SCANS_ATTR, LAST_SCAN_DATE, COMPLIANCE_TYPE, TENANT_NAME_ATTR, \
    ID_ATTR, ATTACK_VECTOR_TYPE, DATA_TYPE, TACTICS_ID_MAPPING, END_DATE, \
    FINOPS_TYPE, ARCHIVE_PREFIX, OUTDATED_TENANTS, KUBERNETES_TYPE, Cloud
from helpers.reports import keep_highest, severity_cmp, merge_dictionaries
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_datetime, week_number
from models.batch_results import BatchResults
from services import SERVICE_PROVIDER
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJobService, AmbiguousJob
from services.clients.s3 import S3Client
from services.coverage_service import CoverageService
from services.environment_service import EnvironmentService
from services.job_statistics_service import JobStatisticsService
from services.license_service import LicenseService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.metrics_service import MetricsService
from services.platform_service import PlatformService
from services.report_service import ReportService
from services.reports_bucket import TenantReportsBucketKeysBuilder, \
    PlatformReportsBucketKeysBuilder, StatisticsBucketKeysBuilder
from services.setting_service import SettingsService
from services.sharding import (ShardsCollectionFactory, ShardsS3IO,
                               ShardsCollection)

_LOG = get_logger(__name__)

TENANT_METRICS_FILE_PATH = '{customer}/accounts/{date}/{project_id}.json'
NEXT_STEP = 'tenant_groups'
CLOUDS = ['aws', 'azure', 'google']


class RegionData(TypedDict):
    resources: List[Dict]


class PrettifiedFinding(TypedDict):
    policy: str
    resource_type: str
    description: str
    severity: str
    regions_data: Dict[str, RegionData]


class TenantMetrics:

    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 s3_client: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 modular_client: Modular,
                 coverage_service: CoverageService,
                 metrics_service: MetricsService,
                 mappings_collector: LazyLoadedMappingsCollector,
                 job_statistics_service: JobStatisticsService,
                 license_service: LicenseService,
                 report_service: ReportService,
                 platform_service: PlatformService):
        self.ambiguous_job_service = ambiguous_job_service
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.modular_client = modular_client
        self.coverage_service = coverage_service
        self.mappings_collector = mappings_collector
        self.metrics_service = metrics_service
        self.job_statistics_service = job_statistics_service
        self.license_service = license_service
        self.report_service = report_service
        self.platform_service = platform_service

        self.today_date = datetime.today()
        self.today_midnight = datetime.combine(self.today_date,
                                               datetime.min.time())

        self._date_marker = self.settings_service.get_report_date_marker()
        self.current_week_date = self._date_marker.get('current_week_date')
        self.last_week_date = self._date_marker.get('last_week_date')
        self.yesterday = (self.today_date - timedelta(days=1)).date()
        self.next_month_date = (self.today_date.date().replace(day=1) +
                                relativedelta(months=1)).isoformat()
        self.month_first_day = datetime.combine(
            self.today_date.replace(day=1), datetime.min.time())
        self.month_first_day_iso = self.month_first_day.date().isoformat()

        # won't cross bounds
        self.month_last_day = self.today_date.date() + relativedelta(day=31)

        self.end_date = None
        self.start_date = None
        self.TO_UPDATE_MARKER = False
        self.weekly_scan_statistics = {}

    @classmethod
    def build(cls) -> 'TenantMetrics':
        return cls(
            environment_service=SERVICE_PROVIDER.environment_service,
            s3_client=SERVICE_PROVIDER.s3,
            ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            modular_client=SERVICE_PROVIDER.modular_client,
            coverage_service=SERVICE_PROVIDER.coverage_service,
            mappings_collector=SERVICE_PROVIDER.mappings_collector,
            metrics_service=SERVICE_PROVIDER.metrics_service,
            job_statistics_service=SERVICE_PROVIDER.job_statistics_service,
            license_service=SERVICE_PROVIDER.license_service,
            report_service=SERVICE_PROVIDER.report_service,
            platform_service=SERVICE_PROVIDER.platform_service
        )

    class ResourcesAndOverviewCollector:
        def __init__(self, meta: dict,
                     mappings_collector: LazyLoadedMappingsCollector):
            self._meta = meta  # raw meta from rules
            self._mappings_collector = mappings_collector

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

        def resources(self) -> List[PrettifiedFinding]:
            result = []
            service = self._mappings_collector.service
            severity = self._mappings_collector.severity
            meta = self._meta
            for rule in self._resources:
                item = {
                    "policy": rule,
                    "resource_type": service.get(rule),
                    "description": meta.get(rule).get('description') or '',
                    "severity": severity.get(rule),
                    "regions_data": {}
                }
                for region, res in self._resources[rule].items():
                    item['regions_data'][region] = {'resources': list(res)}
                result.append(item)
            return result

        def k8s_resources(self) -> List:
            result = []
            service = self._mappings_collector.service
            severity = self._mappings_collector.severity
            meta = self._meta
            for rule in self._resources:
                item = {
                    "policy": rule,
                    "resource_type": service.get(rule),
                    "description": meta.get(rule).get('description') or '',
                    "severity": severity.get(rule),
                    "resources": []
                }
                for region, res in self._resources[rule].items():
                    item['resources'].extend(list(res))
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
            In case where is a resource which violates different rules with
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
                severity = self._mappings_collector.severity.get(rule)
                for region, res in self._resources[rule].items():
                    region_severity.setdefault(region, {}).setdefault(
                        severity, set()).update(res)
            if unique:
                for region, data in region_severity.items():
                    keep_highest(*[
                        data.get(k) for k in
                        sorted(data.keys(), key=cmp_to_key(severity_cmp))
                    ])
            result = {}
            for region, data in region_severity.items():
                for severity, resources in data.items():
                    d = result.setdefault(region, {'severity_data': {}})
                    d['severity_data'].setdefault(severity, 0)
                    d['severity_data'][severity] += len(resources)
            return result

        def attack_vector(self) -> List[Dict]:
            # TODO REFACTOR IT
            temp = {}
            for rule in self._resources:
                attack_vector = self._mappings_collector.mitre.get(rule)
                if not attack_vector:
                    _LOG.debug(f'Attack vector not found for {rule}. Skipping')
                    continue
                severity = self._mappings_collector.severity.get(rule)
                resource_type = self._mappings_collector.service.get(rule)
                description = self._meta.get(rule).get('description') or ''

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

        def k8s_attack_vector(self) -> List[Dict]:
            # TODO REFACTOR IT
            temp = {}
            for rule in self._resources:
                attack_vector = self._mappings_collector.mitre.get(rule)
                if not attack_vector:
                    _LOG.debug(f'Attack vector not found for {rule}. Skipping')
                    continue
                severity = self._mappings_collector.severity.get(rule)
                resource_type = self._mappings_collector.service.get(rule)
                description = self._meta.get(rule).get('description') or ''

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

        def finops(self):
            service_resource_mapping = {}
            for rule in self._resources:
                category = self._mappings_collector.category.get(rule, '')
                if 'FinOps' not in category:
                    continue
                else:
                    category = category.split('>')[-1].strip()

                service_section = self._mappings_collector.service_section.get(
                    rule)
                service_resource_mapping.setdefault(service_section,
                                                    {'rules_data': []})

                description = self._meta.get(rule).get('description') or ''
                severity = self._mappings_collector.severity.get(rule)
                service = self._mappings_collector.service.get(rule)
                resource_type = self._mappings_collector.service.get(rule)
                rule_item = {"rule": description,
                             "service": service,
                             "category": category,
                             "severity": severity,
                             "resource_type": resource_type,
                             "regions_data": {}}

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

    def gz_put_json(self, bucket: str, key: str, obj: dict | list):
        """
        Valid json cannot have null as key though `json.dumps` can dump None
        key and converts it to "null" strings. Msgspec does not do that.
        Metrics can have None as their key so here I use `json.dumps` instead of
        msgspec
        :param bucket:
        :param key:
        :param obj:
        :return:
        """
        return self.s3_client.gz_put_object(
            bucket=bucket,
            key=key,
            body=json.dumps(obj, separators=(',', ':')).encode(),
            content_type='application/json',
            content_encoding='gzip'
        )

    def process_data(self, event):
        """ Collect data about scans of each tenant
        (AWS account/Azure subscription/GCP project) separately.
        Account == tenant """
        result_tenant_data = {}
        customer_to_tenants_mapping = {}
        tenant_objects = {}
        tenant_to_job_mapping = {}
        tenant_last_job_mapping = {}
        missing_tenants = {}
        metrics_bucket: str = self.environment_service.get_metrics_bucket_name()
        self.end_date: Optional[str] = event.get(END_DATE)

        if self.end_date:
            end_datetime = utc_datetime(self.end_date, utc=False)

            if end_datetime < self.today_date.astimezone():
                end_date_datetime = end_datetime
                weekday = end_date_datetime.weekday()
                s3_object_date = self.end_date if weekday == 6 else (
                        end_date_datetime + timedelta(days=6 - weekday)). \
                    date().isoformat()
                self.next_month_date = (
                        end_date_datetime.replace(day=1) +
                        relativedelta(months=1)).date().isoformat()

                su_number = -1 if weekday != 6 else -2
                self.start_date = (end_date_datetime + relativedelta(
                    weekday=SU(su_number))).date()
                self.end_date = end_date_datetime.date()
            else:
                self.start_date, s3_object_date, self.end_date = self._default_dates()
        else:
            self.start_date, s3_object_date, self.end_date = self._default_dates()

        # get all scans for each of existing customer
        _LOG.debug(f'Retrieving jobs between {self.start_date} and '
                   f'{self.end_date} dates')
        for customer in self.modular_client.customer_service().i_get_customer():
            tenant_objects = {}  # todo report shadowing another var with this name
            if customer == SYSTEM_CUSTOMER:  # TODO report different types
                _LOG.debug('Skipping system customer')
                continue
            jobs = self.ambiguous_job_service.get_by_customer_name(
                customer_name=customer.name,
                start=datetime.combine(self.start_date, datetime.min.time()),
                end=datetime.combine(self.end_date, datetime.now().time()),
                limit=100  # TODO report what if there're more
            )
            customer_to_tenants_mapping.setdefault(  # TODO report customer_to_jobs_mapping
                customer.name, []).extend(jobs)

        for customer, jobs in customer_to_tenants_mapping.items():
            current_platforms = {}
            current_accounts = set(job.tenant_name for job in jobs)

            missing = self._check_not_scanned_tenants(
                prev_key=f'{customer}/accounts/{self.last_week_date}/',
                current_accounts=current_accounts)
            _LOG.debug(f'Not scanned accounts within {customer} customer for '
                       f'this week: {missing}')
            for project in missing:
                if not project:
                    _LOG.debug(f'Somehow non-existing missing: "{project}"')
                    continue
                tenant_obj = list(self.modular_client.tenant_service().i_get_by_acc(
                    project, active=True  # todo report limit 1
                ))
                if not tenant_obj:
                    _LOG.warning(f'Cannot find tenant with id {project}. '
                                 f'Skipping...')
                    continue
                if len(tenant_obj) > 1:
                    _LOG.warning(
                        f'There is more than 1 tenant with the project id '
                        f'\'{project}\'. Processing the first one')

                tenant_obj = tenant_obj[0]
                missing_tenants[project] = tenant_obj

            for job in jobs:
                name = job.tenant_name
                if AmbiguousJob(job).is_platform_job:
                    last_scan = current_platforms.setdefault(name, {}).\
                        setdefault(job.platform_id)
                    if not last_scan or last_scan < job.submitted_at:
                        current_platforms[name][job.platform_id] = \
                            job.submitted_at

                result_tenant_data.setdefault(name, {OVERVIEW_TYPE: {
                    TOTAL_SCANS_ATTR: 0,
                    FAILED_SCANS_ATTR: 0,
                    SUCCEEDED_SCANS_ATTR: 0}
                })
                tenant_overview = result_tenant_data[name][OVERVIEW_TYPE]
                tenant_overview[TOTAL_SCANS_ATTR] += 1
                if job.status == JobState.FAILED.value:
                    tenant_overview[FAILED_SCANS_ATTR] += 1
                    continue
                elif job.status == JobState.SUCCEEDED.value:
                    tenant_overview[SUCCEEDED_SCANS_ATTR] += 1
                else:
                    _LOG.warning(f'Unknown scan status: {job.status}; '
                                 f'scan {job.id}')  # TODO report unknown status is still a status. Currently n_failed + n_succeeded != n_total

                if name not in tenant_objects:
                    tenant_obj = self.modular_client.tenant_service().get(name)
                    if not tenant_obj or not tenant_obj.project:
                        _LOG.warning(f'Cannot find tenant {name}. Skipping...')
                        continue
                    tenant_objects[name] = tenant_obj

                tenant_to_job_mapping.setdefault(name, []).append(job)
                last_job = tenant_last_job_mapping.setdefault(name, job)
                if last_job.submitted_at < job.submitted_at:
                    tenant_last_job_mapping[name] = job

            today_date = self.today_date.date().isoformat()
            if tenant_objects and not event.get(END_DATE):
                if today_date == self.month_first_day_iso:
                    self.save_weekly_job_stats(customer, tenant_objects,
                                               end_date=today_date)
                else:
                    self.save_weekly_job_stats(customer, tenant_objects)

        if not tenant_objects:
            _LOG.warning(
                f'No jobs for period {self.start_date} to {self.end_date}')

        for name, tenant_obj in tenant_objects.items():
            cloud = 'google' if tenant_obj.cloud.lower() == 'gcp' \
                else tenant_obj.cloud.lower()
            identifier = tenant_obj.project
            active_regions = list(modular_helpers.get_tenant_regions(tenant_obj))
            _LOG.debug(f'Processing \'{name}\' tenant with id {identifier} '
                       f'and active regions: {", ".join(active_regions)}')
            # general account info
            result_tenant_data.setdefault(name, {}).update({
                CUSTOMER_ATTR: tenant_obj.customer_name,
                TENANT_NAME_ATTR: tenant_obj.name,
                ID_ATTR: tenant_obj.project,
                CLOUD_ATTR: cloud,
                'activated_regions': active_regions,
                'from': self.start_date.isoformat(),
                'to': self.end_date.isoformat(),
                OUTDATED_TENANTS: {},
                LAST_SCAN_DATE: tenant_last_job_mapping[name].submitted_at
            })

            builder = TenantReportsBucketKeysBuilder(tenant_obj)
            if event.get(END_DATE):
                key = (builder.nearest_snapshot_key(self.end_date)
                       or builder.latest_key())
            else:
                key = builder.latest_key()

            collection = ShardsCollectionFactory.from_tenant(tenant_obj)
            collection.io = ShardsS3IO(
                bucket=self.environment_service.default_reports_bucket_name(),
                key=key,
                client=self.s3_client
            )
            collection.fetch_all()
            collection.fetch_meta()

            merge_dictionaries(self._collect_tenant_metrics(collection, tenant_obj),
                               result_tenant_data[name])
            # k8s cluster
            result_tenant_data[name].setdefault(KUBERNETES_TYPE, {})
            platforms = current_platforms.get(tenant_obj.name, {})
            for platform_id, platform_last_scan in platforms.items():
                platform = self.platform_service.get_nullable(platform_id)
                if not platform:
                    _LOG.debug(f'Skipping platform with id {platform_id}: '
                               f'cannot find item with such id')
                    continue
                self.platform_service.fetch_application(platform)

                builder = PlatformReportsBucketKeysBuilder(platform)
                if event.get(END_DATE):
                    k8s_key = (builder.nearest_snapshot_key(self.end_date)
                               or builder.latest_key())
                else:
                    k8s_key = builder.latest_key()

                k8s_collection = ShardsCollectionFactory.from_cloud(
                    Cloud.KUBERNETES)
                k8s_collection.io = ShardsS3IO(
                    bucket=self.environment_service.default_reports_bucket_name(),
                    key=k8s_key,
                    client=self.s3_client
                )
                k8s_collection.fetch_all()
                k8s_collection.fetch_meta()

                result_tenant_data[name][KUBERNETES_TYPE].setdefault(
                    platform.id, self._collect_k8s_metrics(k8s_collection))
                result_tenant_data[name][KUBERNETES_TYPE][platform.id].update({
                    'region': platform.region,
                    'last_scan_date': platform_last_scan
                })

            # saving to s3
            _LOG.debug(f'Saving metrics of {tenant_obj.name} tenant to '
                       f'{metrics_bucket}')
            if not event.get(END_DATE) and \
                    self.today_date.date().isoformat() == self.month_first_day_iso \
                    and not self._is_tenant_active(tenant_obj):
                identifier = f'{ARCHIVE_PREFIX}-{identifier}'

            self.gz_put_json(
                bucket=metrics_bucket,
                key=TENANT_METRICS_FILE_PATH.format(
                    customer=tenant_obj.customer_name,
                    date=s3_object_date, project_id=identifier),
                obj=result_tenant_data.get(name)
            )
            if identifier.startswith(ARCHIVE_PREFIX):
                _LOG.debug(
                    f'Deleting non-archive metrics for tenant {tenant_obj.project}')
                self.s3_client.gz_delete_object(
                    bucket=metrics_bucket,
                    key=TENANT_METRICS_FILE_PATH.format(
                        customer=tenant_obj.customer_name,
                        date=s3_object_date,
                        project_id=tenant_obj.project
                    )
                )

            if not identifier.startswith(ARCHIVE_PREFIX):
                if not event.get(END_DATE) or calendar.monthrange(
                        self.end_date.year, self.end_date.month)[1] == \
                        self.end_date.day:
                    self._save_monthly_state(result_tenant_data.get(name),
                                             identifier,
                                             tenant_obj.customer_name)
                if self.TO_UPDATE_MARKER:
                    _LOG.debug(f'Saving metrics of {tenant_obj.name} for current '
                               f'date')
                    s3_object_date = (self.today_date + relativedelta(
                        weekday=SU(0))).date().isoformat()
                    self.gz_put_json(
                        bucket=metrics_bucket,
                        key=TENANT_METRICS_FILE_PATH.format(
                            customer=tenant_obj.customer_name,
                            date=s3_object_date,
                            project_id=identifier),
                        obj=result_tenant_data.get(name)
                    )

            if not event.get(END_DATE) and \
                    (self.today_date.date().isoformat() ==
                     self.month_first_day_iso or self.TO_UPDATE_MARKER):
                self._save_monthly_rule_statistics(
                    tenant_obj,
                    result_tenant_data[name][RULE_TYPE].get('rules_data', []))
            # to free memory
            result_tenant_data.pop(name)

        for cid, data in self.weekly_scan_statistics.items():
            _LOG.debug(f'Saving weekly statistics for customer {cid}')
            for c in CLOUDS:
                dict_to_save = data.get(c)
                if not dict_to_save:
                    continue
                dict_to_save['customer_name'] = cid
                dict_to_save['cloud'] = c
                dict_to_save['tenants'] = dict_to_save.get('tenants', {})
                self.job_statistics_service.save(dict_to_save)

        _LOG.debug(
            'Copy metrics for tenants that haven\'t been scanned this week')
        for tenant, obj in missing_tenants.items():
            tenant_obj = None
            filename = obj.project
            if not event.get(END_DATE):
                if self.today_date.date() == self.month_first_day_iso:
                    tenant_obj = next(self.modular_client.tenant_service().i_get_by_acc(
                        obj.project
                    ), None)
                    if not self._is_tenant_active(tenant_obj):
                        filename = f'{ARCHIVE_PREFIX}-{obj.project}'
                elif self.today_date.weekday() != 0 and self.s3_client.gz_object_exists(
                        metrics_bucket, TENANT_METRICS_FILE_PATH.format(
                            customer=obj.customer_name,
                            date=self.current_week_date,
                            project_id=obj.project)):
                    continue

            file_path = TENANT_METRICS_FILE_PATH.format(
                customer=obj.customer_name,
                date=self.start_date.isoformat(), project_id=obj.project)
            file_content = self.s3_client.gz_get_json(
                bucket=metrics_bucket,
                key=file_path
            )
            if not file_content:
                _LOG.warning(f'Cannot find file {file_path}')
                continue

            required_types = [FINOPS_TYPE, RESOURCES_TYPE, COMPLIANCE_TYPE,
                              ATTACK_VECTOR_TYPE]
            if any(_type not in file_content for _type in required_types):
                tenant_obj = next(self.modular_client.tenant_service().i_get_by_acc(
                    obj.project
                ), None) if not tenant_obj else tenant_obj
                # SHARDS

                collection = ShardsCollectionFactory.from_tenant(tenant_obj)
                collection.io = ShardsS3IO(
                    bucket=self.environment_service.default_reports_bucket_name(),
                    key=TenantReportsBucketKeysBuilder(
                        tenant_obj).nearest_snapshot_key(self.end_date),
                    client=self.s3_client
                )
                collection.fetch_all()
                collection.fetch_meta()

                if COMPLIANCE_TYPE not in file_content:
                    coverage = self._get_tenant_compliance(
                        modular_helpers.tenant_cloud(tenant_obj),
                        collection
                    )
                    file_content[COMPLIANCE_TYPE] = coverage

                collector = self._initiate_resource_collector(
                    collection,
                    modular_helpers.get_tenant_regions(tenant_obj)
                )

                if FINOPS_TYPE not in file_content:
                    file_content[FINOPS_TYPE] = collector.finops()
                if ATTACK_VECTOR_TYPE not in file_content:
                    file_content[
                        ATTACK_VECTOR_TYPE] = collector.attack_vector()
                if RESOURCES_TYPE not in file_content:
                    file_content[RESOURCES_TYPE] = collector.resources()

            file_content = self._update_missing_tenant_content(file_content)
            self.gz_put_json(
                bucket=metrics_bucket,
                key=TENANT_METRICS_FILE_PATH.format(
                    customer=obj.customer_name,
                    date=self.current_week_date,
                    project_id=filename
                ),
                obj=file_content
            )
            if filename.startswith(ARCHIVE_PREFIX):
                _LOG.debug(
                    f'Deleting non-archive metrics for tenant {obj.project}')
                self.s3_client.gz_delete_object(
                    bucket=metrics_bucket,
                    key=TENANT_METRICS_FILE_PATH.format(
                        customer=obj.customer_name,
                        date=self.current_week_date,
                        project_id=obj.project
                    )
                )

        return {DATA_TYPE: NEXT_STEP,
                END_DATE: self.end_date.isoformat() if event.get(
                    END_DATE) else None,
                'continuously': event.get('continuously')}

    def _save_monthly_rule_statistics(self, tenant_obj, rule_data):
        date_to_process = utc_datetime(self.current_week_date).date()
        if self.today_date.date().isoformat() == self.month_first_day_iso:  # if month ends
            self.gz_put_json(
                bucket=self.environment_service.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    self.today_date.date() - timedelta(days=1),
                    tenant=tenant_obj),
                obj=rule_data)
        elif week_number(date_to_process) != 1:
            self.gz_put_json(
                bucket=self.environment_service.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    date_to_process, tenant=tenant_obj),
                obj=rule_data)
        else:  # if week does not fully belong to the current month
            jobs = self.ambiguous_job_service.get_by_tenant_name(
                tenant_name=tenant_obj.name,
                start=self.month_first_day,
                end=self.end_date,
                status=JobState.SUCCEEDED,
            )
            average = self.report_service.average_statistics(*map(
                self.report_service.job_statistics, jobs
            ))
            self.gz_put_json(
                bucket=self.environment_service.get_statistics_bucket_name(),
                key=StatisticsBucketKeysBuilder.tenant_statistics(
                    date_to_process, tenant=tenant_obj), obj=list(average))

    def _is_tenant_active(self, tenant: Tenant) -> bool:
        _LOG.debug(f'Going to check whether Custodian is activated '
                   f'for tenant {tenant.name}')
        if not tenant.is_active:
            _LOG.debug('Tenant is not active')
            return False
        lic = self.license_service.get_tenant_license(tenant)
        if not lic:
            return False
        if lic.is_expired():
            _LOG.warning(f'License {lic.license_key} has expired')
            return False
        last_month_date = datetime.combine(
            (self.today_date - relativedelta(months=1)).replace(day=1),
            datetime.min.time()
        )
        if not list(self.ambiguous_job_service.get_by_tenant_name(
                tenant_name=tenant.name, start=last_month_date, limit=1)):
            _LOG.warning(
                f'Tenant {tenant.name} was inactive more than month (no scans '
                f'for previous month since {last_month_date.isoformat()})')
            return False
        return True

    def _default_dates(self):
        start_date = utc_datetime(self.last_week_date, utc=False).date() \
            if self.last_week_date else (
                self.today_date - timedelta(days=7)).date()
        s3_object_date = self.current_week_date
        end_date = self.today_midnight if self.current_week_date <= self.yesterday.isoformat() else datetime.now()
        self.TO_UPDATE_MARKER = self.current_week_date <= self.yesterday.isoformat()

        return start_date, s3_object_date, end_date

    def _get_tenant_compliance(self, cloud: Cloud,
                               collection: ShardsCollection) -> Dict[str, list]:
        coverage = self.coverage_service.coverage_from_collection(
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

    def _check_not_scanned_tenants(self, prev_key, current_accounts) -> set:
        """Get accounts that were not scanned during last week"""
        _LOG.debug(f'TEMP: current_accounts: {current_accounts}')
        previous_files = list(self.s3_client.list_dir(
            bucket_name=self.environment_service.get_metrics_bucket_name(),
            key=prev_key))
        _LOG.debug(f'Previous files: {previous_files}')
        prev_accounts = set(
            f.split('/')[-1].split('.')[0] for f in previous_files
            if not f.split('/')[-1].startswith(ARCHIVE_PREFIX)
        )
        _LOG.debug(f'TEMP: prev_accounts: {prev_accounts}')

        ts = self.modular_client.tenant_service()
        tenants = (ts.get(acc) for acc in current_accounts)
        filtered_tenants = (tenant.project for tenant in tenants if
                            tenant is not None)
        return prev_accounts - set(filtered_tenants)

    def _update_missing_tenant_content(self, file_content: dict):
        """
        Reset overview data for tenant that has not been scanned in a
        specific period
        """
        if not file_content.get(OUTDATED_TENANTS, {}):
            file_content[OUTDATED_TENANTS] = {
                file_content[CLOUD_ATTR]:
                    {file_content['tenant_name']: file_content['to']}
            }

        file_content['from'] = (
                    self.start_date + timedelta(days=1)).isoformat()
        # just if I forget - lambda will update metrics of tenants with
        # no scans in this week like outdated. If it’s outdated, duplicate it
        # once a week to avoid making unnecessary READ/PUT actions.
        # If scans appears, process as usual
        file_content['to'] = self.current_week_date
        file_content[OVERVIEW_TYPE][TOTAL_SCANS_ATTR] = 0
        file_content[OVERVIEW_TYPE][FAILED_SCANS_ATTR] = 0
        file_content[OVERVIEW_TYPE][SUCCEEDED_SCANS_ATTR] = 0
        if not file_content[OVERVIEW_TYPE]['regions_data']:
            file_content[OVERVIEW_TYPE]['regions_data'] = {
                'multiregion': {
                    'severity_data': {},
                    'resource_types_data': {}
                }
            }
        return file_content

    def _save_monthly_state(self, data: dict, project_id: str, customer: str):
        path = f'{customer}/accounts/monthly/{self.next_month_date}/{project_id}.json'
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        _LOG.debug(f'Save monthly metrics for account {project_id}')
        self.gz_put_json(
            bucket=metrics_bucket,
            key=path,
            obj=data
        )

    def save_weekly_job_stats(self, customer, tenant_objects,
                              end_date: str = None):
        def append_time_period(new_start_date, new_end_date):
            nonlocal time_periods
            time_periods.append((new_start_date, new_end_date))

        if not end_date:
            start_date = (utc_datetime(
                self.last_week_date) - timedelta(days=7)).date()
        else:
            start_date = utc_datetime(self.last_week_date).date()
        time_periods = []
        end_date = utc_datetime(end_date).date() if end_date else utc_datetime(
            self.last_week_date).date()

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
            if self.job_statistics_service.get_by_customer_and_date(
                    customer, start_date, end_date):
                continue

            scans = self.ambiguous_job_service.get_by_customer_name(
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
                    tenant_obj = self.modular_client.tenant_service().get(name)
                    if not tenant_obj or not tenant_obj.project:
                        _LOG.warning(f'Cannot find tenant {name}. Skipping...')
                        continue

                cloud = tenant_obj.cloud.lower()
                weekly_stats[cloud].setdefault('scanned_regions', {}).\
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
                weekly_stats[cloud][LAST_SCAN_DATE] = self._get_last_scan_date(
                    scan.submitted_at, weekly_stats[cloud].get(LAST_SCAN_DATE))

    @staticmethod
    def _get_last_scan_date(new_scan_date: str, last_scan_date: str = None):
        if not last_scan_date:
            return new_scan_date
        last_scan_datetime = utc_datetime(last_scan_date, utc=False)
        scan_datetime = utc_datetime(new_scan_date, utc=False)
        if last_scan_datetime < scan_datetime:
            return new_scan_date
        return last_scan_date

    def _initiate_resource_collector(self, collection: ShardsCollection,
                                     regions) -> ResourcesAndOverviewCollector:
        it = self.metrics_service.create_resources_generator(
            collection, regions
        )
        col = self.ResourcesAndOverviewCollector(collection.meta,
                                                 self.mappings_collector)
        for rule, region, dto, _ in it:
            col.add_resource(rule, region, dto)
        return col

    def _collect_tenant_metrics(self, collection, tenant_obj) -> dict:
        result = {}
        collector = self._initiate_resource_collector(
            collection, modular_helpers.get_tenant_regions(tenant_obj)
        )

        # coverage
        _LOG.debug(f'Calculating {tenant_obj.name} coverage')
        result.update({
            COMPLIANCE_TYPE: self._get_tenant_compliance(
                modular_helpers.tenant_cloud(tenant_obj), collection)
        })
        # resources
        _LOG.debug(f'Collecting {tenant_obj.name} resources metrics')
        result[RESOURCES_TYPE] = collector.resources()
        # attack vector
        result[ATTACK_VECTOR_TYPE] = collector.attack_vector()
        # overview
        _LOG.debug(f'Collecting {tenant_obj.name} overview metrics')
        result[OVERVIEW_TYPE] = {
                'resources_violated': collector.len_of_unique(),
                'regions_data': collector.region_severity()
        }
        # rule
        _LOG.debug(f'Collecting {tenant_obj.name} rule metrics')
        jobs = self.ambiguous_job_service.get_by_tenant_name(
                tenant_name=tenant_obj.name,
                start=datetime.combine(self.start_date, datetime.min.time()),
                end=self.end_date,
                status=JobState.SUCCEEDED,
        )  # TODO report query again?
        average = self.report_service.average_statistics(*map(
                self.report_service.job_statistics, jobs
        ))  # todo download in threads?
        result[RULE_TYPE] = {
                    'rules_data': list(average),
                    'violated_resources_length': collector.len_of_unique()
                }
        # finops
        result[FINOPS_TYPE] = collector.finops()
        return result

    def _collect_k8s_metrics(self, collection) -> dict:
        result = {}
        collector = self._initiate_resource_collector(collection, [])

        # coverage
        _LOG.debug(f'Calculating k8s coverage')
        compliance = self._get_tenant_compliance(
            Cloud.KUBERNETES, collection).get('regions_data', [])
        if not compliance:
            result['compliance_data'] = []
        else:
            result['compliance_data'] = compliance[0].get('standards_data', [])
        # resources
        _LOG.debug(f'Collecting k8s resources metrics')
        result['policy_data'] = collector.k8s_resources()
        # attack vector
        result['mitre_data'] = collector.k8s_attack_vector()
        return result


TENANT_METRICS = TenantMetrics.build()
