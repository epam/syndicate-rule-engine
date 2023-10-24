import calendar
import json
from datetime import datetime, timedelta
from functools import cmp_to_key
from typing import List, Dict, Tuple, Any, TypedDict

from dateutil.relativedelta import relativedelta, SU
from modular_sdk.models.tenant import Tenant

from helpers import get_logger, CustodianException
from helpers.constants import \
    RULE_TYPE, OVERVIEW_TYPE, RESOURCES_TYPE, CLOUD_ATTR, CUSTOMER_ATTR, \
    SUCCEEDED_SCANS_ATTR, JOB_SUCCEEDED_STATUS, FAILED_SCANS_ATTR, \
    JOB_FAILED_STATUS, TOTAL_SCANS_ATTR, TENANT_ATTR, LAST_SCAN_DATE, \
    COMPLIANCE_TYPE, TENANT_NAME_ATTR, ID_ATTR, ATTACK_VECTOR_TYPE, \
    DATA_TYPE, TACTICS_ID_MAPPING, MANUAL_TYPE_ATTR, REACTIVE_TYPE_ATTR, \
    AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, END_DATE, FINOPS_TYPE, OUTDATED_TENANTS
from helpers.reports import hashable, keep_highest
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_datetime
from helpers.utils import severity_cmp
from services import SERVICE_PROVIDER
from services.batch_results_service import BatchResultsService
from services.clients.s3 import S3Client
from services.coverage_service import CoverageService
from services.environment_service import EnvironmentService
from services.findings_service import FindingsService
from services.job_service import JobService
from services.job_statistics_service import JobStatisticsService
from services.metrics_service import MetricsService
from services.modular_service import ModularService
from services.rule_meta_service import LazyLoadedMappingsCollector
from services.rule_report_service import RuleReportService
from services.setting_service import SettingsService

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

    def __init__(self, job_service: JobService, s3_client: S3Client,
                 batch_results_service: BatchResultsService,
                 findings_service: FindingsService,
                 environment_service: EnvironmentService,
                 rule_report_service: RuleReportService,
                 settings_service: SettingsService,
                 modular_service: ModularService,
                 coverage_service: CoverageService,
                 metrics_service: MetricsService,
                 mappings_collector: LazyLoadedMappingsCollector,
                 job_statistics_service: JobStatisticsService):
        self.job_service = job_service
        self.batch_results_service = batch_results_service
        self.s3_client = s3_client
        self.findings_service = findings_service
        self.environment_service = environment_service
        self.rule_report_service = rule_report_service
        self.settings_service = settings_service
        self.modular_service = modular_service
        self.coverage_service = coverage_service
        self.mappings_collector = mappings_collector
        self.metrics_service = metrics_service
        self.job_statistics_service = job_statistics_service

        self.today_date = datetime.today()
        self.today_midnight = datetime.combine(self.today_date,
                                               datetime.min.time())

        self._date_marker = self.settings_service.get_report_date_marker()
        self.current_week_date = self._date_marker.get('current_week_date')
        self.last_week_date = self._date_marker.get('last_week_date')
        self.last_week_datetime = utc_datetime(
            self.last_week_date if self.last_week_date else
            str((self.today_date - timedelta(days=7)).date()), utc=False)
        self.yesterday = (self.today_date - timedelta(days=1)).date()
        self.next_month_date = (self.today_date.date().replace(day=1) +
                                relativedelta(months=1)).isoformat()
        self.month_first_day = self.today_date.date().replace(
            day=1).isoformat()

        self.TO_UPDATE_MARKER = False
        self.weekly_scan_statistics = {}

    @classmethod
    def build(cls) -> 'TenantMetrics':
        return cls(
            job_service=SERVICE_PROVIDER.job_service(),
            environment_service=SERVICE_PROVIDER.environment_service(),
            s3_client=SERVICE_PROVIDER.s3(),
            batch_results_service=SERVICE_PROVIDER.batch_results_service(),
            findings_service=SERVICE_PROVIDER.findings_service(),
            rule_report_service=SERVICE_PROVIDER.rule_report_service(),
            settings_service=SERVICE_PROVIDER.settings_service(),
            modular_service=SERVICE_PROVIDER.modular_service(),
            coverage_service=SERVICE_PROVIDER.coverage_service(),
            mappings_collector=SERVICE_PROVIDER.mappings_collector(),
            metrics_service=SERVICE_PROVIDER.metrics_service(),
            job_statistics_service=SERVICE_PROVIDER.job_statistics_service()
        )

    class ResourcesAndOverviewCollector:
        def __init__(self, findings: dict,
                     mappings_collector: LazyLoadedMappingsCollector):
            self._findings = findings
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
            for rule in self._resources:
                item = {
                    "policy": rule,
                    "resource_type": self._mappings_collector.service.get(
                        rule),
                    "description": self._findings.get(rule).get(
                        'description') or '',
                    "severity": self._mappings_collector.severity.get(rule),
                    "regions_data": {}
                }
                for region, res in self._resources[rule].items():
                    item['regions_data'][region] = {'resources': list(res)}
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
                description = self._findings.get(rule).get('description') or ''

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

        def finops(self):
            service_resource_mapping = {}
            for rule in self._resources:
                category = self._mappings_collector.category.get(rule)
                if 'FinOps' not in category:
                    continue
                else:
                    category = category.split('>')[-1].strip()

                service_section = self._mappings_collector.service_section.get(
                    rule)
                service_resource_mapping.setdefault(service_section,
                                                    {'rules_data': []})

                description = self._findings.get(rule).get('description') or ''
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
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        stat_bucket = self.environment_service.get_statistics_bucket_name()
        end_date = event.get(END_DATE)

        if end_date:
            end_datetime = utc_datetime(end_date, utc=False)

            if end_datetime < self.today_date.astimezone():
                end_date_datetime = end_datetime
                weekday = end_date_datetime.weekday()
                s3_object_date = end_date if weekday == 6 else (
                        end_date_datetime + timedelta(days=6 - weekday)). \
                    date().isoformat()
                self.next_month_date = (
                        end_date_datetime.replace(day=1) +
                        relativedelta(months=1)).date().isoformat()

                su_number = -1 if weekday != 6 else -2
                start_date = (end_date_datetime + relativedelta(
                    weekday=SU(su_number))).date()
                end_date = end_date_datetime.date()
            else:
                start_date, s3_object_date, end_date = self._default_dates()
        else:
            start_date, s3_object_date, end_date = self._default_dates()

        # get all scans for each of existing customer
        _LOG.debug(f'Retrieving jobs between {start_date} and '
                   f'{end_date} dates')
        for customer in self.modular_service.i_get_customers():
            tenant_objects = {}
            if customer == SYSTEM_CUSTOMER:
                _LOG.debug('Skipping system customer')
                continue

            for job_type in (MANUAL_TYPE_ATTR, REACTIVE_TYPE_ATTR):
                jobs = self._retrieve_recent_scans(
                    start=datetime.combine(start_date, datetime.min.time()),
                    end=datetime.combine(end_date, datetime.min.time()),
                    customer_name=customer.name, job_type=job_type)
                customer_to_tenants_mapping.setdefault(
                    customer.name, {}).setdefault(job_type, []).extend(jobs)

        for customer, types in customer_to_tenants_mapping.items():
            current_accounts = set()
            for t, jobs in types.items():
                current_accounts.update(
                    {getattr(j, 'tenant_display_name', None) or
                     getattr(j, 'tenant_name', None) for j in
                     jobs if j})
            missing = self._check_not_scanned_tenants(
                prev_key=f'{customer}/accounts/{self.last_week_date}/',
                current_accounts=current_accounts)
            _LOG.debug(f'Not scanned accounts within {customer} customer for '
                       f'this week: {missing}')
            for project in missing:
                if not project:
                    _LOG.debug(f'Somehow non-existing missing: "{project}"')
                    continue
                tenant_obj = list(self.modular_service.i_get_tenants_by_acc(
                    acc=project, active=True))
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

            for t, jobs in types.items():
                for job in jobs:
                    name = job.tenant_display_name if t == MANUAL_TYPE_ATTR \
                        else job.tenant_name
                    result_tenant_data.setdefault(name, {OVERVIEW_TYPE: {
                        TOTAL_SCANS_ATTR: 0,
                        FAILED_SCANS_ATTR: 0,
                        SUCCEEDED_SCANS_ATTR: 0
                    }
                    })
                    tenant_overview = result_tenant_data[name][OVERVIEW_TYPE]
                    tenant_overview[TOTAL_SCANS_ATTR] += 1
                    if job.status == JOB_FAILED_STATUS:
                        tenant_overview[FAILED_SCANS_ATTR] += 1
                        continue
                    elif job.status == JOB_SUCCEEDED_STATUS:
                        tenant_overview[SUCCEEDED_SCANS_ATTR] += 1
                    else:
                        _LOG.warning(f'Unknown scan status: {job.status}; '
                                     f'scan {job.job_id}')

                    if name not in tenant_objects:
                        tenant_obj = self.modular_service.get_tenant(
                            tenant=name)
                        if not tenant_obj or not tenant_obj.project:
                            _LOG.warning(
                                f'Cannot find tenant {name}. Skipping...')
                            continue
                        tenant_objects[name] = tenant_obj

                    if t == MANUAL_TYPE_ATTR:
                        tenant_to_job_mapping.setdefault(name, {}).setdefault(
                            t, []).append(job)
                    last_job = tenant_last_job_mapping.setdefault(name, job)
                    if last_job.submitted_at < job.submitted_at:
                        tenant_last_job_mapping[name] = job

            today_date = self.today_date.date().isoformat()
            if not event.get(END_DATE):
                if today_date == self.month_first_day:
                    self.save_weekly_job_stats(customer, tenant_objects,
                                               end_date=today_date)
                else:
                    self.save_weekly_job_stats(customer, tenant_objects)

        if not tenant_objects:
            _LOG.warning(f'No jobs for period {start_date} to {end_date}')

        for name, tenant_obj in tenant_objects.items():
            cloud = 'google' if tenant_obj.cloud.lower() == 'gcp' \
                else tenant_obj.cloud.lower()
            identifier = tenant_obj.project
            active_regions = list(self.modular_service.get_tenant_regions(
                tenant_obj))
            _LOG.debug(f'Processing \'{name}\' tenant with id {identifier} '
                       f'and active regions: {", ".join(active_regions)}')
            if event.get(END_DATE):
                findings = self.findings_service.get_findings_content(
                    tenant_obj.project, findings_date=end_date.isoformat())
            else:
                findings = self.findings_service.get_findings_content(
                    tenant_obj.project)
            if not findings:
                _LOG.warning(
                    f'Cannot find findings for tenant \'{name}\'. Skipping')
                continue

            # save for customer metrics
            # todo drop after s3 listing with prefix will be tested
            if not event.get(END_DATE):
                obj_name = f'{self.next_month_date}/{self.findings_service._get_key(tenant_objects.get(name).project)}'
                self.s3_client.put_object(
                    bucket_name=stat_bucket,
                    object_name=obj_name,
                    body=json.dumps(findings, separators=(',', ':'))
                )

            col = self._initiate_resource_collector(findings, tenant_obj)
            # general account info
            result_tenant_data.setdefault(name, {}).update({
                CUSTOMER_ATTR: tenant_obj.customer_name,
                TENANT_NAME_ATTR: tenant_obj.name,
                ID_ATTR: tenant_obj.account_number or tenant_obj.project,
                CLOUD_ATTR: cloud,
                'activated_regions': active_regions,
                'from': start_date.isoformat(),
                'to': end_date.isoformat(),
                OUTDATED_TENANTS: {},
                LAST_SCAN_DATE: tenant_last_job_mapping[name].submitted_at
            })
            # coverage
            _LOG.debug('Calculating tenant coverage')
            coverage = self._get_tenant_compliance(tenant_obj, findings)
            result_tenant_data.get(name).update(
                {COMPLIANCE_TYPE: coverage})
            # resources
            _LOG.debug('Collecting tenant resources metrics')
            result_tenant_data.setdefault(name, {}).update({
                RESOURCES_TYPE: col.resources()
            })
            # overview
            _LOG.debug('Collecting tenant overview metrics')
            result_tenant_data.setdefault(name, {}).setdefault(OVERVIEW_TYPE,
                                                               {}).update({
                'resources_violated': col.len_of_unique(),
                'regions_data': col.region_severity()
            })
            # rule
            _LOG.debug('Collecting tenant rule metrics')
            try:
                statistics = self.rule_report_service.attain_referenced_reports(
                    start_iso=datetime.combine(start_date,
                                               datetime.min.time()),
                    end_iso=end_date,
                    cloud_ids=[identifier],
                    entity_attr=TENANT_ATTR,
                    source_list=tenant_to_job_mapping.get(name, {}).get(
                        MANUAL_TYPE_ATTR, []),
                    list_format=True,
                    typ=MANUAL_TYPE_ATTR)
            except CustodianException as e:
                _LOG.error(f'Caught error: {e}\nRule statistic for tenant '
                           f'{tenant_obj.name} will be empty')
                statistics = {}
            result_tenant_data[name].setdefault(
                RULE_TYPE, {
                    'rules_data': statistics.get(name, []),
                    'violated_resources_length': col.len_of_unique()
                })
            # attack vector
            result_tenant_data[name].setdefault(
                ATTACK_VECTOR_TYPE, col.attack_vector()
            )
            # finops
            result_tenant_data[name].setdefault(
                FINOPS_TYPE, col.finops()
            )
            # saving to s3
            _LOG.debug(f'Saving metrics of {tenant_obj.name} tenant to '
                       f'{metrics_bucket}')
            self.s3_client.put_object(
                bucket_name=metrics_bucket,
                object_name=TENANT_METRICS_FILE_PATH.format(
                    customer=tenant_obj.customer_name,
                    date=s3_object_date, project_id=identifier),
                body=json.dumps(result_tenant_data.get(name),
                                separators=(",", ":"))
            )
            if not event.get(END_DATE) or calendar.monthrange(
                    end_date.year, end_date.month)[1] == end_date.day:
                self._save_monthly_state(result_tenant_data.get(name),
                                         identifier,
                                         tenant_obj.customer_name)

            if self.TO_UPDATE_MARKER:
                _LOG.debug(f'Saving metrics of {tenant_obj.name} for current '
                           f'date')
                s3_object_date = (self.today_date + relativedelta(
                    weekday=SU(0))).date().isoformat()
                self.s3_client.put_object(
                    bucket_name=metrics_bucket,
                    object_name=TENANT_METRICS_FILE_PATH.format(
                        customer=tenant_obj.customer_name,
                        date=s3_object_date,
                        project_id=identifier),
                    body=json.dumps(result_tenant_data.get(name),
                                    separators=(',', ':'))
                )
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
                dict_to_save['tenants'] = dict_to_save.get(
                    'tenants', {})
                self.job_statistics_service.create(dict_to_save).save()

        _LOG.debug(
            'Copy metrics for tenants that haven\'t been scanned this week')
        for tenant, obj in missing_tenants.items():
            if self.today_date.weekday() != 0:  # not Monday
                if self.s3_client.file_exists(
                        metrics_bucket, TENANT_METRICS_FILE_PATH.format(
                            customer=obj.customer_name,
                            date=self.current_week_date,
                            project_id=obj.project)):
                    continue

            file_path = TENANT_METRICS_FILE_PATH.format(
                customer=obj.customer_name,
                date=start_date.isoformat(), project_id=obj.project)
            file_content = self.s3_client.get_json_file_content(
                bucket_name=metrics_bucket,
                full_file_name=file_path
            )
            if not file_content:
                _LOG.warning(f'Cannot find file {file_path}')
                continue

            required_types = [FINOPS_TYPE, RESOURCES_TYPE, COMPLIANCE_TYPE,
                              ATTACK_VECTOR_TYPE]
            if any(_type not in file_content for _type in required_types):
                tenant_obj = next(self.modular_service.i_get_tenants_by_acc(
                    obj.project), None)
                findings = self.findings_service.get_findings_content(
                    tenant_obj.project,
                    findings_date=end_date.isoformat())
                if COMPLIANCE_TYPE not in file_content:
                    coverage = self._get_tenant_compliance(tenant_obj,
                                                           findings)
                    file_content[COMPLIANCE_TYPE] = coverage

                col = self._initiate_resource_collector(findings, tenant_obj)

                if FINOPS_TYPE not in file_content:
                    file_content[FINOPS_TYPE] = col.finops()
                if ATTACK_VECTOR_TYPE not in file_content:
                    file_content[ATTACK_VECTOR_TYPE] = col.attack_vector()
                if RESOURCES_TYPE not in file_content:
                    file_content[RESOURCES_TYPE] = col.resources()

            file_content = self._update_missing_tenant_content(
                file_content, start_date)
            self.s3_client.put_object(
                bucket_name=metrics_bucket,
                object_name=TENANT_METRICS_FILE_PATH.format(
                    customer=obj.customer_name,
                    date=self.current_week_date,
                    project_id=obj.project),
                body=json.dumps(file_content, separators=(',', ':'))
            )

        return {DATA_TYPE: NEXT_STEP,
                END_DATE: end_date.isoformat() if event.get(
                    END_DATE) else None,
                'continuously': event.get('continuously')}

    def _default_dates(self):
        start_date = self.last_week_datetime.date()
        s3_object_date = self.current_week_date
        end_date = self.today_midnight if self.current_week_date <= self.yesterday.isoformat() else datetime.now()
        self.TO_UPDATE_MARKER = self.current_week_date <= self.yesterday.isoformat()

        return start_date, s3_object_date, end_date

    def _retrieve_recent_scans(
            self, customer_name: str, end: datetime, job_type: str,
            start: datetime = None) -> Tuple[Any, List]:
        """
        Retrieves all jobs that have been executed in the last week and
        extracts the account names and status from them
        :return: list of jobs
        """
        # todo add xray
        if job_type == REACTIVE_TYPE_ATTR:
            jobs = self.batch_results_service.get_between_period_by_customer(
                customer_name=customer_name, start=start.isoformat(),
                end=end.isoformat(), limit=100, only_succeeded=False
            )
        else:
            if not start:
                jobs = self.job_service.get_customer_jobs(
                    customer_display_name=customer_name, limit=100
                )
            else:
                jobs = self.job_service.get_customer_jobs_between_period(
                    start_period=start, end_period=end, customer=customer_name,
                    only_succeeded=False, limit=100)
        _LOG.debug(f'Retrieved {len(jobs)} {job_type} jobs for customer '
                   f'{customer_name}')
        return jobs

    def _get_tenant_compliance(self, tenant: Tenant,
                               findings: dict) -> Dict[str, list]:
        points = self.coverage_service.derive_points_from_findings(findings)
        if tenant.cloud == AWS_CLOUD_ATTR:
            points = self.coverage_service.distribute_multiregion(points)
        elif tenant.cloud == AZURE_CLOUD_ATTR:
            points = self.coverage_service.congest_to_multiregion(points)
        coverage = self.coverage_service.calculate_region_coverages(
            points=points, cloud=tenant.cloud
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
        average_coverage = [{'name': k, 'value': sum(v) / len(v)}
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
        )
        _LOG.debug(f'TEMP: prev_accounts: {prev_accounts}')
        return prev_accounts - set(
            self.modular_service.get_tenant(acc).project for acc in
            current_accounts)

    def _update_missing_tenant_content(self, file_content: dict, start_date):
        """
        Reset overview data for tenant that has not been scanned in a
        specific period
        """
        if not file_content.get(OUTDATED_TENANTS, {}):
            file_content[OUTDATED_TENANTS] = {
                file_content[CLOUD_ATTR]:
                    {file_content['tenant_name']: file_content['to']}
            }

        file_content['from'] = (start_date + timedelta(days=1)).isoformat()
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
        self.s3_client.put_object(
            bucket_name=metrics_bucket,
            object_name=path,
            body=json.dumps(data, separators=(",", ":")))

    def save_weekly_job_stats(self, customer, tenant_objects,
                              end_date: str = None):
        def append_time_period(new_start_date, new_end_date):
            nonlocal time_periods
            time_periods.append((new_start_date, new_end_date))

        if not end_date:
            start_date = (utc_datetime(
                self.last_week_date) - timedelta(days=7)).date()
        else:
            start_date = self.last_week_date
        time_periods = []
        end_date = utc_datetime(end_date) if end_date else utc_datetime(
            self.last_week_date)

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

            ed_scans = self._retrieve_recent_scans(
                customer, end=utc_datetime(end_date),
                start=utc_datetime(start_date), job_type=REACTIVE_TYPE_ATTR)
            manual_scans = self._retrieve_recent_scans(
                customer, end=utc_datetime(end_date),
                start=utc_datetime(start_date), job_type=MANUAL_TYPE_ATTR)

            self.weekly_scan_statistics.setdefault(customer, {}). \
                setdefault('customer_name', customer)
            weekly_stats = self.weekly_scan_statistics[customer]
            for c in CLOUDS:
                weekly_stats.setdefault(c, {})
                weekly_stats[c]['from_date'] = start_date
                weekly_stats[c]['to_date'] = end_date
                weekly_stats[c]['failed'] = 0
                weekly_stats[c]['succeeded'] = 0

            for scan in ed_scans + manual_scans:
                name = getattr(scan, 'tenant_display_name', None) or \
                       getattr(scan, 'tenant_name', None)
                if not (tenant_obj := tenant_objects.get(name)):
                    tenant_obj = self.modular_service.get_tenant(tenant=name)
                    if not tenant_obj or not tenant_obj.project:
                        _LOG.warning(f'Cannot find tenant {name}. Skipping...')
                        continue
                cloud = tenant_obj.cloud.lower()
                weekly_stats[cloud].setdefault('tenants', {}).setdefault(
                    tenant_obj.project, {'failed_scans': 0,
                                         'succeeded_scans': 0})
                if scan.status == JOB_FAILED_STATUS:
                    weekly_stats[cloud]['failed'] += 1
                    weekly_stats[cloud]['tenants'][tenant_obj.project][
                        'failed_scans'] += 1
                elif scan.status == JOB_SUCCEEDED_STATUS:
                    weekly_stats[cloud]['tenants'][tenant_obj.project][
                        'succeeded_scans'] += 1
                    weekly_stats[cloud]['succeeded'] += 1

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

    def _initiate_resource_collector(self, findings, tenant_obj):
        it = self.metrics_service.create_resources_generator(
            findings, tenant_obj.cloud,
            self.modular_service.get_tenant_regions(tenant_obj))
        col = self.ResourcesAndOverviewCollector(findings,
                                                 self.mappings_collector)
        for rule, region, dto in it:
            col.add_resource(rule, region, dto)
        return col


TENANT_METRICS = TenantMetrics.build()
