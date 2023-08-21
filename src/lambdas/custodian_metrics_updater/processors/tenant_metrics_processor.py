import json
from copy import deepcopy
from datetime import datetime, timedelta
from typing import List, Union, Dict, Tuple, Any

from dateutil.relativedelta import relativedelta, SU
from modular_sdk.models.tenant import Tenant

from helpers import get_logger, CustodianException, filter_dict
from helpers.constants import \
    RULE_TYPE, OVERVIEW_TYPE, RESOURCES_TYPE, CLOUD_ATTR, CUSTOMER_ATTR, \
    SUCCEEDED_SCANS_ATTR, JOB_SUCCEEDED_STATUS, FAILED_SCANS_ATTR, \
    JOB_FAILED_STATUS, TOTAL_SCANS_ATTR, TENANT_ATTR, LAST_SCAN_DATE, \
    COMPLIANCE_TYPE, TENANT_NAME_ATTR, ID_ATTR, ATTACK_VECTOR_TYPE, \
    DATA_TYPE, TACTICS_ID_MAPPING, MANUAL_TYPE_ATTR, REACTIVE_TYPE_ATTR, \
    MULTIREGION, START_DATE, AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.batch_results_service import BatchResultsService
from services.clients.s3 import S3Client
from services.coverage_service import CoverageService
from services.environment_service import EnvironmentService
from services.findings_service import FindingsService
from services.job_service import JobService
from services.modular_service import ModularService
from services.rule_meta_service import RuleMetaService
from services.rule_report_service import RuleReportService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

TENANT_METRICS_FILE_PATH = '{customer}/accounts/{date}/{project_id}.json'
NEXT_STEP = 'tenant_groups'


class TenantMetrics:

    def __init__(self, job_service: JobService, s3_client: S3Client,
                 batch_results_service: BatchResultsService,
                 findings_service: FindingsService,
                 environment_service: EnvironmentService,
                 rule_report_service: RuleReportService,
                 settings_service: SettingsService,
                 modular_service: ModularService,
                 rule_meta_service: RuleMetaService,
                 coverage_service: CoverageService):
        self.job_service = job_service
        self.batch_results_service = batch_results_service
        self.s3_client = s3_client
        self.findings_service = findings_service
        self.environment_service = environment_service
        self.rule_report_service = rule_report_service
        self.settings_service = settings_service
        self.modular_service = modular_service
        self.rule_meta_service = rule_meta_service
        self.coverage_service = coverage_service

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
        self.current_month_date = self.today_date.date().replace(day=1)
        self.next_month_date = (self.today_date.date().replace(day=1) +
                                relativedelta(months=1)).isoformat()

        self.TO_UPDATE_MARKER = False
        self.COVERAGE_CLOUD_MAPPING = {
            'aws': None,
            'azure': None,
            'google': None
        }
        self.metadata = {}

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

        start_date = event.get(START_DATE)
        if start_date and utc_datetime(start_date,
                                       utc=False) > self.today_date:
            s3_object_date = utc_datetime(start_date,
                                          utc=False).date().isoformat()
        else:
            s3_object_date = self.current_week_date

        if self.current_week_date <= self.yesterday.isoformat():
            end_date = self.today_midnight
            self.TO_UPDATE_MARKER = True
        else:
            end_date = datetime.now()
            self.TO_UPDATE_MARKER = False

        # get all scans for each of existing customer
        _LOG.debug(f'Retrieving jobs between {self.last_week_datetime} and '
                   f'{end_date} dates')
        for customer in self.modular_service.i_get_customers():
            for job_type in (MANUAL_TYPE_ATTR, REACTIVE_TYPE_ATTR):
                jobs = self._retrieve_recent_scans(
                    start=self.last_week_datetime, end=end_date,
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
            missing = self._check_not_scanned_accounts(
                prev_key=f'{customer}/accounts/{self.last_week_date}/',
                current_accounts=current_accounts)
            _LOG.debug(f'Not scanned accounts within {customer} customer for '
                       f'this week: {missing}')
            for project in missing:
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

        if not tenant_objects:
            _LOG.warning(f'No jobs for period {self.last_week_date} to '
                         f'{end_date}')

        for name, tenant_obj in tenant_objects.items():
            cloud = 'google' if tenant_obj.cloud.lower() == 'gcp' \
                else tenant_obj.cloud.lower()
            identifier = tenant_obj.project
            active_regions = list(self.modular_service.get_tenant_regions(
                tenant_obj))
            _LOG.debug(f'Processing \'{name}\' tenant with id {identifier} '
                       f'and active regions: {", ".join(active_regions)}')
            findings = self.findings_service.get_findings_content(
                tenant_objects.get(name).project)
            if not findings:
                _LOG.warning(
                    f'Cannot find findings for tenant \'{name}\'. Skipping')
                continue
            self._get_policies_metadata(findings.keys())

            # save for customer metrics
            stat_bucket = self.environment_service.get_statistics_bucket_name()
            obj_name = f'{self.next_month_date}/{self.findings_service._get_key(tenant_objects.get(name).project)}'
            self.s3_client.put_object(bucket_name=stat_bucket,
                                      object_name=obj_name,
                                      body=json.dumps(findings))

            # general account info
            result_tenant_data.setdefault(name, {}).update({
                CUSTOMER_ATTR: tenant_obj.customer_name,
                TENANT_NAME_ATTR: tenant_obj.name,
                ID_ATTR: tenant_obj.account_number or tenant_obj.project,
                CLOUD_ATTR: cloud,
                'activated_regions': active_regions,
                'from': self.last_week_datetime.isoformat(),
                'to': end_date.isoformat(),
                LAST_SCAN_DATE: tenant_last_job_mapping[name].submitted_at
            })
            # coverage
            _LOG.debug('Calculating tenant coverage')
            coverage = self._get_tenant_compliance(tenant_obj, findings)
            result_tenant_data.get(name).update(
                {COMPLIANCE_TYPE: coverage})
            # resources
            _LOG.debug('Collecting tenant resources metrics')
            resources = self._prettify_findings(findings, active_regions)
            result_tenant_data.get(name).update({RESOURCES_TYPE: resources})
            # overview
            _LOG.debug('Collecting tenant overview metrics')
            violated_resources_len = len(
                self.findings_service.retrieve_unique_resources(findings))
            result_tenant_data.get(name, {}).setdefault(
                OVERVIEW_TYPE, {}).update(
                {
                    'resources_violated': violated_resources_len,
                    'regions_data': {
                        **self.get_number_of_resources_by_region_and_severity(
                            findings)
                    }
                }
            )
            # rule
            _LOG.debug('Collecting tenant rule metrics')
            try:
                statistics = self.rule_report_service.attain_referenced_reports(
                    start_iso=self.last_week_datetime,
                    end_iso=end_date,
                    account_dn=name,
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
                    'violated_resources_length': violated_resources_len
                })
            # attack vector
            _LOG.debug('Collecting tenant attack vector metrics')
            try:
                attack_vector = self._extract_attack_vector_data(findings)
            except KeyError as e:
                _LOG.error(f'Caught error: {e}\nMITRE statistic for tenant '
                           f'{tenant_obj.name} will be empty')
                attack_vector = {}
            result_tenant_data[name].setdefault(ATTACK_VECTOR_TYPE,
                                                attack_vector)

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
            self._save_monthly_state(result_tenant_data.get(name), identifier,
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
                    body=json.dumps(result_tenant_data.get(name))
                )
            # to free memory
            result_tenant_data.pop(name)

        _LOG.debug(
            'Copy metrics for tenants that haven\'t been scanned this week')
        for tenant, obj in missing_tenants.items():
            file_path = TENANT_METRICS_FILE_PATH.format(
                customer=obj.customer_name,
                date=self.last_week_date, project_id=obj.project)
            file_content = self.s3_client.get_json_file_content(
                bucket_name=metrics_bucket,
                full_file_name=file_path
            )
            if not file_content:
                _LOG.warning(f'Cannot find file {file_path}')
                continue

            file_content = self._update_missing_tenant_content(file_content,
                                                               end_date)
            self.s3_client.put_object(
                bucket_name=metrics_bucket,
                object_name=TENANT_METRICS_FILE_PATH.format(
                    customer=obj.customer_name,
                    date=self.current_week_date,
                    project_id=obj.project),
                body=json.dumps(file_content)
            )

        return {DATA_TYPE: NEXT_STEP, START_DATE: start_date,
                'continuously': event.get('continuously')}

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
                end=end.isoformat(), limit=100
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

    def _prettify_findings(self, findings: dict,
                           active_regions: Union[list, set]) -> List[dict]:
        """
        Add severity and filter resources dict
        """
        resulted_findings = []
        for rule, data in findings.items():
            if all(resources == [] for resources in data.get(
                    'resources').values()):
                continue

            for region in data.get('resources').copy().keys():
                if region not in active_regions and region != MULTIREGION:
                    data['resources'].pop(region)

            data['resource_type'] = data.pop('resourceType', None)
            data.pop('standard_points', None)
            data.pop('report_fields', None)

            for region, resources in data.get('resources', {}).items():
                if not resources:
                    continue

                report_fields = set(self.metadata.get(rule, {}).get(
                    'report_fields', []))
                for r in resources:
                    short_resource = filter_dict(r, report_fields)
                    data.setdefault('regions_data', {}).setdefault(
                        region, {}).setdefault('resources', []).append(
                        short_resource)

            data.pop('resources', None)
            data_copy = deepcopy(data)
            data_copy.pop('attack_vector', None)

            data_copy['description'] = data_copy.get('description', '')
            data_copy['severity'] = self.metadata.get(rule, {}).get(
                'severity', 'Unknown')

            if data_copy.get('regions_data'):
                resulted_findings.append({'policy': rule, **data_copy})
        return resulted_findings

    def _extract_attack_vector_data(self, content: dict):
        temp = {}

        def process_resources(res, res_type, sev, sub_tech):
            return [{'resource': r, 'resource_type': res_type,
                     'severity': sev.capitalize(),
                     'sub_techniques': sub_tech} for r in res]

        for policy, value in content.items():
            policy = policy.replace('epam-', 'ecc-')
            attack_vector = self.metadata.get(policy, {}).get('mitre')
            if not attack_vector:
                continue
            severity = self.metadata.get(policy, {}).get(
                'severity', 'Unknown').capitalize()
            regions_data = value.get('regions_data', {})
            resource_type = value.get('resource_type', {})

            for region, resources in regions_data.items():
                if not resources:
                    continue

                report_fields = set(self.metadata.get(policy, {}).get(
                    'report_fields', []))
                resources['resources'] = [filter_dict(r, report_fields)
                                          for r in resources['resources']]

                for tactic, data in attack_vector.items():
                    for technique in data:
                        technique_name = technique.get('tn_name')
                        technique_id = technique.get('tn_id')
                        sub_techniques = [st['st_name'] for st in
                                          technique.get('st', []) if st]

                        resources_data = process_resources(
                            resources['resources'], resource_type, severity,
                            sub_techniques)

                        tactics_data = temp.setdefault(tactic, {
                            'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                            'techniques_data': {}})
                        techniques_data = tactics_data[
                            'techniques_data'].setdefault(technique_name, {
                            'technique_id': technique_id, 'regions_data': {}})
                        regions_data = techniques_data[
                            'regions_data'].setdefault(region,
                                                       {'resources': []})
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

    def _check_not_scanned_accounts(self, prev_key, current_accounts) -> set:
        """Get accounts that were not scanned during last week"""

        previous_files = self.s3_client.list_dir(
            bucket_name=self.environment_service.get_metrics_bucket_name(),
            key=prev_key)
        prev_accounts = set(
            f.split('/')[-1].split('.')[0] for f in previous_files
        )
        return prev_accounts - set(
            self.modular_service.get_tenant(acc).project for acc in
            current_accounts)

    def _update_missing_tenant_content(self, file_content: dict, end_date):
        """
        Reset overview data for tenant that has not been scanned in a
        specific period
        """

        file_content['from'] = (self.last_week_datetime + timedelta(
            days=1)).isoformat()
        file_content['to'] = end_date.isoformat()
        file_content[OVERVIEW_TYPE][TOTAL_SCANS_ATTR] = 0
        file_content[OVERVIEW_TYPE][FAILED_SCANS_ATTR] = 0
        file_content[OVERVIEW_TYPE][SUCCEEDED_SCANS_ATTR] = 0
        file_content[OVERVIEW_TYPE]['regions_data'] = {}
        file_content[OVERVIEW_TYPE]['resources_violated'] = 0
        return file_content

    def _save_monthly_state(self, data: dict, project_id: str, customer: str):
        path = f'{customer}/accounts/monthly/{self.next_month_date}/{project_id}.json'
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        if not self.s3_client.file_exists(bucket_name=metrics_bucket,
                                          key=path + '.gz'):
            _LOG.debug(f'Save monthly metrics for account {project_id}')
            self.s3_client.put_object(
                bucket_name=metrics_bucket,
                object_name=path,
                body=json.dumps(data, separators=(",", ":")))

    def _get_policies_metadata(self, policies):
        for p in policies:
            p = p.replace('epam-', 'ecc-')
            if p not in self.metadata:
                _LOG.debug(f'Querying policy {p}')
                item = self.rule_meta_service.get_latest_meta(
                    p, attributes_to_get=['n', 'r', 'st', 'mi', 'se'])
                if item:
                    self.metadata.update({p: item.get_json()})

    def get_number_of_resources_by_region_and_severity(self,
                                                       content: dict) -> dict:
        """
        Returns number of unique resources within region and severity.
        Expected content structure:
        {
          "policy_name1": {
            "description": str,
            "resource_type": str,
            "resources": {
              "region1": [...], "region2": [...], ...
            }
          },
          "policy_name2": {}
          ...
        }
        """
        result = {}
        filtered_result = {}
        for policy, value in content.items():
            severity = self.metadata.get(policy, {}).get(
                'severity', 'Unknown').capitalize()
            regions_data = value.get('regions_data', {})
            for region, resources in regions_data.items():
                if not resources:
                    continue
                result.setdefault(region, {}).setdefault(
                    'severity_data', {}).setdefault(severity, [])
                resources = resources.get('resources', [])
                for r in resources:
                    result[region]['severity_data'][severity].append(
                        ':'.join(f'{k}:{v}' for k, v in r.items()))

        # filter and save unique resource
        for region, severity_data in result.items():
            for severity, data in severity_data['severity_data'].items():
                filtered_result.setdefault(region, {}).setdefault(
                    'severity_data', {}).setdefault(severity, 0)
                filtered_result[region]['severity_data'][severity] += \
                    len(set(data))

        return filtered_result


TENANT_METRICS = TenantMetrics(
    job_service=SERVICE_PROVIDER.job_service(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    batch_results_service=SERVICE_PROVIDER.batch_results_service(),
    findings_service=SERVICE_PROVIDER.findings_service(),
    rule_report_service=SERVICE_PROVIDER.rule_report_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    rule_meta_service=SERVICE_PROVIDER.rule_meta_service(),
    coverage_service=SERVICE_PROVIDER.coverage_service()
)
