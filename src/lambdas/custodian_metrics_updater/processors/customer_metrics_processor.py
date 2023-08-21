import json
from datetime import datetime, timedelta, date

from dateutil.relativedelta import relativedelta, MO

from helpers import get_logger
from helpers.constants import OVERVIEW_TYPE, DATA_TYPE, START_DATE
from services import SERVICE_PROVIDER
from services.batch_results_service import BatchResultsService
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.findings_service import FindingsService
from services.job_service import JobService
from services.job_statistics_service import JobStatisticsService
from services.modular_service import ModularService
from services.metrics_service import TenantMetricsService, \
    CustomerMetricsService
from services.rule_meta_service import RuleMetaService
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

CLOUDS = ['aws', 'azure', 'google']
SEVERITIES = ['Critical', 'High', 'Medium', 'Low', 'Info']

TENANT_GROUP_METRICS_FILE_PATH = '{customer}/tenants/{date}/{tenant}.json'
TENANT_GROUP_METRICS_FOLDER_PATH = '{customer}/tenants/{date}/'

TYPE_ATTR = 'type'
NEXT_STEP = 'difference'


class CustomerMetrics:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 tenant_metrics_service: TenantMetricsService,
                 customer_metrics_service: CustomerMetricsService,
                 modular_service: ModularService, findings_service: FindingsService,
                 job_statistics_service: JobStatisticsService,
                 job_service: JobService, rule_meta_service: RuleMetaService,
                 batch_results_service: BatchResultsService):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.tenant_metrics_service = tenant_metrics_service
        self.customer_metrics_service = customer_metrics_service
        self.modular_service = modular_service
        self.findings_service = findings_service
        self.job_statistics_service = job_statistics_service
        self.job_service = job_service
        self.batch_results_service = batch_results_service
        self.rule_meta_service = rule_meta_service

        self.today_date = datetime.utcnow().today().date()
        self._date_marker = self.settings_service.get_report_date_marker()
        self.month_first_day = self.today_date.replace(day=1)
        self.last_month_date = (
                self.month_first_day - timedelta(days=1)).replace(day=1)
        self.metadata = {}

        self.CUSTOMER_OVERVIEW = None

    def process_data(self, event):
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        customers = set(
            customer.split('/')[0]
            for customer in self.s3_client.list_dir(bucket_name=metrics_bucket)
        )
        customers_to_process = [customer for customer in customers if
                                not self.current_month_customer_metrics_exist(
                                    customer)]

        for customer in customers_to_process:
            tenant_to_object = {}
            self.CUSTOMER_OVERVIEW = {c: {'total_scanned_tenants': 0,
                                          'last_scan_date': None,
                                          'resources_violated': 0,
                                          'succeeded_scans': 0,
                                          'failed_scans': 0,
                                          'severity_data': {},
                                          'resource_types_data': {},
                                          'total_scans': 0} for c in CLOUDS}

            weekly_data = self._get_weekly_statistics(
                customer, self.last_month_date, self.month_first_day)
            if not weekly_data:
                _LOG.debug(f'There is no data for customer {customer}')
                continue

            for data in weekly_data:
                for acc in (data.tenants or []):
                    if acc not in tenant_to_object:
                        tenant_obj = self._get_or_create_tenant_object(
                            tenant_to_object, acc)
                        if not tenant_obj:
                            _LOG.warning(f'No tenant  with project id {acc}')
                            continue
                        cloud = tenant_obj.cloud.lower()
                        self._update_customer_overview_with_findings(
                            cloud, tenant_to_object.get(acc))
                self._update_customer_overview_with_scan_data(data)

            self._update_customer_overview_with_total_scanned_tenants(
                tenant_to_object)
            items = self._collect_chief_overview_item(
                customer=customer, customer_data=self.CUSTOMER_OVERVIEW)
            self.customer_metrics_service.batch_save([items])

        return {DATA_TYPE: NEXT_STEP, START_DATE: event.get(START_DATE),
                'continuously': event.get('continuously')}

    def _get_or_create_tenant_object(self, tenant_to_object, acc):
        if acc not in tenant_to_object:
            tenant_obj = next(self.modular_service.i_get_tenants_by_acc(
                acc), None)
            if not tenant_obj:
                _LOG.warning(
                    f'Unknown tenant with id {acc}. Skipping...')
                return None
            tenant_to_object[acc] = tenant_obj
        return tenant_to_object.get(acc)

    def _update_customer_overview_with_findings(self, cloud, tenant_obj):
        findings = self._get_last_month_findings(tenant_obj.project)
        self.CUSTOMER_OVERVIEW[cloud]['resources_violated'] += len(
            self.findings_service.unique_resources_from_raw_findings(
                findings))
        severity_type_data = self.get_number_of_resources_by_severity_and_type(
            findings)
        for s, v in severity_type_data.get('severity_data',
                                           {}).items():
            self.CUSTOMER_OVERVIEW[cloud]['severity_data'].setdefault(s, 0)
            self.CUSTOMER_OVERVIEW[cloud]['severity_data'][s] += v
        for t, v in severity_type_data.get('resource_types_data',
                                           {}).items():
            self.CUSTOMER_OVERVIEW[cloud]['resource_types_data'].setdefault(
                t, 0)
            self.CUSTOMER_OVERVIEW[cloud]['resource_types_data'][t] += v

    def _update_customer_overview_with_scan_data(self, data):
        self.CUSTOMER_OVERVIEW[data.cloud]['succeeded_scans'] += \
            data.succeeded
        self.CUSTOMER_OVERVIEW[data.cloud]['failed_scans'] += \
            data.failed
        self.CUSTOMER_OVERVIEW[data.cloud]['total_scans'] += \
            data.failed + data.succeeded

        last_scan = self.CUSTOMER_OVERVIEW[data.cloud]['last_scan_date']
        if not last_scan or last_scan < data.last_scan_date:
            self.CUSTOMER_OVERVIEW[data.cloud]['last_scan_date'] = \
                data.last_scan_date

    def _update_customer_overview_with_total_scanned_tenants(
            self, tenant_to_object):
        for c in CLOUDS:
            if not self.CUSTOMER_OVERVIEW.get(c):
                continue
            self.CUSTOMER_OVERVIEW[c]['total_scanned_tenants'] = \
                len([v for _, v in tenant_to_object.items() if
                     v.cloud == c.upper()])

    def _collect_chief_overview_item(self, customer: str, customer_data: dict):
        overview_item = {
            TYPE_ATTR: OVERVIEW_TYPE.upper(),
            **{c: customer_data.get(c, {}) for c in CLOUDS},
            'customer': customer,
            'date': self.month_first_day.isoformat()}
        for c in CLOUDS:
            if all(self._is_empty(v) for v in overview_item[c].values()):
                overview_item[c] = {}
            elif overview_item[c]['succeeded_scans'] == 0:
                overview_item[c]['resources_violated'] = 0
                overview_item[c]['resource_types_data'] = {}
                overview_item[c]['severity_data'] = {}
        return self.customer_metrics_service.create(overview_item)

    def _get_last_month_findings(self, acc_id: str) -> dict:
        statistics_bucket = self.environment_service.get_statistics_bucket_name()
        file = self.s3_client.get_file_content(
            bucket_name=statistics_bucket, decode=True,
            full_file_name=f'findings/{self.month_first_day.isoformat()}/'
                           f'{acc_id}.json.gz')
        if not file:
            file = self.s3_client.get_file_content(
                bucket_name=statistics_bucket, decode=True,
                full_file_name=f'findings/{self.today_date.isoformat()}/'
                               f'{acc_id}.json.gz')
        if not file:
            file = self.s3_client.get_file_content(
                bucket_name=statistics_bucket, decode=True,
                full_file_name=f'findings/{acc_id}.json.gz')
        return json.loads(file)

    def create_customer_items(self, customer):
        overview_item = {
            TYPE_ATTR: OVERVIEW_TYPE.upper(),
            **{c: self.CUSTOMER_OVERVIEW.get(c, {}) for c in CLOUDS},
            'customer': customer,
            'date': self.month_first_day.isoformat()}

        return self.customer_metrics_service.create(overview_item)

    def _get_weekly_statistics(self, customer: str, from_date: date,
                               to_date: date):
        items = self.job_statistics_service.get_by_customer_and_date(
            customer, from_date.isoformat(), to_date.isoformat())
        return items

    def current_month_customer_metrics_exist(self, customer):
        return self.customer_metrics_service.get_all_types_by_customer_date(
                customer=customer, overview_only=True,
                date=self.month_first_day.isoformat()).get(OVERVIEW_TYPE.upper())

    @staticmethod
    def _is_empty(value):
        if value in [None, 0, {}, []]:
            return True
        return False

    def _get_last_week_scans(self, customer):
        start_week_date = (self.today_date + relativedelta(
            weekday=MO(-1)))

        customer_jobs = self.job_service.get_customer_jobs_between_period(
            end_period=self.today_date, start_period=start_week_date,
            customer=customer, only_succeeded=False, limit=10,
            attributes_to_get=["stopped_at", 'tenant_display_name', "status"])
        customer_ed_jobs = self.batch_results_service.get_between_period_by_customer(
            customer_name=customer, start=start_week_date.isoformat(),
            end=self.today_date.isoformat(), limit=10,
            attributes_to_get=['t', 's', 'jsta']
        )
        return customer_jobs + customer_ed_jobs

    def get_number_of_resources_by_severity_and_type(self,
                                                     content: dict) -> dict:
        result = {}
        for policy, value in content.items():
            policy = policy.replace('epam-', 'ecc-')
            severity = self.metadata.get(policy)
            if not severity:
                _LOG.debug(f'Get metadata for policy {policy}')
                self.metadata[policy] = self.rule_meta_service.\
                    get_latest_meta(policy)
            if not self.metadata.get(policy):
                _LOG.warning(f'Cannot find metadata for rule {policy}')
                severity = 'Unknown'
            else:
                severity = self.metadata.get(policy).get_json().get(
                    'severity', 'Unknown').capitalize()
            resource_type = value.get('resourceType', '')
            regions_data = value.get('resources', {})
            for _, resources in regions_data.items():
                if not resources:
                    continue
                result.setdefault('severity_data', {}).setdefault(severity, [])
                result.setdefault('resource_types_data', {}).setdefault(
                    resource_type, [])
                resources_str = [':'.join([f'{k}:{v}' for k, v in r.items()]) for r in resources]
                result['severity_data'][severity].extend(resources_str)
                result['resource_types_data'][resource_type].extend(resources_str)
        for s, v in result['severity_data'].items():
            result['severity_data'][s] = len(set(v))
        for r, v in result['resource_types_data'].items():
            result['resource_types_data'][r] = len(set(v))
        return result


CUSTOMER_METRICS_DIFF = CustomerMetrics(
    s3_client=SERVICE_PROVIDER.s3(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    tenant_metrics_service=SERVICE_PROVIDER.tenant_metrics_service(),
    customer_metrics_service=SERVICE_PROVIDER.customer_metrics_service(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    findings_service=SERVICE_PROVIDER.findings_service(),
    job_statistics_service=SERVICE_PROVIDER.job_statistics_service(),
    job_service=SERVICE_PROVIDER.job_service(),
    rule_meta_service=SERVICE_PROVIDER.rule_meta_service(),
    batch_results_service=SERVICE_PROVIDER.batch_results_service()
)
