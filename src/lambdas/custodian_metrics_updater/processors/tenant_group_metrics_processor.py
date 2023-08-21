import heapq
import json
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta, SU, MO

from helpers import get_logger
from helpers.constants import CUSTOMER_ATTR, NAME_ATTR, VALUE_ATTR, \
    OVERVIEW_TYPE, COMPLIANCE_TYPE, RESOURCES_TYPE, TENANT_ATTR, DATA_TYPE, \
    LAST_SCAN_DATE, SEVERITY_DATA_ATTR, RESOURCE_TYPES_DATA_ATTR, \
    TENANT_NAME_ATTR, ID_ATTR, TENANT_DISPLAY_NAME_ATTR, AVERAGE_DATA_ATTR, \
    ACTIVATED_REGIONS_ATTR, ACCOUNT_ID_ATTR, ATTACK_VECTOR_TYPE, START_DATE
from helpers.time_helper import utc_datetime
from helpers.utils import get_last_element
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.job_statistics_service import JobStatisticsService
from services.metrics_service import CustomerMetricsService
from services.environment_service import EnvironmentService
from services.modular_service import ModularService
from services.setting_service import SettingsService
from services.metrics_service import TenantMetricsService

_LOG = get_logger(__name__)

CLOUDS = ['aws', 'azure', 'google']
SEVERITIES = ['Critical', 'High', 'Medium', 'Low', 'Info']

TENANT_METRICS_PATH = '{customer}/accounts/{date}'
TENANT_GROUP_METRICS_FILE_PATH = '{customer}/tenants/{date}/{tenant}.json'

TOP_TENANT_LENGTH = 10
TOP_CLOUD_LENGTH = 5

NEXT_STEP = 'difference'
NEXT_STEP_CUSTOMER = 'customer'
NEXT_STEP_TENANT_GROUP = 'tenant_groups'
TYPE_ATTR = 'type'


class TenantMetrics:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 tenant_metrics_service: TenantMetricsService,
                 customer_metrics_service: CustomerMetricsService,
                 modular_service: ModularService,
                 job_statistics_service: JobStatisticsService):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.tenant_metrics_service = tenant_metrics_service
        self.customer_metrics_service = customer_metrics_service
        self.modular_service = modular_service
        self.job_statistics_service = job_statistics_service

        self.TOP_RESOURCES_BY_TENANT = []
        self.TOP_RESOURCES_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_COMPLIANCE_BY_TENANT = []
        self.TOP_COMPLIANCE_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_ATTACK_BY_TENANT = []
        self.TOP_ATTACK_BY_CLOUD = {c: [] for c in CLOUDS}
        self.CUSTOMER_GENERAL = {
            c: {"total_scanned_tenants": 0,
                "last_scan_date": None} for c in CLOUDS
        }
        self.CUSTOMER_COMPLIANCE = None
        self.CUSTOMER_ATTACK = {c: {} for c in CLOUDS}
        self.mean_coverage = {**{c: [] for c in CLOUDS}}

        self.today_date = datetime.utcnow().today()
        self.today_midnight = datetime.combine(self.today_date,
                                               datetime.min.time())
        self.yesterday = (self.today_date - timedelta(days=1)).date()
        self.next_month_date = (self.today_date.date().replace(day=1) +
                                relativedelta(months=1)).isoformat()
        self.month_first_day = self.today_date.date().replace(
            day=1).isoformat()
        self.prev_month_first_day = (
                self.today_date - relativedelta(months=1)).replace(day=1).date().isoformat()
        self.TO_UPDATE_MARKER = False

        self._date_marker = self.settings_service.get_report_date_marker()
        self.current_week_date = self._date_marker.get('current_week_date')
        self.last_week_date = self._date_marker.get('last_week_date')

        self._tenant_metrics_exists = None
        self._cust_metrics_exists = None
        self.next_step = NEXT_STEP
        self.weekly_scan_statistics = {}
        if self.today_date.weekday() == 0:
            self.start_week_date = (self.today_date + relativedelta(
                weekday=MO(-2))).date().isoformat()
        else:
            self.start_week_date = (self.today_date + relativedelta(
                weekday=MO(-1))).date().isoformat()

    def process_data(self, event):
        customer_metrics_level = event.get('level')
        if customer_metrics_level and customer_metrics_level == 'customer':
            end_date = self.month_first_day
            s3_object_date = f'monthly/{self.next_month_date}'
        elif self.current_week_date <= self.yesterday.isoformat():
            end_date = self.today_midnight
            self.TO_UPDATE_MARKER = True
            s3_object_date = (self.today_date + relativedelta(
                weekday=SU(0))).date().isoformat()
        else:
            end_date = datetime.utcnow()
            s3_object_date = self.current_week_date

        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        customers = set(
            customer.split('/')[0] for customer in
            self.s3_client.list_dir(bucket_name=metrics_bucket)
        )

        for customer in customers:
            _LOG.debug(f'Processing customer {customer}')
            self._reset_variables()
            tenant_group_to_files_mapping = {}

            account_filenames = list(self.s3_client.list_dir(
                bucket_name=metrics_bucket,
                key=TENANT_METRICS_PATH.format(customer=customer,
                                               date=s3_object_date)))
            for filename in account_filenames:
                if not filename.endswith('.json') and not \
                        filename.endswith('.json.gz'):
                    continue

                project_id = get_last_element(filename.replace(
                    '.json', '').replace('.gz', ''), '/')
                if not project_id:
                    _LOG.warning(f'Cannot get project id from file {filename}')
                    continue

                tenant_obj = list(self.modular_service.i_get_tenants_by_acc(
                    acc=project_id, attrs_to_get=['dntl', 'n']))
                if not tenant_obj:
                    _LOG.warning(f'Unknown tenant with project id '
                                 f'{project_id}. Skipping...')
                    continue

                if len(tenant_obj) > 1:
                    tenant_names = ", ".join([t.name for t in tenant_obj])
                    _LOG.warning(f'There is more than one tenant with account '
                                 f'ID \'{project_id}\': {tenant_names}\n'
                                 f'Processing the first one.')

                # group tenants with the same display name
                tenant_display_name = tenant_obj[0].display_name_to_lower
                tenant_group_to_files_mapping.setdefault(
                    tenant_display_name, []).append(filename)

            if not tenant_group_to_files_mapping:
                _LOG.warning(
                    f'Nothing to process: no tenant metrics for customer '
                    f'{customer}; date: {end_date}')

            for tenant_dn, filenames in tenant_group_to_files_mapping.items():
                _LOG.debug(f'Processing tenant group {tenant_dn}')
                compressed_metrics = {
                    OVERVIEW_TYPE: {c: {} for c in CLOUDS},
                    COMPLIANCE_TYPE: {c: {} for c in CLOUDS},
                    ATTACK_VECTOR_TYPE: {c: {'data': []} for c in CLOUDS}
                }
                general_tenant_info = {}
                tenant_group_compliance_mapping = {c: [] for c in CLOUDS}
                tenant_group_overview_mapping = {c: [] for c in CLOUDS}
                tenant_group_resources_mapping = {c: [] for c in CLOUDS}
                tenant_group_attack_mapping = {c: [] for c in CLOUDS}

                for tenant in filenames:
                    tenant_item = {}
                    _LOG.debug(
                        f'Processing tenant {get_last_element(tenant, "/")} '
                        f'within tenant group {tenant_dn}')
                    tenant_content = self.s3_client.get_json_file_content(
                        bucket_name=metrics_bucket, full_file_name=tenant)
                    cloud = tenant_content.pop('cloud', 'unknown').lower()
                    _id = tenant_content.pop(ID_ATTR, None)
                    tenant_name = tenant_content.pop(TENANT_NAME_ATTR, None)
                    last_scan = tenant_content.get(LAST_SCAN_DATE)
                    activated_regions = tenant_content.get(
                        ACTIVATED_REGIONS_ATTR)

                    self._update_general_customer_data(cloud, last_scan)

                    for _type in [COMPLIANCE_TYPE, OVERVIEW_TYPE,
                                  RESOURCES_TYPE, ATTACK_VECTOR_TYPE]:
                        tenant_item[_type] = tenant_content.get(_type)

                    self._calculate_resources(tenant_content, tenant_item)
                    self._process_attack_vector_metrics(
                        tenant_content, tenant_item, cloud, compressed_metrics)

                    if not general_tenant_info:
                        general_tenant_info.update({
                            CUSTOMER_ATTR: customer,
                            'from': tenant_content.get('from'),
                            'to': tenant_content.get('to'),
                            TENANT_DISPLAY_NAME_ATTR: tenant_dn
                        })

                    regions_data = tenant_item[OVERVIEW_TYPE]['regions_data']
                    compliance_data = tenant_item[COMPLIANCE_TYPE]
                    for t in (OVERVIEW_TYPE, COMPLIANCE_TYPE,
                              ATTACK_VECTOR_TYPE):
                        self._add_base_info_to_compressed_metrics(
                            cloud, t, last_scan, activated_regions,
                            tenant_name, _id, compressed_metrics)
                    self._process_compliance_metrics(cloud, compliance_data,
                                                     compressed_metrics)
                    self._process_overview_metrics(
                        cloud, regions_data, tenant_content.get(
                            OVERVIEW_TYPE), compressed_metrics)

                    for mapping, _type in [
                        (tenant_group_compliance_mapping, COMPLIANCE_TYPE),
                        (tenant_group_overview_mapping, OVERVIEW_TYPE),
                        (tenant_group_resources_mapping, RESOURCES_TYPE),
                        (tenant_group_attack_mapping, ATTACK_VECTOR_TYPE)
                    ]:
                        item = {
                            ACCOUNT_ID_ATTR: _id,
                            TENANT_NAME_ATTR: tenant_name,
                            LAST_SCAN_DATE: last_scan,
                            ACTIVATED_REGIONS_ATTR: activated_regions
                        }
                        content = tenant_item.get(_type)
                        if isinstance(content, dict):
                            item.update({**content})
                        elif _type == ATTACK_VECTOR_TYPE:
                            item.update({'mitre_data': content})
                        else:
                            item.update({'data': content})

                        mapping.setdefault(cloud, []).append(item)

                compressed_metrics.update({**general_tenant_info})
                tenant_group_data = {
                    **general_tenant_info,
                    COMPLIANCE_TYPE: tenant_group_compliance_mapping,
                    OVERVIEW_TYPE: tenant_group_overview_mapping,
                    RESOURCES_TYPE: tenant_group_resources_mapping,
                    ATTACK_VECTOR_TYPE: tenant_group_attack_mapping
                }

                self.save_weekly_job_stats(customer,
                                           tenant_group_overview_mapping)

                today_date = self.today_date.date().isoformat()
                if today_date == self.month_first_day:
                    self.save_weekly_job_stats(customer,
                                               tenant_group_overview_mapping,
                                               end_date=today_date)

                self.s3_client.put_object(
                    bucket_name=metrics_bucket,
                    object_name=TENANT_GROUP_METRICS_FILE_PATH.format(
                        customer=customer, date=s3_object_date,
                        tenant=tenant_dn),
                    body=json.dumps(tenant_group_data, separators=(",", ":")))

                if customer_metrics_level:
                    self.add_department_metrics(tenant_dn, compressed_metrics,
                                                general_tenant_info)

            # save top tenants and new date marker
            if self.current_month_tenant_metrics_not_exist(customer):
                if not customer_metrics_level:
                    self.next_step = NEXT_STEP_TENANT_GROUP
                else:
                    _LOG.debug('Saving top tenants to the table')
                    top_items = self.create_top_items()
                    self.tenant_metrics_service.batch_save(top_items)
                    self.next_step = NEXT_STEP_CUSTOMER

            if self.current_month_customer_metrics_not_exist(customer) \
                    and any([self.CUSTOMER_COMPLIANCE, any(self.CUSTOMER_ATTACK.values())]):
                if not customer_metrics_level:
                    self.next_step = NEXT_STEP_TENANT_GROUP
                else:
                    _LOG.debug('Saving customer metrics to the table')
                    customer_items = self.create_customer_items(customer)
                    self.customer_metrics_service.batch_save(customer_items)
                    self.next_step = NEXT_STEP_CUSTOMER

            for cid, data in self.weekly_scan_statistics.items():
                _LOG.debug(f'Saving weekly statistics for customer {customer}')
                for c in CLOUDS:
                    dict_to_save = data.get(c)
                    if not dict_to_save:
                        continue
                    dict_to_save['customer_name'] = cid
                    dict_to_save['cloud'] = c
                    dict_to_save['tenants'] = list(set(dict_to_save.get(
                        'tenants', [])))
                    self.job_statistics_service.create(dict_to_save).save()

        return {DATA_TYPE: self.next_step, START_DATE: event.get(START_DATE),
                'continuously': event.get('continuously'),
                'level': 'customer' if self.next_step == NEXT_STEP_TENANT_GROUP else None}

    def save_weekly_job_stats(self, customer, tenant_group_overview_mapping,
                              end_date=None):
        start_date = self.last_week_date
        if not end_date:
            end_date = self.last_week_date
            start_date = (utc_datetime(
                self.last_week_date) - timedelta(days=7)).date().isoformat()

        if self.job_statistics_service.get_by_customer_and_date(
                customer, start_date, end_date):
            return

        self.weekly_scan_statistics.setdefault(customer, {}). \
            setdefault('customer_name', customer)
        weekly_stats = self.weekly_scan_statistics[customer]
        for cloud in CLOUDS:
            data = tenant_group_overview_mapping.get(cloud)
            if not data:
                continue
            data = data[0]

            weekly_stats.setdefault(cloud, {})
            weekly_stats[cloud]['from_date'] = start_date
            weekly_stats[cloud]['to_date'] = end_date
            weekly_stats[cloud]['failed'] = 0
            weekly_stats[cloud]['succeeded'] = 0
            weekly_stats[cloud][LAST_SCAN_DATE] = self._get_last_scan_date(
                data[LAST_SCAN_DATE],
                weekly_stats[cloud].get(LAST_SCAN_DATE))
            weekly_stats[cloud]['failed'] += data['failed_scans']
            weekly_stats[cloud]['succeeded'] += data['succeeded_scans']
            weekly_stats[cloud].setdefault('tenants', []).append(
                data['account_id'])

    def create_top_items(self):
        top_metrics_by_cloud = []  # top resources, compliance, attack by cloud
        top_metrics_by_tenant = []  # top resources, compliance, attack by tenant

        self._sort_top_tenants_by_attack()
        for cloud in CLOUDS:
            self._sort_top_tenants_by_attack_by_cloud(cloud)

        for metrics_type, defining_attr in [('RESOURCES', 'all_resources'), (
                'COMPLIANCE', 'mean_coverage'), ('ATTACK', 'index')]:
            if not self._tenant_metrics_exists.get(
                    f'{metrics_type}_BY_TENANT'):
                for t in getattr(self, f'TOP_{metrics_type}_BY_TENANT'):
                    if all(not t.get(c) for c in CLOUDS):
                        continue

                    top_metrics_by_tenant.extend([
                        self.tenant_metrics_service.create({
                            TENANT_DISPLAY_NAME_ATTR: t.get(TENANT_ATTR),
                            CUSTOMER_ATTR: t.get(CUSTOMER_ATTR),
                            'date': self.month_first_day,
                            TYPE_ATTR: f'{metrics_type}_BY_TENANT',
                            **{cloud: t.get(cloud) for cloud in CLOUDS},
                            'defining_attribute': t.get(defining_attr)
                        })
                    ])

            if self._tenant_metrics_exists.get(f'{metrics_type}_BY_CLOUD'):
                continue
            for cloud in CLOUDS:
                top_metrics_by_cloud.extend([
                    self.tenant_metrics_service.create({
                        TENANT_DISPLAY_NAME_ATTR: t.get(TENANT_ATTR),
                        CUSTOMER_ATTR: t.get(CUSTOMER_ATTR),
                        'date': self.month_first_day,
                        TYPE_ATTR: f'{metrics_type}_BY_CLOUD',
                        cloud: t.get(cloud),
                        'defining_attribute': t.get(defining_attr)
                    }) for t in
                    getattr(self, f'TOP_{metrics_type}_BY_CLOUD').get(
                        cloud, [])
                ])

        _LOG.debug(f'Top by tenant: {top_metrics_by_tenant}')
        _LOG.debug(f'Top by cloud: {top_metrics_by_cloud}')
        return top_metrics_by_tenant + top_metrics_by_cloud

    @staticmethod
    def _calculate_resources(tenant_content: dict, tenant_item: dict):
        reduced_account_resources = []
        region_resource_type_mapping = {}  # result data for resource report
        for resource in tenant_content.get(RESOURCES_TYPE):
            res_type = resource.get('resource_type', {})
            regions_data = resource.pop('regions_data', {})
            reduced_regions_data = {}
            for region, resources in regions_data.items():
                if not resources or not resources.get(RESOURCES_TYPE):
                    continue
                resources = resources.get(RESOURCES_TYPE, [])
                region_resource_type_mapping.setdefault(region, {})
                region_resource_type_mapping[region].setdefault(res_type, [])
                for r in resources:
                    # loop for pre-filtering resources to find unique ones
                    if not r:
                        continue
                    region_resource_type_mapping[region][res_type].append(
                        ':'.join(f'{k}:{v}' for k, v in r.items()))
                reduced_regions_data.update(
                    {region: {'total_violated_resources': len(resources)}})

            reduced_account_resources.append(
                {**resource, 'regions_data': reduced_regions_data})

        # add new field to overview report and compress old field for
        # resources report
        if tenant_item[OVERVIEW_TYPE]['regions_data']:
            for region, resources in region_resource_type_mapping.items():
                tenant_item[OVERVIEW_TYPE]['regions_data'].setdefault(
                    region, {}).setdefault(RESOURCE_TYPES_DATA_ATTR, {})
                for r_type, r in resources.items():
                    tenant_item[OVERVIEW_TYPE]['regions_data'][region][
                        RESOURCE_TYPES_DATA_ATTR][r_type] = len(set(r))
        tenant_item[RESOURCES_TYPE] = reduced_account_resources

    def add_department_metrics(self, tenant_dn, compressed_metrics,
                               general_tenant_info):
        if not self.current_month_tenant_metrics_not_exist(
                general_tenant_info[CUSTOMER_ATTR]):
            _LOG.debug('Department metrics already exist for this month')
            return
        self._process_department_overview_metrics(
            tenant_dn, compressed_metrics[OVERVIEW_TYPE], general_tenant_info)
        self._process_department_compliance_metrics(
            tenant_dn, compressed_metrics[COMPLIANCE_TYPE],
            general_tenant_info)
        self._process_department_attack_metrics(tenant_dn, compressed_metrics[
            ATTACK_VECTOR_TYPE], general_tenant_info)

    def _process_department_overview_metrics(self, tenant_dn,
                                             overview_metrics,
                                             general_tenant_info):
        amount = sum([v.get('resources_violated', 0) for v in
                      overview_metrics.values()])
        tenant_data = {**general_tenant_info, **overview_metrics}
        self.add_tenant_to_top_by_resources(tenant_display_name=tenant_dn,
                                            amount=amount,
                                            tenant_data=tenant_data)

        for cloud, value in overview_metrics.items():
            if value.get('resources_violated'):
                tenant_data = {**general_tenant_info, cloud: value}
                self.add_tenant_to_top_by_resources_by_cloud(
                    tenant_name=tenant_dn, amount=value['resources_violated'],
                    tenant_data=tenant_data, cloud=cloud)

    def _process_department_compliance_metrics(self, tenant_dn,
                                               compliance_metrics,
                                               general_tenant_info):
        tenant_mean_coverage = []
        for cloud, value in compliance_metrics.items():
            if not value:
                continue
            coverages = [percent['value'] for percent in
                         value[AVERAGE_DATA_ATTR]]
            tenant_mean_coverage.extend(coverages)
            tenant_data = {**general_tenant_info, cloud: value}
            self.add_tenant_to_top_by_compliance_by_cloud(
                tenant_name=tenant_dn,
                coverage=sum(coverages) / len(coverages),
                tenant_data=tenant_data, cloud=cloud)
            self.mean_coverage[cloud].extend(coverages)

        tenant_coverage = sum(tenant_mean_coverage) / len(tenant_mean_coverage)
        tenant_data = {**general_tenant_info, **compliance_metrics}
        self.add_tenant_to_top_by_compliance(tenant_display_name=tenant_dn,
                                             coverage=tenant_coverage,
                                             tenant_data=tenant_data)

    def _process_department_attack_metrics(self, tenant_dn, attack_metrics,
                                           general_tenant_info):
        attack_by_tenant = {c: v.get('data', []) for c, v in
                            attack_metrics.items()}
        attack_by_cloud = {c: [] for c in self.TOP_ATTACK_BY_CLOUD}
        for cloud, value in attack_metrics.items():
            if value.get('data'):
                tenant_data = {**general_tenant_info, cloud: value}
                attack_by_cloud[cloud].append(
                    {TENANT_ATTR: tenant_dn, **tenant_data,
                     'sort_by': self.attack_overall_severity.copy()})

        self.TOP_ATTACK_BY_TENANT.append(
            {TENANT_ATTR: tenant_dn, **general_tenant_info,
             'sort_by': self.attack_overall_severity.copy(),
             **attack_by_tenant})
        for cloud, attacks in attack_by_cloud.items():
            self.TOP_ATTACK_BY_CLOUD[cloud].extend(attacks)

    @staticmethod
    def _sort_key(item, key):
        return item[key]

    def _add_tenant_to_top(self, tenant_name, value, tenant_data, top_list,
                           top_length, sort_key, reverse=False):
        top_list.append(
            {TENANT_ATTR: tenant_name, sort_key: value, **tenant_data})
        if len(top_list) > top_length:
            top_list = heapq.nlargest(
                top_length, top_list,
                key=lambda item: self._sort_key(item,
                                                sort_key)) if reverse else \
                heapq.nsmallest(
                    top_length, top_list,
                    key=lambda item: self._sort_key(item, sort_key))
        return top_list

    def add_tenant_to_top_by_resources(self, tenant_display_name, amount,
                                       tenant_data):
        self.TOP_RESOURCES_BY_TENANT = self._add_tenant_to_top(
            tenant_display_name, amount, tenant_data,
            self.TOP_RESOURCES_BY_TENANT, TOP_TENANT_LENGTH,
            'all_resources', reverse=True)

    def add_tenant_to_top_by_resources_by_cloud(self, tenant_name, amount,
                                                tenant_data, cloud):
        self.TOP_RESOURCES_BY_CLOUD[cloud] = self._add_tenant_to_top(
            tenant_name, amount, tenant_data,
            self.TOP_RESOURCES_BY_CLOUD[cloud],
            TOP_CLOUD_LENGTH, 'all_resources', reverse=True)

    def add_tenant_to_top_by_compliance(self, tenant_display_name, coverage,
                                        tenant_data):
        self.TOP_COMPLIANCE_BY_TENANT = self._add_tenant_to_top(
            tenant_display_name, coverage, tenant_data,
            self.TOP_COMPLIANCE_BY_TENANT, TOP_TENANT_LENGTH,
            'mean_coverage')

    def add_tenant_to_top_by_compliance_by_cloud(self, tenant_name, coverage,
                                                 tenant_data, cloud):
        self.TOP_COMPLIANCE_BY_CLOUD[cloud] = self._add_tenant_to_top(
            tenant_name, coverage, tenant_data,
            self.TOP_COMPLIANCE_BY_CLOUD[cloud],
            TOP_CLOUD_LENGTH, 'mean_coverage')

    def _sort_top_tenants_by_attack(self):
        resulted_top = []

        index = 1
        for tenant in self.TOP_ATTACK_BY_TENANT:
            sorted_tenants = {}
            overall_severity = {}
            for _ in [c for c in CLOUDS if tenant.get(c)]:
                for severity in SEVERITIES:
                    if severity_value := tenant['sort_by'].get(severity, 0):
                        overall_severity[severity] = overall_severity.get(
                            severity, 0) + severity_value

            for severity in SEVERITIES:
                if severity in overall_severity:
                    sorted_tenants.setdefault(severity, []).append({
                        **tenant, 'sort_by': overall_severity[severity]})
                    break

            for s in SEVERITIES:
                tenants = sorted_tenants.get(s, {})
                if tenants:
                    tenants = sorted(tenants, key=lambda v: v['sort_by'],
                                     reverse=True)
                    for t in tenants[:TOP_TENANT_LENGTH]:
                        # BE must sort this tenants by its place in top
                        t['index'] = index
                        index += 1
                        resulted_top.append(t)
                    if len(resulted_top) >= TOP_TENANT_LENGTH:
                        self.TOP_ATTACK_BY_TENANT = resulted_top[
                                                    :TOP_TENANT_LENGTH]
                        return

        # if len(resulted_top) less than TOP_TENANT_LENGTH
        self.TOP_ATTACK_BY_TENANT = resulted_top

    def _sort_top_tenants_by_attack_by_cloud(self, cloud):
        resulted_top = []

        index = 1
        for tenant in self.TOP_ATTACK_BY_CLOUD[cloud]:
            sorted_tenants = {}
            if tenant.get(cloud):
                for severity in SEVERITIES:
                    severity_value = tenant['sort_by'].get(severity, 0)
                    if severity_value:
                        sorted_tenants.setdefault(severity, []).append({
                            **tenant, 'sort_by': severity_value})
                        break

            for s in SEVERITIES:
                tenants = sorted_tenants.get(s, {})
                if tenants:
                    tenants = sorted(tenants, key=lambda v: v['sort_by'],
                                     reverse=True)
                    for t in tenants[:TOP_CLOUD_LENGTH]:
                        # BE must sort this tenants by its place in top
                        t['index'] = index
                        index += 1
                        resulted_top.append(t)
                    if len(resulted_top) >= TOP_CLOUD_LENGTH:
                        self.TOP_ATTACK_BY_CLOUD[cloud] = resulted_top[
                                                          :TOP_CLOUD_LENGTH]
                        return

        # if len(resulted_top) less than TOP_TENANT_LENGTH
        self.TOP_ATTACK_BY_CLOUD[cloud] = resulted_top

    @staticmethod
    def is_month_passed():
        today = datetime.today().date()
        return today.day == 1

    def current_month_customer_metrics_not_exist(self, customer):
        if self._cust_metrics_exists is None:
            self._cust_metrics_exists = self.customer_metrics_service.get_all_types_by_customer_date(
                customer=customer,
                date=datetime.today().date().replace(day=1).isoformat())
        return not all(self._cust_metrics_exists.values())

    def current_month_tenant_metrics_not_exist(self, customer):
        if self._tenant_metrics_exists is None:
            self._tenant_metrics_exists = self.tenant_metrics_service.get_all_types_by_customer_date(
                date=datetime.today().date().replace(day=1).isoformat(),
                customer=customer)
        return not all(self._tenant_metrics_exists.values())

    def _update_general_customer_data(self, cloud: str, scan_date: str):
        """
        Updates last scan date and calculates amount of customer tenants
        """
        self.CUSTOMER_GENERAL[cloud]['total_scanned_tenants'] += 1
        last_scan_date = self.CUSTOMER_GENERAL[cloud]['last_scan_date']
        self.CUSTOMER_GENERAL[cloud]['last_scan_date'] = \
            self._get_last_scan_date(scan_date, last_scan_date)

    @staticmethod
    def _get_last_scan_date(new_scan_date: str, last_scan_date: str = None):
        if not last_scan_date:
            return new_scan_date
        last_scan_datetime = utc_datetime(last_scan_date, utc=False)
        scan_datetime = utc_datetime(new_scan_date, utc=False)
        if last_scan_datetime < scan_datetime:
            return new_scan_date
        return last_scan_date

    def create_customer_items(self, customer):
        items = []

        compliance_item = self.create_compliance_item(customer)

        if not self._cust_metrics_exists.get(ATTACK_VECTOR_TYPE):
            attack_vector_item = self.create_attack_vector_item(customer)
            if attack_vector_item:
                items.append(self.customer_metrics_service.create(
                    attack_vector_item))

        if not self._cust_metrics_exists.get(COMPLIANCE_TYPE):
            for cloud in CLOUDS:
                if compliance_item[cloud]:
                    compliance_item[cloud].update(self.CUSTOMER_GENERAL[cloud])
            items.append(self.customer_metrics_service.create(compliance_item))

        return items

    def create_compliance_item(self, customer):
        for cloud in CLOUDS:
            updated_standards = [
                {NAME_ATTR: standard, VALUE_ATTR: sum(value) / len(value)}
                for standard, value in
                self.CUSTOMER_COMPLIANCE.setdefault(cloud, {}). \
                    setdefault(AVERAGE_DATA_ATTR, {}).items()
            ]
            self.CUSTOMER_COMPLIANCE[cloud][
                AVERAGE_DATA_ATTR] = updated_standards

        return {
            TYPE_ATTR: COMPLIANCE_TYPE.upper(),
            **{cloud: self.CUSTOMER_COMPLIANCE.get(cloud, {}) for cloud in
               CLOUDS},
            CUSTOMER_ATTR: customer,
            'date': self.month_first_day,
            'average': {
                cloud: sum(self.mean_coverage[cloud]) / len(
                    self.mean_coverage[cloud])
                for cloud in CLOUDS if self.mean_coverage.get(cloud)
            }
        }

    def create_attack_vector_item(self, customer):
        cloud_data = {
                cloud: {'data': [
                    {**v, 'tactic': k} for k, v in
                    self.CUSTOMER_ATTACK.get(cloud, {}).items()]}
                for cloud in CLOUDS
            }
        if not any([i['data'] for i in cloud_data.values()]):
            return {}

        item = {
            TYPE_ATTR: ATTACK_VECTOR_TYPE.upper(),
            **cloud_data,
            CUSTOMER_ATTR: customer,
            'date': self.month_first_day
        }
        return item

    def _process_compliance_metrics(self, cloud, compliance_data,
                                    compressed_metrics):
        _LOG.debug('Process metrics for compliance report')
        coverage_data = compliance_data.get(AVERAGE_DATA_ATTR)
        if not coverage_data:
            coverage_data = compliance_data.get('regions_data')[0].get(
                'standards_data', [])
        if coverage_data:
            self.CUSTOMER_COMPLIANCE[cloud].setdefault(AVERAGE_DATA_ATTR, {})
            for standard in coverage_data:
                self.CUSTOMER_COMPLIANCE[cloud][AVERAGE_DATA_ATTR]. \
                    setdefault(standard[NAME_ATTR], []).append(
                    standard[VALUE_ATTR])
        compressed_metrics[COMPLIANCE_TYPE][cloud].setdefault(
            AVERAGE_DATA_ATTR, coverage_data)

    @staticmethod
    def _process_overview_metrics(cloud, regions_data, overview_data,
                                  compressed_metrics):
        _LOG.debug('Process metrics for overview report')
        for key in ('total_scans', 'succeeded_scans', 'failed_scans',
                    'resources_violated'):
            key_value = overview_data.get(key, 0)
            compressed_metrics[OVERVIEW_TYPE][cloud].setdefault(key, 0)
            compressed_metrics[OVERVIEW_TYPE][cloud][key] += key_value

        for region, data in regions_data.items():
            for name, value in data.get(RESOURCE_TYPES_DATA_ATTR, {}).items():
                compressed_metrics[OVERVIEW_TYPE][cloud].setdefault(
                    RESOURCE_TYPES_DATA_ATTR, {}).setdefault(name, 0)
                compressed_metrics[OVERVIEW_TYPE][cloud][
                    RESOURCE_TYPES_DATA_ATTR][name] += value

            for s, v in data.get(SEVERITY_DATA_ATTR, {}).items():
                compressed_metrics[OVERVIEW_TYPE][cloud].setdefault(
                    SEVERITY_DATA_ATTR, {}).setdefault(s, 0)
                compressed_metrics[OVERVIEW_TYPE][cloud][
                    SEVERITY_DATA_ATTR][s] += v

    def _process_attack_vector_metrics(self, tenant_content, tenant_item,
                                       cloud, compressed_metrics):
        _LOG.debug('Process metrics for attack vector report')
        reduced_attack_metrics = []
        self.attack_overall_severity = {}
        compressed_attack = compressed_metrics[ATTACK_VECTOR_TYPE][cloud][
            'data']
        for tactic in tenant_content.get(ATTACK_VECTOR_TYPE):
            tactic_name = tactic.get('tactic')
            tactic_id = tactic.get('tactic_id')
            tactic_item = {
                'tactic_id': tactic_id,
                'tactic': tactic_name,
                'techniques_data': []
            }
            tactic_severity = []
            for tech in tactic.get('techniques_data', []):
                technique_item = {
                    'technique_id': tech.get('technique_id'),
                    'technique': tech.get('technique'),
                    'regions_data': {}
                }
                for region, resource in tech.get('regions_data', {}).items():
                    severity_sum = {}
                    for data in resource.get('resources'):
                        severity = data.get('severity')
                        severity_sum[severity] = severity_sum.get(
                            severity, 0) + 1
                    technique_item['regions_data'].setdefault(
                        region, {'severity_data': {}})[
                        'severity_data'] = severity_sum
                    tactic_severity.append(severity_sum)
                tactic_item['techniques_data'].append(technique_item)

            severity_data = {}
            [severity_data.update({s: severity_data.get(s, 0) + v})
             for t in tactic_severity for s, v in t.items()]
            compressed_attack.append({
                'tactic_id': tactic_id,
                'tactic': tactic_name,
                'severity_data': severity_data
            })

            for severity_name, value in severity_data.items():
                self.attack_overall_severity.setdefault(severity_name, 0)
                self.attack_overall_severity[severity_name] += value

                self.CUSTOMER_ATTACK[cloud].setdefault(
                    tactic_name, {
                        'tactic_id': tactic_id, 'severity_data': {}})[
                    'severity_data'].setdefault(severity_name, 0)
                self.CUSTOMER_ATTACK[cloud][tactic_name]['severity_data'][
                    severity_name] += value

            reduced_attack_metrics.append(tactic_item)

        tenant_item[ATTACK_VECTOR_TYPE] = reduced_attack_metrics

    @staticmethod
    def _add_base_info_to_compressed_metrics(cloud, report_type, last_scan,
                                             activated_regions, tenant_name,
                                             _id, compressed_metrics):
        compressed_metrics[report_type][cloud][LAST_SCAN_DATE] = last_scan
        compressed_metrics[report_type][cloud][ACTIVATED_REGIONS_ATTR] = \
            activated_regions
        compressed_metrics[report_type][cloud][TENANT_NAME_ATTR] = \
            tenant_name
        compressed_metrics[report_type][cloud][ACCOUNT_ID_ATTR] = _id

    def _reset_variables(self):
        _LOG.debug('Resetting department and c-level variables')
        self.CUSTOMER_COMPLIANCE = {c: {} for c in CLOUDS}
        self.CUSTOMER_GENERAL = {
            c: {"total_scanned_tenants": 0,
                "last_scan_date": None} for c in CLOUDS
        }
        self.TOP_RESOURCES_BY_TENANT = []
        self.TOP_RESOURCES_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_COMPLIANCE_BY_TENANT = []
        self.TOP_COMPLIANCE_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_ATTACK_BY_TENANT = []
        self.TOP_ATTACK_BY_CLOUD = {c: [] for c in CLOUDS}
        self._tenant_metrics_exists = None
        self._cust_metrics_exists = None
        self.weekly_scan_statistics = {}


TENANT_GROUP_METRICS = TenantMetrics(
    s3_client=SERVICE_PROVIDER.s3(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    tenant_metrics_service=SERVICE_PROVIDER.tenant_metrics_service(),
    customer_metrics_service=SERVICE_PROVIDER.customer_metrics_service(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    job_statistics_service=SERVICE_PROVIDER.job_statistics_service()
)
