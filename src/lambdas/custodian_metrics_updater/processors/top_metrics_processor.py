import copy
import heapq
from datetime import datetime, date
from functools import cmp_to_key

from dateutil.relativedelta import relativedelta
from modular_sdk.modular import Modular

from helpers import get_logger, hashable
from helpers.constants import CUSTOMER_ATTR, NAME_ATTR, VALUE_ATTR, END_DATE, \
    OVERVIEW_TYPE, COMPLIANCE_TYPE, RESOURCES_TYPE, TENANT_ATTR, DATA_TYPE, \
    TENANT_DISPLAY_NAME_ATTR, AVERAGE_DATA_ATTR, ATTACK_VECTOR_TYPE, \
    FINOPS_TYPE, OUTDATED_TENANTS
from helpers.reports import keep_highest, severity_cmp
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.job_statistics_service import JobStatisticsService
from services.metrics_service import CustomerMetricsService
from services.metrics_service import TenantMetricsService

_LOG = get_logger(__name__)

CLOUDS = ['aws', 'azure', 'google']
SEVERITIES = ['Critical', 'High', 'Medium', 'Low', 'Info']

TENANT_METRICS_PATH = '{customer}/accounts/{date}'
TENANT_GROUP_METRICS_PATH = '{customer}/tenants/{date}'

TOP_TENANT_LENGTH = 10
TOP_CLOUD_LENGTH = 5

NEXT_STEP = 'difference'
TYPE_ATTR = 'type'


class TopMetrics:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 tenant_metrics_service: TenantMetricsService,
                 customer_metrics_service: CustomerMetricsService,
                 modular_client: Modular,
                 job_statistics_service: JobStatisticsService):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.tenant_metrics_service = tenant_metrics_service
        self.customer_metrics_service = customer_metrics_service
        self.modular_client = modular_client
        self.job_statistics_service = job_statistics_service

        self.TOP_RESOURCES_BY_TENANT = []
        self.TOP_RESOURCES_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_COMPLIANCE_BY_TENANT = []
        self.TOP_COMPLIANCE_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_ATTACK_BY_TENANT = []
        self.TOP_ATTACK_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_FINOPS_BY_TENANT = []
        self.TOP_FINOPS_BY_CLOUD = {c: [] for c in CLOUDS}
        self.CUSTOMER_GENERAL = {
            OUTDATED_TENANTS: {},
            **{c: {'total_scanned_tenants': 0,
                   'last_scan_date': None} for c in CLOUDS}
        }
        self.CUSTOMER_COMPLIANCE = None
        self.CUSTOMER_ATTACK = None
        self.CUSTOMER_FINOPS = None
        self.CUSTOMER_OVERVIEW = {c: {'total_scanned_tenants': 0,
                                      'last_scan_date': None,
                                      'resources_violated': 0,
                                      'succeeded_scans': 0,
                                      'failed_scans': 0,
                                      'severity_data': {},
                                      'resource_types_data': {},
                                      'total_scans': 0} for c in CLOUDS}
        self.mean_coverage = {**{c: [] for c in CLOUDS}}

        self.today_date = datetime.utcnow().today()
        self.month_first_day = self.today_date.date().replace(
            day=1).isoformat()
        self.prev_month_first_day = (self.today_date + relativedelta(months=-1, day=1)).date()
        self.attack_overall_severity = {}
        self._tenant_metrics_exists = None
        self._cust_metrics_exists = None
        self.tenant_scan_mapping = {}
        self.ggl_tenant_obj_mapping = {}
        self.metrics_bucket = self.environment_service.get_metrics_bucket_name()
        self.customer_scanned_tenant_list = {}

    @classmethod
    def build(cls) -> 'TopMetrics':
        return cls(
            s3_client=SERVICE_PROVIDER.s3,
            environment_service=SERVICE_PROVIDER.environment_service,
            tenant_metrics_service=SERVICE_PROVIDER.tenant_metrics_service,
            customer_metrics_service=SERVICE_PROVIDER.customer_metrics_service,
            modular_client=SERVICE_PROVIDER.modular_client,
            job_statistics_service=SERVICE_PROVIDER.job_statistics_service
        )

    def process_data(self, event):
        if end_date := event.get(END_DATE):
            s3_object_date = f'monthly/{utc_datetime(end_date).replace(day=1)}'
        else:
            end_date = self.month_first_day
            s3_object_date = f'monthly/{self.month_first_day}'

        customers = set(
            customer.split('/')[0] for customer in
            self.s3_client.list_dir(bucket_name=self.metrics_bucket)
        )
        for customer in customers:
            self._reset_variables()
            if self.current_month_tenant_metrics_exist(customer) and \
                    self.current_month_customer_metrics_exist(customer):
                _LOG.debug(f'Customer and department metrics for customer '
                           f'{customer} already exist')
                continue

            _LOG.debug(f'Processing customer {customer}')
            tenant_filenames = self.s3_client.list_dir(
                bucket_name=self.metrics_bucket,
                key=TENANT_GROUP_METRICS_PATH.format(customer=customer,
                                                     date=s3_object_date))

            self.tenant_scan_mapping[
                customer] = self._get_month_customer_scan_stats(
                customer, to_date=utc_datetime(end_date, utc=False).date(),
                from_date=self.prev_month_first_day)

            for filename in tenant_filenames:
                if not filename.endswith('.json') and not \
                        filename.endswith('.json.gz'):
                    continue

                _LOG.debug(f'Processing tenant group {filename}')
                tenant_group_content = self.s3_client.gz_get_json(
                    bucket=self.metrics_bucket, key=filename)
                tenant_group_content = copy.deepcopy(tenant_group_content)
                customer = tenant_group_content.get(CUSTOMER_ATTR)

                resource_data = tenant_group_content.get(RESOURCES_TYPE, {})
                attack_data = tenant_group_content.get(ATTACK_VECTOR_TYPE, {})
                compliance_data = tenant_group_content.get(COMPLIANCE_TYPE, {})
                overview_data = tenant_group_content.get(OVERVIEW_TYPE, {})
                finops_data = tenant_group_content.get(FINOPS_TYPE, {})
                tenant_group_content[RESOURCES_TYPE] = self._unpack_metrics(
                    resource_data)
                tenant_group_content[COMPLIANCE_TYPE] = self._unpack_metrics(
                    compliance_data)
                tenant_group_content[ATTACK_VECTOR_TYPE] = self._unpack_metrics(
                    attack_data)
                tenant_group_content[OVERVIEW_TYPE] = self._unpack_metrics(
                    overview_data)
                tenant_group_content[FINOPS_TYPE] = self._unpack_metrics(
                    finops_data)

                self._process_customer_compliance(
                    tenant_group_content[COMPLIANCE_TYPE])
                self._process_customer_overview(
                    tenant_group_content[OVERVIEW_TYPE])
                self._process_customer_finops(
                    tenant_group_content[FINOPS_TYPE])
                attack_by_tenant = self._process_customer_mitre(
                    tenant_group_content[RESOURCES_TYPE], customer,
                    s3_object_date)
                self.add_department_metrics(tenant_group_content, customer,
                                            attack_by_tenant)

            # save top tenants and new date marker
            if not self.current_month_tenant_metrics_exist(customer):
                _LOG.debug('Saving top tenants to the table')
                top_items = self.create_top_items()
                self.tenant_metrics_service.batch_save(top_items)

            if not self.current_month_customer_metrics_exist(customer):
                _LOG.debug('Saving customer metrics to the table')
                customer_items = self.create_customer_items(customer)
                self.customer_metrics_service.batch_save(customer_items)

        return {DATA_TYPE: NEXT_STEP, END_DATE: event.get(END_DATE),
                'continuously': event.get('continuously')}

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
                            'defining_attribute': t.get(defining_attr),
                            OUTDATED_TENANTS: t.get(OUTDATED_TENANTS, [])
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
                        'defining_attribute': t.get(defining_attr),
                        OUTDATED_TENANTS: t.get(OUTDATED_TENANTS, [])
                    }) for t in
                    getattr(self, f'TOP_{metrics_type}_BY_CLOUD').get(
                        cloud, [])
                ])

        _LOG.debug(f'Top by tenant: {top_metrics_by_tenant}')
        _LOG.debug(f'Top by cloud: {top_metrics_by_cloud}')
        return top_metrics_by_tenant + top_metrics_by_cloud

    def add_department_metrics(self, tenant_group_content, customer,
                               attack_by_tenant):
        tenant_dn = tenant_group_content.get('tenant_display_name')
        general_tenant_info = {
            CUSTOMER_ATTR: customer,
            'from': tenant_group_content['from'],
            'to': tenant_group_content['to'],
            'tenant_display_name': tenant_group_content['tenant_display_name']
        }

        self._process_department_overview_metrics(tenant_dn,
                                                  tenant_group_content,
                                                  general_tenant_info)
        self._process_department_compliance_metrics(tenant_dn,
                                                    tenant_group_content,
                                                    general_tenant_info)
        self._process_department_attack_metrics(tenant_dn,
                                                attack_by_tenant,
                                                general_tenant_info)
        # self._process_department_finops_metrics(tenant_dn,
        #                                         tenant_group_content,
        #                                         general_tenant_info)

    def _process_department_overview_metrics(self, tenant_dn, metrics,
                                             general_tenant_info):
        customer = metrics[CUSTOMER_ATTR]
        resources_sum = 0
        for cloud in CLOUDS:
            if not (overview_metrics := metrics[OVERVIEW_TYPE].get(cloud)):
                continue

            acc_id = overview_metrics.get('account_id')

            if self.tenant_scan_mapping.get(customer, {}).get(acc_id):
                scans = self.tenant_scan_mapping[customer][acc_id]
            else:
                scans = {'failed_scans': 0,
                         'succeeded_scans': 0}
            overview_metrics['failed_scans'] = scans['failed_scans']
            overview_metrics['succeeded_scans'] = scans['succeeded_scans']
            overview_metrics['total_scans'] = scans['succeeded_scans'] + scans['failed_scans']
            self.CUSTOMER_OVERVIEW[cloud]['failed_scans'] += scans['failed_scans']
            self.CUSTOMER_OVERVIEW[cloud]['succeeded_scans'] += scans['succeeded_scans']
            self.CUSTOMER_OVERVIEW[cloud]['total_scans'] += scans['succeeded_scans'] + scans['failed_scans']

            sum_regions_data = {'resource_types_data': {}, 'severity_data': {}}
            for region, data in overview_metrics.get('regions_data', {}).items():
                for severity, value in data.get('severity_data', {}).items():
                    sum_regions_data['severity_data'][severity] = \
                        sum_regions_data['severity_data'].setdefault(severity, 0) + value
                for _type, value in data.get('resource_types_data', {}).items():
                    sum_regions_data['resource_types_data'][_type] = \
                        sum_regions_data['resource_types_data'].setdefault(_type, 0) + value

            overview_metrics.pop('regions_data', None)
            overview_metrics.update(sum_regions_data)
            if overview_metrics.get('resources_violated'):
                tenant_data = {
                    **general_tenant_info, cloud: overview_metrics
                }
                self.add_tenant_to_top_by_resources_by_cloud(
                    tenant_name=tenant_dn,
                    amount=overview_metrics['resources_violated'],
                    tenant_data=tenant_data, cloud=cloud)

                resources_sum += overview_metrics.get('resources_violated', 0)

        tenant_data = {
            **general_tenant_info, **metrics[OVERVIEW_TYPE]
        }
        self.add_tenant_to_top_by_resources(tenant_display_name=tenant_dn,
                                            amount=resources_sum,
                                            tenant_data=tenant_data)

    def _process_department_compliance_metrics(self, tenant_dn, metrics,
                                               general_tenant_info):
        tenant_mean_coverage = []
        for cloud in CLOUDS:
            if not (compliance_metrics := metrics[COMPLIANCE_TYPE].get(cloud)):
                continue

            if not compliance_metrics.get(AVERAGE_DATA_ATTR):
                temp = {}
                for regions_data in compliance_metrics.get('regions_data', []):
                    for compliance in regions_data['standards_data']:
                        temp.setdefault(compliance[NAME_ATTR], []).append(
                            compliance['value'])
                compliance_metrics[AVERAGE_DATA_ATTR] = [
                    {NAME_ATTR: standard, 'value': sum(value) / len(value)} for standard, value in temp.items()]
                compliance_metrics.pop('regions_data', None)

            coverages = [percent['value'] for percent in
                         compliance_metrics[AVERAGE_DATA_ATTR]]
            if not coverages:
                _LOG.warning(f'Skipping {compliance_metrics["tenant_name"]} '
                             f'because there is no coverage')
                continue

            tenant_mean_coverage.extend(coverages)

            tenant_data = {
                **general_tenant_info, cloud: compliance_metrics
            }
            self.add_tenant_to_top_by_compliance_by_cloud(
                tenant_name=tenant_dn,
                coverage=sum(coverages) / len(coverages),
                tenant_data=tenant_data, cloud=cloud)
            self.mean_coverage[cloud].extend(coverages)

        tenant_coverage = sum(tenant_mean_coverage) / len(tenant_mean_coverage)
        tenant_data = {
            **general_tenant_info, **metrics[COMPLIANCE_TYPE]
        }
        self.add_tenant_to_top_by_compliance(tenant_display_name=tenant_dn,
                                             coverage=tenant_coverage,
                                             tenant_data=tenant_data)

    def _process_department_finops_metrics(self, tenant_dn, metrics,
                                           general_tenant_info):
        resources_sum = 0
        new_finops_metrics = {}
        for cloud in CLOUDS:
            new_finops_metrics[cloud] = []
            if not (finops_metrics := metrics[FINOPS_TYPE].get(cloud)):
                continue

            resources_cloud_sum = 0
            for service_data in finops_metrics.get('service_data', []):
                severity_sum = {}
                for region, severity_data in service_data['regions_data'].items():
                    for severity, value in severity_data['severity_data'].items():
                        severity_sum[severity] = severity_sum.setdefault(severity, 0) + value
                        resources_cloud_sum += value

                new_finops_metrics[cloud].append({
                    'service_section': service_data.get('service_section'),
                    'severity_data': severity_sum
                })

            tenant_data = {**general_tenant_info, cloud: new_finops_metrics[cloud]}
            self.add_tenant_to_top_by_finops_by_cloud(
                tenant_name=tenant_dn, amount=resources_cloud_sum,
                tenant_data=tenant_data, cloud=cloud)

            resources_sum += resources_cloud_sum

        tenant_data = {**general_tenant_info, **new_finops_metrics}
        self.add_tenant_to_top_by_finops(tenant_display_name=tenant_dn,
                                         amount=resources_sum,
                                         tenant_data=tenant_data)

    def _process_department_attack_metrics(self, tenant_dn, attack_by_tenant,
                                           general_tenant_info):
        for cloud in CLOUDS:
            tenant_data = {
                **general_tenant_info, cloud: {'data': attack_by_tenant[cloud]}
            }
            tenant_attack_severity = {}
            for tactic in attack_by_tenant[cloud]:
                for k, v in tactic['severity_data'].items():
                    tenant_attack_severity[k] = tenant_attack_severity.setdefault(k, 0) + v

            self.TOP_ATTACK_BY_CLOUD[cloud].append({
                TENANT_ATTR: tenant_dn, **tenant_data,
                'sort_by': tenant_attack_severity})

        self.TOP_ATTACK_BY_TENANT.append(
            {TENANT_ATTR: tenant_dn, **general_tenant_info,
             'sort_by': self.attack_overall_severity.copy(),
             **attack_by_tenant})

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

    def add_tenant_to_top_by_finops(self, tenant_display_name, amount,
                                    tenant_data):
        self.TOP_FINOPS_BY_TENANT = self._add_tenant_to_top(
            tenant_display_name, amount, tenant_data,
            self.TOP_FINOPS_BY_TENANT, TOP_TENANT_LENGTH,
            'all_resources', reverse=True)

    def add_tenant_to_top_by_finops_by_cloud(self, tenant_name, amount,
                                             tenant_data, cloud):
        self.TOP_FINOPS_BY_CLOUD[cloud] = self._add_tenant_to_top(
            tenant_name, amount, tenant_data,
            self.TOP_FINOPS_BY_CLOUD[cloud],
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

    def current_month_customer_metrics_exist(self, customer):
        if self._cust_metrics_exists is None:
            self._cust_metrics_exists = self.customer_metrics_service.get_all_types_by_customer_date(
                customer=customer,
                date=datetime.today().date().replace(day=1).isoformat())
        return all(self._cust_metrics_exists.values())

    def current_month_tenant_metrics_exist(self, customer):
        if self._tenant_metrics_exists is None:
            self._tenant_metrics_exists = self.tenant_metrics_service.get_all_types_by_customer_date(
                date=datetime.today().date().replace(day=1).isoformat(),
                customer=customer)
        return all(self._tenant_metrics_exists.values())

    def _update_general_customer_data(self, cloud: str, data: dict):
        """
        Updates last scan date and calculates amount of customer tenants
        """
        if data.get('succeeded') or data.get('failed'):
            if tenants := data.get('tenants', {}):
                self.customer_scanned_tenant_list.setdefault(cloud, set()).\
                    update(tenants.as_dict().keys())
        last_scan_date = self.CUSTOMER_GENERAL[cloud]['last_scan_date']
        self.CUSTOMER_GENERAL[cloud]['last_scan_date'] = \
            self._get_last_scan_date(data.get('last_scan_date'),
                                     last_scan_date)

    @staticmethod
    def _get_last_scan_date(new_scan_date: str, last_scan_date: str = None):
        if not new_scan_date:
            return last_scan_date
        if not last_scan_date:
            return new_scan_date
        last_scan_datetime = utc_datetime(last_scan_date, utc=False)
        scan_datetime = utc_datetime(new_scan_date, utc=False)
        if last_scan_datetime < scan_datetime:
            return new_scan_date
        return last_scan_date

    def create_customer_items(self, customer):
        items = []

        if not self._cust_metrics_exists.get(ATTACK_VECTOR_TYPE.upper()):
            attack_vector_item = self.create_attack_vector_item(customer)
            if attack_vector_item:
                items.append(self.customer_metrics_service.create(
                    attack_vector_item))

        if not self._cust_metrics_exists.get(COMPLIANCE_TYPE.upper()):
            compliance_item = self.create_compliance_item(customer)
            if any(compliance_item[cloud] for cloud in CLOUDS):
                for cloud in CLOUDS:
                    if compliance_item[cloud]:
                        compliance_item[cloud].update(
                            self.CUSTOMER_GENERAL[cloud])
                items.append(self.customer_metrics_service.create(
                    compliance_item))

        if not self._cust_metrics_exists.get(OVERVIEW_TYPE.upper()):
            overview_item = self.create_overview_item(customer)
            if any(overview_item[cloud] for cloud in CLOUDS):
                for cloud in CLOUDS:
                    if overview_item[cloud]:
                        overview_item[cloud].update(
                            self.CUSTOMER_GENERAL[cloud])
                items.append(
                    self.customer_metrics_service.create(overview_item))
        #
        # if not self._cust_metrics_exists.get(FINOPS_TYPE.upper()):
        #     finops_item = self.create_finops_item(customer)
        #     for cloud in CLOUDS:
        #         if finops_item[cloud]:
        #             finops_item[cloud].update(self.CUSTOMER_GENERAL[cloud])
        #     items.append(self.customer_metrics_service.create(finops_item))

        return items

    def create_overview_item(self, customer):
        overview_item = {
            TYPE_ATTR: OVERVIEW_TYPE.upper(),
            **{c: self.CUSTOMER_OVERVIEW.get(c, {}) for c in CLOUDS},
            'customer': customer,
            'date': self.month_first_day}
        for c in CLOUDS:
            if all(self._is_empty(v) for v in overview_item[c].values()):
                overview_item[c] = {}
            elif overview_item[c]['succeeded_scans'] == 0:
                overview_item[c]['resources_violated'] = 0
                overview_item[c]['resource_types_data'] = {}
                overview_item[c]['severity_data'] = {}
        return overview_item

    def create_compliance_item(self, customer):
        for cloud in CLOUDS:
            updated_standards = [
                {NAME_ATTR: standard, VALUE_ATTR: sum(value) / len(value)}
                for standard, value in
                self.CUSTOMER_COMPLIANCE.setdefault(cloud, {}).setdefault(
                    AVERAGE_DATA_ATTR, {}).items()
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

    def create_finops_item(self, customer):
        # TODO
        cloud_data = {
                cloud: {'data': [
                    {**v, 'tactic': k} for k, v in
                    self.CUSTOMER_FINOPS.get(cloud, {}).items()]}
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

    def _process_customer_compliance(self, compliance_data):
        for cloud in CLOUDS:
            _LOG.debug('Process metrics for compliance report')
            coverage_data = compliance_data[cloud].get(AVERAGE_DATA_ATTR)
            if not coverage_data:
                coverage_data = compliance_data[cloud].get('regions_data', [])
                if coverage_data:
                    coverage_data = coverage_data[0].get('standards_data', [])
                else:
                    continue
            self.CUSTOMER_COMPLIANCE[cloud].setdefault(AVERAGE_DATA_ATTR, {})
            for standard in coverage_data:
                self.CUSTOMER_COMPLIANCE[cloud][AVERAGE_DATA_ATTR]. \
                    setdefault(standard[NAME_ATTR], []).append(
                    standard[VALUE_ATTR])

    def _get_month_customer_scan_stats(self, customer: str, from_date: date,
                                       to_date: date):
        if self.tenant_scan_mapping.get(customer):
            return self.tenant_scan_mapping.get(customer)

        tenant_scan_mapping = {}
        items = self.job_statistics_service.get_by_customer_and_date(
            customer, from_date.isoformat(), to_date.isoformat())
        for i in items:
            for tenant, scans in (i.tenants.attribute_values or {}).items():
                tenant_scan_mapping.setdefault(
                    tenant, {'failed_scans': 0, 'succeeded_scans': 0})
                tenant_scan_mapping[tenant]['failed_scans'] += int(scans[
                    'failed_scans'])
                tenant_scan_mapping[tenant]['succeeded_scans'] += int(scans[
                    'succeeded_scans'])

            self._update_general_customer_data(i.cloud, i.attribute_values)

        for cloud in CLOUDS:
            self.CUSTOMER_GENERAL[cloud]['total_scanned_tenants'] = \
                len(self.customer_scanned_tenant_list.get(cloud, []))
        return tenant_scan_mapping

    def _reset_variables(self):
        _LOG.debug('Resetting department and c-level variables')
        self.CUSTOMER_COMPLIANCE = {c: {} for c in CLOUDS}
        self.CUSTOMER_FINOPS = {c: {} for c in CLOUDS}
        self.CUSTOMER_ATTACK = {c: {} for c in CLOUDS}
        self.CUSTOMER_GENERAL = {
            OUTDATED_TENANTS: {},
            **{c: {"total_scanned_tenants": 0,
                   "last_scan_date": None} for c in CLOUDS}
        }
        self.CUSTOMER_OVERVIEW = {c: {'total_scanned_tenants': 0,
                                      'last_scan_date': None,
                                      'resources_violated': 0,
                                      'succeeded_scans': 0,
                                      'failed_scans': 0,
                                      'severity_data': {},
                                      'resource_types_data': {},
                                      'total_scans': 0} for c in CLOUDS}
        self.TOP_RESOURCES_BY_TENANT = []
        self.TOP_RESOURCES_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_COMPLIANCE_BY_TENANT = []
        self.TOP_COMPLIANCE_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_ATTACK_BY_TENANT = []
        self.TOP_ATTACK_BY_CLOUD = {c: [] for c in CLOUDS}
        self.TOP_FINOPS_BY_TENANT = []
        self.TOP_FINOPS_BY_CLOUD = {c: [] for c in CLOUDS}
        self._tenant_metrics_exists = None
        self._cust_metrics_exists = None
        self.mean_coverage = {**{c: [] for c in CLOUDS}}
        self.attack_overall_severity = {}
        self.customer_scanned_tenant_list = {}

    @staticmethod
    def _unpack_metrics(data):
        for cloud in CLOUDS:
            if data.get(cloud):
                data[cloud] = data[cloud][0]
            else:
                data[cloud] = {}
        return data

    def _process_customer_finops(self, finops_data: dict):
        pass

    def _process_customer_overview(self, overview_data: dict):
        """
        :param overview_data: example:
        {'aws':
            {'account_id': '267745785661',
            'tenant_name': 'ONSHA',
            'last_scan_date': '2023-08-14T13:28:00.481518Z',
            'activated_regions': ['eu-west-1', 'eu-central-1'],
            'total_scans': 1,
            'failed_scans': 0,
            'succeeded_scans': 1,
            'resources_violated': 153,
            'regions_data': {
                'eu-central-1': {
                    'severity_data': {'Low': 16, 'Info': 54, 'High': 48, 'Medium': 35},
                    'resource_types_data': {'aws.account': 5,
                                            'aws.ebs-snapshot': 2,
                                            'log-group': 15, ...}
                }
            }
        },
        'azure': {}, 'google': {}}
        :return:
        """
        for cloud in CLOUDS:
            self.CUSTOMER_OVERVIEW[cloud]['resources_violated'] += \
                overview_data.get(cloud, {}).get('resources_violated', 0)

            for region, data in overview_data.get(cloud, {}).get(
                    'regions_data', {}).items():
                for severity, value in data.get('severity_data', {}).items():
                    self.CUSTOMER_OVERVIEW[cloud]['severity_data'].setdefault(
                        severity, 0)
                    self.CUSTOMER_OVERVIEW[cloud]['severity_data'][severity] \
                        += value
                for _type, value in data.get('resource_types_data', {}).items():
                    self.CUSTOMER_OVERVIEW[cloud]['resource_types_data'].\
                        setdefault(_type, 0)
                    self.CUSTOMER_OVERVIEW[cloud]['resource_types_data'][
                        _type] += value

    def _process_account_mitre(self, mitre_content: list, cloud: str
                               ) -> list[dict]:
        """
        :param mitre_content: Example:
        [{
            'tactic_id': 'TA0001',
            'tactic': 'Initial Access',
            'techniques_data': [
                {'technique_id': 'T1189',
                'technique': 'Valid Accounts'
                'regions_data': {
                    'eu-central-1': {
                        'resources': [
                            {
                                'resource': {'id': 'f48fpywjvf',
                                            'name': 'massage-planner-prod'},
                                'resource_type': 'rest-api',
                                'severity': 'High',
                                'sub_techniques': ['Default Accounts']
                            }
                        ]
                    }
                }
                }
            ]
        }]
        :return:
        """
        _LOG.debug('Process metrics for attack vector report')
        reduced_attack_metrics = []

        for tactic in mitre_content:
            tactic_name = tactic.get('tactic')
            tactic_id = tactic.get('tactic_id')
            tactic_item = {
                'tactic_id': tactic_id,
                'tactic': tactic_name,
                'severity_data': {}
            }

            severity_set = {}
            for tech in tactic.get('techniques_data', []):
                for region, resource in tech.get('regions_data', {}).items():
                    for data in resource.get('resources'):
                        severity = data.get('severity')
                        severity_set.setdefault(severity, set()).add(
                            hashable(data['resource']))

            keep_highest(*[severity_set.get(k) for k in sorted(
                severity_set.keys(), key=cmp_to_key(severity_cmp))])
            severity_sum = {k: len(v) for k, v in severity_set.items()}

            tactic_item['severity_data'] = severity_sum

            for severity_name, value in severity_sum.items():
                self.attack_overall_severity.setdefault(severity_name, 0)
                self.attack_overall_severity[severity_name] += value

                self.CUSTOMER_ATTACK[cloud].setdefault(
                        tactic_name, {
                            'tactic_id': tactic_id, 'severity_data': {}})[
                        'severity_data'].setdefault(severity_name, 0)
                self.CUSTOMER_ATTACK[cloud][tactic_name]['severity_data'][
                        severity_name] += value

            reduced_attack_metrics.append(tactic_item)

        return reduced_attack_metrics

    @staticmethod
    def _is_empty(value):
        if value in [None, 0, {}, []]:
            return True
        return False

    def _process_customer_mitre(self, metrics, customer, s3_object_date):
        attack_by_tenant = {c: [] for c in CLOUDS}
        for cloud in CLOUDS:
            if not metrics.get(cloud):
                continue

            account_id = metrics[cloud].get('account_id')

            account_path = TENANT_METRICS_PATH.format(
                customer=customer,
                date=s3_object_date) + '/' + account_id + '.json.gz'
            account_data = self.s3_client.gz_get_json(
                self.metrics_bucket, account_path).pop(ATTACK_VECTOR_TYPE, {})
            attack_metrics = self._process_account_mitre(account_data, cloud)
            attack_by_tenant[cloud] = attack_metrics
            if not attack_metrics:
                continue
        return attack_by_tenant


CUSTOMER_METRICS = TopMetrics.build()
