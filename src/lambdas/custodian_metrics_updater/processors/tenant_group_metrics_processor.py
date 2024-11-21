import calendar
import copy
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta, SU
from modular_sdk.modular import Modular

from helpers import get_logger, hashable, get_last_element
from helpers.constants import CUSTOMER_ATTR, OVERVIEW_TYPE, COMPLIANCE_TYPE, \
    RESOURCES_TYPE, DATA_TYPE, ACCOUNT_ID_ATTR, ATTACK_VECTOR_TYPE, END_DATE, \
    LAST_SCAN_DATE, SEVERITY_DATA_ATTR, RESOURCE_TYPES_DATA_ATTR, \
    TENANT_NAME_ATTR, ID_ATTR, TENANT_DISPLAY_NAME_ATTR, AVERAGE_DATA_ATTR, \
    ACTIVATED_REGIONS_ATTR, FINOPS_TYPE, OUTDATED_TENANTS, ARCHIVE_PREFIX
from helpers.time_helper import utc_datetime
from services import SERVICE_PROVIDER
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.setting_service import SettingsService

_LOG = get_logger(__name__)

CLOUDS = ['aws', 'azure', 'google']
TENANT_METRICS_PATH = '{customer}/accounts/{date}'
TENANT_GROUP_METRICS_FILE_PATH = '{customer}/tenants/{date}/{tenant}.json'
NEXT_STEP = 'customer'


class TenantGroupMetrics:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 modular_client: Modular,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.modular_client = modular_client
        self.mapping = mappings_collector

        self.today_date = datetime.utcnow().today()
        self.today_midnight = datetime.combine(self.today_date,
                                               datetime.min.time())
        self.yesterday = (self.today_date - timedelta(days=1)).date()
        self.next_month_date = (self.today_date.date() + relativedelta(months=+1, day=1)).isoformat()
        self.month_first_day = self.today_date.date().replace(
            day=1).isoformat()
        self.prev_month_first_day = (self.today_date + relativedelta(months=-1, day=1)).date()
        self.TO_UPDATE_MARKER = False

        self._date_marker = self.settings_service.get_report_date_marker()
        self.current_week_date = self._date_marker.get('current_week_date')
        self.last_week_date = self._date_marker.get('last_week_date')

    @classmethod
    def build(cls) -> 'TenantGroupMetrics':
        return cls(
            s3_client=SERVICE_PROVIDER.s3,
            environment_service=SERVICE_PROVIDER.environment_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            modular_client=SERVICE_PROVIDER.modular_client,
            mappings_collector=SERVICE_PROVIDER.mappings_collector
        )

    def _calculate_resources(self, tenant_metrics: dict, cloud: str) -> dict:
        mapping = {}  # region resource to services
        reduced_resources = []
        for resource in tenant_metrics.get(RESOURCES_TYPE) or []:
            service = self.mapping.service.get(resource.get('policy'), {}) \
                      or 'Unknown'
            region_data = resource.get('regions_data') or {}
            for region, res in region_data.items():
                for r in res.get(RESOURCES_TYPE) or []:
                    mapping.setdefault(region, {}).setdefault(
                        hashable(r), set()).add(service)
            reduced_resources.append({
                **{k: v for k, v in resource.items() if k != 'regions_data'},
                'resource_type': service,
                'regions_data': {
                    region: {'total_violated_resources': len(res.get(RESOURCES_TYPE) or [])}
                    for region, res in region_data.items()
                }
            })
        result = {
            OVERVIEW_TYPE: {cloud: copy.deepcopy(tenant_metrics.get(OVERVIEW_TYPE) or {})},
            RESOURCES_TYPE: {cloud: {'data': reduced_resources}}
        }
        for region, res_services in mapping.items():
            region_map = {}
            for res, services in res_services.items():
                # 0 means we prefer the first found service for this resource
                # a situation when one resource violates different services
                # is possible, for example aws.account
                service = sorted(services)[0]
                region_map.setdefault(service, 0)
                region_map[service] += 1
            result[OVERVIEW_TYPE][cloud].setdefault('regions_data', {}).setdefault(
                region, {})[RESOURCE_TYPES_DATA_ATTR] = region_map
        return result

    def process_data(self, event):
        if end_date := event.get(END_DATE):
            end_date = utc_datetime(end_date, utc=False).date()
            weekday = end_date.weekday()
            if weekday == 6:
                s3_object_date = end_date
            else:
                s3_object_date = (end_date + timedelta(
                    days=6 - weekday)).isoformat()
            self.next_month_date = (end_date.replace(day=1) +
                                    relativedelta(months=1)).isoformat()
        elif self.current_week_date <= self.yesterday.isoformat():
            end_date = self.today_midnight
            self.TO_UPDATE_MARKER = True
            s3_object_date = (utc_datetime(self.current_week_date) + relativedelta(
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
            tenant_group_to_files_mapping = {}
            account_filenames = list(self.s3_client.list_dir(
                bucket_name=metrics_bucket,
                key=TENANT_METRICS_PATH.format(customer=customer,
                                               date=s3_object_date)))
            for filename in account_filenames:
                project_id = get_last_element(filename.replace(
                    '.json', '').replace('.gz', ''), '/')
                if not project_id:
                    _LOG.warning(f'Cannot get project id from file {filename}')
                    continue
                elif project_id.startswith(ARCHIVE_PREFIX):
                    _LOG.warning(f'Skipping archived tenant {filename}')
                    continue

                tenant_obj = list(self.modular_client.tenant_service().i_get_by_acc(
                    project_id, attributes_to_get=['dntl', 'n']
                ))

                if not tenant_obj:
                    _LOG.warning(f'Unknown tenant with project id '
                                 f'{project_id}. Skipping...')
                    continue

                if len(tenant_obj) > 1:
                    tenant_names = ", ".join(t.name for t in tenant_obj)
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
                    RESOURCES_TYPE: {c: {'data': []} for c in CLOUDS},
                    ATTACK_VECTOR_TYPE: {c: {'mitre_data': []} for c in CLOUDS},
                    FINOPS_TYPE: {c: {'service_data': []} for c in CLOUDS}
                }
                general_tenant_info = {}
                tenant_group_compliance_mapping = {c: [] for c in CLOUDS}
                tenant_group_overview_mapping = {c: [] for c in CLOUDS}
                tenant_group_resources_mapping = {c: [] for c in CLOUDS}
                tenant_group_attack_mapping = {c: [] for c in CLOUDS}
                tenant_group_finops_mapping = {c: [] for c in CLOUDS}

                for tenant in filenames:
                    _LOG.debug(
                        f'Processing tenant {get_last_element(tenant, "/")} '
                        f'within tenant group {tenant_dn}')
                    tenant_content = self.s3_client.gz_get_json(
                        bucket=metrics_bucket, key=tenant)
                    cloud = tenant_content['cloud'].lower()
                    _id = tenant_content.pop(ID_ATTR, None)
                    tenant_name = tenant_content.pop(TENANT_NAME_ATTR, None)
                    last_scan = tenant_content.get(LAST_SCAN_DATE)
                    activated_regions = tenant_content.get(
                        ACTIVATED_REGIONS_ATTR)
                    outdated_tenants = tenant_content.get(OUTDATED_TENANTS, {})

                    if not general_tenant_info:  # why here?
                        general_tenant_info.update({
                            CUSTOMER_ATTR: customer,
                            'from': tenant_content.get('from'),
                            'to': utc_datetime(s3_object_date).isoformat(),
                            TENANT_DISPLAY_NAME_ATTR: tenant_dn,
                            OUTDATED_TENANTS: outdated_tenants
                        })
                    else:
                        for c, tenants in outdated_tenants.items():
                            general_tenant_info[OUTDATED_TENANTS].\
                                setdefault(c, {}).update(tenants)

                    # does not modify tenant_content, updates compressed item
                    # with resources and overview
                    compressed_metrics.update(**self._calculate_resources(
                        tenant_content, cloud))

                    # does not modify tenant_content, adds attack_vector to
                    # compressed_metrics
                    compressed_metrics[ATTACK_VECTOR_TYPE][cloud]['mitre_data'] = self._process_attack_vector_metrics(
                        tenant_content
                    )
                    # modifies compressed_metrics, does not change
                    # tenant_content
                    compressed_metrics[COMPLIANCE_TYPE][cloud] = copy.deepcopy(
                        tenant_content[COMPLIANCE_TYPE])
                    if tenant_content.get(FINOPS_TYPE):
                        # modifies compressed_metrics, does not change finops
                        compressed_metrics[FINOPS_TYPE][cloud]['service_data'] = self._process_finops_metrics(
                            tenant_content[FINOPS_TYPE])

                    for t in (OVERVIEW_TYPE, COMPLIANCE_TYPE,
                              ATTACK_VECTOR_TYPE, RESOURCES_TYPE, FINOPS_TYPE):
                        self._add_base_info_to_compressed_metrics(
                            cloud, t, last_scan, activated_regions,
                            tenant_name, _id, compressed_metrics)  # why here?

                    for mapping, _type in [
                        (tenant_group_compliance_mapping, COMPLIANCE_TYPE),
                        (tenant_group_overview_mapping, OVERVIEW_TYPE),
                        (tenant_group_resources_mapping, RESOURCES_TYPE),
                        (tenant_group_attack_mapping, ATTACK_VECTOR_TYPE),
                        (tenant_group_finops_mapping, FINOPS_TYPE)
                    ]:
                        content = compressed_metrics.get(_type, {}).get(cloud)
                        if isinstance(mapping[cloud], list):
                            mapping[cloud].append(content)
                        else:
                            mapping[cloud].update(**content)

                tenant_group_data = {
                    **general_tenant_info,
                    COMPLIANCE_TYPE: tenant_group_compliance_mapping,
                    OVERVIEW_TYPE: tenant_group_overview_mapping,
                    RESOURCES_TYPE: tenant_group_resources_mapping,
                    ATTACK_VECTOR_TYPE: tenant_group_attack_mapping,
                    FINOPS_TYPE: tenant_group_finops_mapping
                }

                self.s3_client.gz_put_json(
                    bucket=metrics_bucket,
                    key=TENANT_GROUP_METRICS_FILE_PATH.format(
                        customer=customer, date=s3_object_date,
                        tenant=tenant_dn),
                    obj=tenant_group_data
                )

                if not event.get(END_DATE) or calendar.monthrange(
                        end_date.year, end_date.month)[1] == end_date.day:
                    self._save_monthly_state(tenant_group_data, tenant_dn,
                                             customer)

        return {DATA_TYPE: NEXT_STEP, END_DATE: event.get(END_DATE),
                'continuously': event.get('continuously')}

    @staticmethod
    def is_month_passed():
        today = datetime.today().date()
        return today.day == 1

    @staticmethod
    def _get_last_scan_date(new_scan_date: str, last_scan_date: str = None):
        if not last_scan_date:
            return new_scan_date
        last_scan_datetime = utc_datetime(last_scan_date, utc=False)
        scan_datetime = utc_datetime(new_scan_date, utc=False)
        if last_scan_datetime < scan_datetime:
            return new_scan_date
        return last_scan_date

    @staticmethod
    def _process_compliance_metrics(cloud, compliance_data,
                                    compressed_metrics):
        _LOG.debug('Process metrics for compliance report')
        coverage_data = compliance_data.get(AVERAGE_DATA_ATTR)
        if not coverage_data:
            coverage_data = compliance_data.get('regions_data')[0].get(
                'standards_data', [])
        compressed_metrics[COMPLIANCE_TYPE][cloud].setdefault(
            AVERAGE_DATA_ATTR, coverage_data)

    @staticmethod
    def _process_overview_metrics(cloud: str, overview_data: dict,
                                  compressed_metrics: dict):
        _LOG.debug('Process metrics for overview report')
        for key in ('total_scans', 'succeeded_scans', 'failed_scans',
                    'resources_violated'):
            key_value = overview_data.get(key, 0)
            compressed_metrics[OVERVIEW_TYPE][cloud].setdefault(key, 0)
            compressed_metrics[OVERVIEW_TYPE][cloud][key] += key_value

        for region, data in (overview_data.get('regions_data') or {}).items():
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

    @staticmethod
    def _process_attack_vector_metrics(tenant_content: dict):
        _LOG.debug('Process metrics for attack vector report')
        reduced_attack_metrics = []
        for tactic in tenant_content.get(ATTACK_VECTOR_TYPE, []):
            tactic_name = tactic.get('tactic')
            tactic_id = tactic.get('tactic_id')
            tactic_item = {
                'tactic_id': tactic_id,
                'tactic': tactic_name,
                'techniques_data': []
            }
            for tech in tactic.get('techniques_data', []):
                technique_item = {
                    'technique_id': tech.get('technique_id'),
                    'technique': tech.get('technique'),
                    'regions_data': {}
                }
                for region, resource in tech.get('regions_data', {}).items():
                    severity_list = {}
                    for data in resource.get('resources'):
                        severity = data.get('severity')
                        for _ in range(len(data['sub_techniques']) if len(data['sub_techniques']) != 0 else 1):
                            severity_list.setdefault(severity, []).append(
                                data['resource'])

                    severity_sum = {k: len(v) for k, v in severity_list.items()}
                    technique_item['regions_data'].setdefault(
                        region, {'severity_data': {}})[
                        'severity_data'] = severity_sum
                tactic_item['techniques_data'].append(technique_item)

            reduced_attack_metrics.append(tactic_item)
        return reduced_attack_metrics

    @staticmethod
    def _process_finops_metrics(finops_content: dict):
        _LOG.debug('Process metrics for FinOps report')
        service_severity_mapping = {}

        for service_item in finops_content:
            service_section = service_item.get('service_section')
            service_severity_mapping.setdefault(service_section,
                                                {'rules_data': []})
            for rule_item in service_item['rules_data']:
                new_item = copy.deepcopy(rule_item)
                for region, data in rule_item['regions_data'].items():
                    new_item['regions_data'][region].pop('resources', None)
                    new_item['regions_data'][region]['total_violated_resources'] = \
                        {'value': len(data.get('resources', 0))}

                service_severity_mapping[service_section]['rules_data'].append(new_item)

        return [{'service_section': service, **data}
                for service, data in service_severity_mapping.items()]

    @staticmethod
    def _add_base_info_to_compressed_metrics(cloud, report_type, last_scan,
                                             activated_regions, tenant_name,
                                             _id, compressed_metrics):
        compressed_metrics[report_type][cloud][LAST_SCAN_DATE] = last_scan
        compressed_metrics[report_type][cloud][ACTIVATED_REGIONS_ATTR] = \
            activated_regions
        compressed_metrics[report_type][cloud][TENANT_NAME_ATTR] = tenant_name
        compressed_metrics[report_type][cloud][ACCOUNT_ID_ATTR] = _id

    def _save_monthly_state(self, data: dict, group: str, customer: str):
        path = f'{customer}/tenants/monthly/{self.next_month_date}/{group}.json'
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        _LOG.debug(f'Save monthly metrics for tenant group {group}')
        self.s3_client.gz_put_json(
            bucket=metrics_bucket,
            key=path,
            obj=data
        )

    @staticmethod
    def deduplication(resources: dict):
        _unique = set()
        for resource in resources:
            res = hashable(resource)
            _unique.add(res)
        return _unique


# TENANT_GROUP_METRICS = TenantGroupMetrics.build()
