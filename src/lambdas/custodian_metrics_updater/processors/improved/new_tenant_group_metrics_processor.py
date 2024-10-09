import copy
from datetime import datetime
from pathlib import PurePosixPath
from typing import Generator

from dateutil.relativedelta import SU, relativedelta
from modular_sdk.modular import Modular

from helpers import hashable
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso, utc_datetime
from services import SP
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.reports_bucket import MetricsBucketKeysBuilder

CLOUDS = ['aws', 'azure', 'google']  # todo, organize somehow
_LOG = get_logger(__name__)


class TenantGroupMetrics:
    def __init__(self, modular: Modular,
                 environment_service: EnvironmentService,
                 s3_client: S3Client):
        self._mc = modular
        self._env = environment_service
        self._s3 = s3_client

    @classmethod
    def build(cls) -> 'TenantGroupMetrics':
        return cls(
            modular=SP.modular_client,
            environment_service=SP.environment_service,
            s3_client=SP.s3,
        )

    def _get_tenant_ids_for(self, customer_name: str,
                            date: datetime) -> Generator[str, None, None]:
        it = self._s3.list_dir(
            bucket_name=self._env.get_metrics_bucket_name(),
            key=MetricsBucketKeysBuilder.list_customer_accounts_metrics_prefix(customer_name, date)
        )
        for key in it:
            yield self.tenant_id_from_key(key)

    @staticmethod
    def _base_compressed_metrics() -> dict:
        return {
            'overview': {c: {} for c in CLOUDS},
            'compliance': {c: {} for c in CLOUDS},
            'resources': {c: {'data': []} for c in CLOUDS},
            'attack_vector': {c: {'mitre_data': []} for c in CLOUDS},
            'finops': {c: {'service_data': []} for c in CLOUDS}
        }

    @staticmethod
    def _calculate_resources(tenant_metrics: dict, cloud: str) -> dict:
        mapping = {}  # region resource to services
        reduced_resources = []
        for resource in tenant_metrics.get('resources', []):
            service = resource.get('resource_type') or 'Unknown'  # must always be in theory
            region_data = resource.get('regions_data') or {}
            for region, res in region_data.items():
                for r in res.get('resources') or []:
                    mapping.setdefault(region, {}).setdefault(
                        hashable(r), set()).add(service)
            reduced_resources.append({
                **{k: v for k, v in resource.items() if k != 'regions_data'},
                'resource_type': service,
                'regions_data': {
                    region: {'total_violated_resources': len(res.get('resources') or [])}
                    for region, res in region_data.items()
                }
            })
        result = {
            'overview': {cloud: copy.deepcopy(tenant_metrics.get('overview') or {})},
            'resources': {cloud: {'data': reduced_resources}}
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
            result['overview'][cloud].setdefault('regions_data', {}).setdefault(
                region, {})['resource_types_data'] = region_map
        return result

    @staticmethod
    def _process_attack_vector_metrics(tenant_content: dict):
        _LOG.debug('Process metrics for attack vector report')
        reduced_attack_metrics = []
        for tactic in tenant_content.get('attack_vector', []):
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
        compressed_metrics[report_type][cloud]['last_scan_date'] = last_scan
        compressed_metrics[report_type][cloud]['activated_regions'] = \
            activated_regions
        compressed_metrics[report_type][cloud]['tenant_name'] = tenant_name
        compressed_metrics[report_type][cloud]['account_id'] = _id

    def _process_customer_data(self, customer_name: str,
                               start: datetime, end: datetime):
        tenant_groups_mapping = {}
        for tenant_id in self._get_tenant_ids_for(customer_name, end):
            tenant = next(self._mc.tenant_service().i_get_by_acc(
                acc=tenant_id,
                limit=1
            ), None)  # todo active??
            if not tenant:
                _LOG.warning(f'Tenant with id {tenant_id} not found')
                continue
            tdn = tenant.display_name_to_lower.lower()  # just in case :)
            tenant_groups_mapping.setdefault(tdn, []).append(tenant)

        for tdn, tenants in tenant_groups_mapping.items():
            compressed_metrics = self._base_compressed_metrics()
            general_tenant_info = {}
            tenant_group_compliance_mapping = {c: [] for c in CLOUDS}
            tenant_group_overview_mapping = {c: [] for c in CLOUDS}
            tenant_group_resources_mapping = {c: [] for c in CLOUDS}
            tenant_group_attack_mapping = {c: [] for c in CLOUDS}
            tenant_group_finops_mapping = {c: [] for c in CLOUDS}

            for tenant in tenants:
                _LOG.info(f'Processing tenant {tenant.name} within tenant group {tdn}')
                tenant_content = self._s3.gz_get_json(
                    bucket=self._env.get_metrics_bucket_name(),
                    key=MetricsBucketKeysBuilder(tenant).account_metrics(end)
                )
                cloud = tenant_content['cloud'].lower()
                if not general_tenant_info:
                    general_tenant_info.update({
                        'customer': customer_name,
                        'from': utc_iso(start),
                        'to': utc_iso(end),
                        'tenant_display_name': tdn,
                        'outdated_tenants': tenant_content.get('outdated_tenants', {})
                    })
                else:
                    for _c, _tenants in tenant_content.get('outdated_tenants', {}).items():
                        general_tenant_info['outdated_tenants'].setdefault(_c, {}).update(_tenants)

                compressed_metrics.update(self._calculate_resources(
                    tenant_metrics=tenant_content,
                    cloud=cloud
                ))
                compressed_metrics['attack_vector'][cloud]['mitre_data'] = self._process_attack_vector_metrics(
                    tenant_content=tenant_content
                )
                compressed_metrics['compliance'][cloud] = copy.deepcopy(
                    tenant_content['compliance']
                )
                if tenant_content.get('finops'):
                    compressed_metrics['finops'][cloud]['service_data'] = self._process_finops_metrics(
                        tenant_content['finops']
                    )
                for t in ('overview', 'compliance',
                          'attack_vector', 'resources', 'finops'):
                    self._add_base_info_to_compressed_metrics(
                        cloud, t,
                        tenant_content.get('last_scan_date'),
                        tenant_content.get('activated_regions'),
                        tenant_content.get('tenant_name'),
                        tenant_content.get('id'),
                        compressed_metrics
                    )  # why here?
                for mapping, _type in [
                    (tenant_group_compliance_mapping, 'compliance'),
                    (tenant_group_overview_mapping, 'overview'),
                    (tenant_group_resources_mapping, 'resources'),
                    (tenant_group_attack_mapping, 'attack_vector'),
                    (tenant_group_finops_mapping, 'finops')
                ]:
                    content = compressed_metrics.get(_type, {}).get(cloud)
                    if isinstance(mapping[cloud], list):
                        mapping[cloud].append(content)
                    else:
                        mapping[cloud].update(**content)
            tenant_group_data = {
                **general_tenant_info,
                'compliance': tenant_group_compliance_mapping,
                'overview': tenant_group_overview_mapping,
                'resources': tenant_group_resources_mapping,
                'attack_vector': tenant_group_attack_mapping,
                'finops': tenant_group_finops_mapping
            }

            self._s3.gz_put_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenants[0]).tenant_metrics(end),
                obj=tenant_group_data
            )
            _LOG.info('Saving monthly metrics to s3')  # todo monthly seems not monthly, must be fixed
            self._s3.gz_put_json(
                bucket=self._env.get_metrics_bucket_name(),
                key=MetricsBucketKeysBuilder(tenants[0]).tenant_monthly_metrics(end),
                obj=tenant_group_data
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

        it = self._s3.common_prefixes(
            bucket=self._env.get_metrics_bucket_name(),
            delimiter='/'
        )
        for customer_name in map(lambda x: x.strip('/'), it):
            _LOG.info(f'Collecting group metrics for customer {customer_name}')
            self._process_customer_data(customer_name, start, end)

        return {
            'data_type': 'customer',
            # todo end_date,
            # todo pass period which we use to collect data
            'continuously': event.get('continuously')  # todo for what
        }


TENANT_GROUP_METRICS = TenantGroupMetrics.build()
