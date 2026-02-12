import operator
import uuid
from datetime import timedelta
from http import HTTPStatus
from typing import Any, Iterable, List, cast

import msgspec.json
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular
from typing_extensions import NotRequired, TypedDict

from handlers import AbstractHandler, Mapping
from helpers import map_by
from helpers.constants import (
    Cloud,
    Endpoint,
    HTTPMethod,
    RabbitCommand,
    ReportType,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models.metrics import ReportMetrics
from services import SP, modular_helpers
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.platform_service import PlatformService
from services.rabbitmq_service import RabbitMQService
from services.rbac_service import TenantsAccessPayload
from services.reports import ReportMetricsService, add_diff
from validators.swagger_request_models import (
    CLevelGetReportModel,
    DepartmentGetReportModel,
    OperationalGetReportModel,
    ProjectGetReportModel,
)
from validators.utils import validate_kwargs


_LOG = get_logger(__name__)

SRE_REPORTS_TYPE_TO_M3_MAPPING = {
    # Operational
    ReportType.OPERATIONAL_RESOURCES: 'CUSTODIAN_RESOURCES_REPORT',
    ReportType.OPERATIONAL_OVERVIEW: 'CUSTODIAN_OVERVIEW_REPORT',
    ReportType.OPERATIONAL_RULES: 'CUSTODIAN_RULES_REPORT',
    ReportType.OPERATIONAL_FINOPS: 'CUSTODIAN_FINOPS_REPORT',
    ReportType.OPERATIONAL_COMPLIANCE: 'CUSTODIAN_COMPLIANCE_REPORT',
    ReportType.OPERATIONAL_ATTACKS: 'CUSTODIAN_ATTACKS_REPORT',
    ReportType.OPERATIONAL_KUBERNETES: 'CUSTODIAN_K8S_CLUSTER_REPORT',
    ReportType.OPERATIONAL_DEPRECATION: 'CUSTODIAN_DEPRECATIONS_REPORT',
    # Project
    ReportType.PROJECT_OVERVIEW: 'CUSTODIAN_PROJECT_OVERVIEW_REPORT',
    ReportType.PROJECT_COMPLIANCE: 'CUSTODIAN_PROJECT_COMPLIANCE_REPORT',
    ReportType.PROJECT_RESOURCES: 'CUSTODIAN_PROJECT_RESOURCES_REPORT',
    ReportType.PROJECT_FINOPS: 'CUSTODIAN_PROJECT_FINOPS_REPORT',
    ReportType.PROJECT_ATTACKS: 'CUSTODIAN_PROJECT_ATTACKS_REPORT',
    # Department
    ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD: 'CUSTODIAN_TOP_RESOURCES_BY_CLOUD_REPORT',
    ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES: 'CUSTODIAN_TOP_TENANTS_VIOLATED_RESOURCES_REPORT',
    ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE: 'CUSTODIAN_TOP_TENANTS_COMPLIANCE_REPORT',
    ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD: 'CUSTODIAN_TOP_COMPLIANCE_BY_CLOUD_REPORT',
    ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS: 'CUSTODIAN_TOP_TENANTS_ATTACKS_REPORT',
    ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD: 'CUSTODIAN_TOP_TENANTS_BY_CLOUD_ATTACKS_REPORT',
    # C-Level
    ReportType.C_LEVEL_OVERVIEW: 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT',
    ReportType.C_LEVEL_COMPLIANCE: 'CUSTODIAN_CUSTOMER_COMPLIANCE_REPORT',
    ReportType.C_LEVEL_ATTACKS: 'CUSTODIAN_CUSTOMER_ATTACKS_REPORT',
}


class ReportMetadata(msgspec.Struct, kw_only=True, eq=False, frozen=True):
    type: ReportType
    description: str
    version: str
    created_at: str
    to: str
    from_: str | None = msgspec.field(default=None, name='from')


class MaestroReport(TypedDict, total=False):
    # NOTE: these are not all the fields
    receivers: list[str] | tuple[str, ...]
    metadata: ReportMetadata
    customer: str

    data: list | dict

    externalData: bool
    externalDataKey: NotRequired[str]
    externalDataBucket: NotRequired[str]


class MaestroLinkedTenantReport(TypedDict, total=False):
    tenant_display_name: str
    data: dict


def _compliance_diff_callback(key, new, old) -> dict:
    res = {'value': round(new * 100, 2)}
    if isinstance(old, (int, float)) and not isinstance(old, bool):
        res['diff'] = round((new - old) * 100, 2)
    return res


class MaestroModelBuilder:
    """
    Keeps all the dirty logic of transforming our report metrics to the
    format that maestro expects. I believe this logic won't last long
    and something will be changed drastically soon so this class is kind of
    congested.
    """

    __slots__ = ('_receivers',)

    def __init__(self, receivers: tuple[str, ...] = ()):
        self._receivers = receivers  # base receivers

    @staticmethod
    def _operational_overview_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_OVERVIEW
        inner = data['data']
        inner['rules_data'] = inner.pop('rules', [])
        regions_data = {}
        for region, rd in inner.pop('regions', {}).items():
            rd['resources_data'] = rd.pop('resources')
            rd['violations_data'] = rd.pop('violations', {})
            rd['attacks_data'] = rd.pop('attacks', {})
            rd['standards_data'] = [{
                'name': st['name'],
                'value': round(st['value'] * 100, 2),
            } for st in rd.pop('standards', ())]
            rd.pop('resource_types', None)
            rd.pop('services', None)
            regions_data[region] = rd
        inner['regions_data'] = regions_data
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'tenant_metadata': data['metadata'],
            'data': data['data'],
            'exceptions_data': data.get('exceptions_data', []),
            'externalData': False,
        }


    @staticmethod
    def _operational_resources_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RESOURCES
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'tenant_metadata': data['metadata'],
            'data': data['data'],
            'exceptions_data': data.get('exceptions_data', []),
            'externalData': False,
        }

    @staticmethod
    def _operational_rules_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_RULES
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'outdated_tenants': data['outdated_tenants'],
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {
                'rules_data': data.get('data', []),
                'violated_resources_length': data.get('resources_violated', 0),
            },
        }

    @staticmethod
    def _operational_finops_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_FINOPS
        for item in data.setdefault('data', []):
            for rules_data in item.setdefault('rules_data', []):
                rd = {}
                for region, res in rules_data.pop('resources', {}).items():
                    rd[region] = {'resources': res}
                rules_data['regions_data'] = rd
                rules_data.pop('service', None)
                rules_data.pop('severity', None)
                rules_data.pop('resource_type', None)
                rules_data.pop('rule', None)
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'tenant_metadata': data['metadata'],
            'data': data['data'],
            'exceptions_data': data.get('exceptions_data', []),
        }

    @staticmethod
    def _operational_deprecations_custom(
        rep: ReportMetrics, data: dict
    ) -> dict:
        assert rep.type == ReportType.OPERATIONAL_DEPRECATION
        for item in data.setdefault('data', []):
            item['regions_data'] = {
                region: {'resources': res}
                for region, res in item.pop('resources', {}).items()
            }
            item.pop('resource_type', None)
            item.pop('description', None)
            item.pop('remediation_complexity', None)
            item.pop('remediation', None)
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'tenant_metadata': data['metadata'],
            'data': data['data'],
            'exceptions_data': data.get('exceptions_data', []),
        }

    @staticmethod
    def _operational_compliance_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_COMPLIANCE

        regions_data = [
            {
                'region': region,
                'standards_data': [
                    {'name': name, 'value': round(value * 100, 2)}
                    for name, value in standards.items()
                ],
            }
            for region, standards in data['data'].get('regions', {}).items()
        ]
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'outdated_tenants': data['outdated_tenants'],
            'activated_regions': data['activated_regions'],
            'last_scan_date': data['last_scan_date'],
            'data': {'regions_data': regions_data},
        }

    @staticmethod
    def _operational_attacks_custom(rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_ATTACKS
        for item in data['data']:
            for attack in item.get('attacks', ()):
                for violation in attack.get('violations', ()):
                    violation.pop('description', None)
                    violation.pop('remediation', None)
                    violation.pop('remediation_complexity', None)
                    violation.pop('severity', None)
        return {
            'tenant_name': rep.tenant,
            'id': data['id'],
            'cloud': rep.cloud.value,  # pyright: ignore
            'tenant_metadata': data['metadata'],
            'data': data['data'],
            'exceptions_data': data.get('exceptions_data', []),
        }

    def _operational_k8s_custom(self, rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.OPERATIONAL_KUBERNETES
        return {
            'metadata': self.build_report_metadata(
                rep
            ),  # kludge, remove when fixed
            'tenant_name': data['tenant_name'],
            'last_scan_date': data['last_scan_date'],
            'outdated_tenants': data['outdated_tenants'],
            'cluster_id': rep.platform_id,
            'cloud': Cloud.AWS.value,  # TODO: get from tenant
            'region': data['region'],
            'cluster_metadata': {
                'rules': {
                    'violated': data.get('violated_rules', []),
                },
            },
            'data': {
                'policy_data': data['resources'],
                'mitre_data': data['mitre'],
                'compliance_data': [
                    {'name': name, 'value': round(cov * 100, 2)}
                    for name, cov in data['compliance'].items()
                ],
            },
        }

    def build_base(self, rep: ReportMetrics) -> MaestroReport:
        return {
            'receivers': self._receivers,
            'customer': rep.customer,
            'from': rep.start
            if rep.start
            else utc_iso(
                utc_datetime(rep.end) - timedelta(days=7)
            ),  # TODO: fix on maestro side. Not every report can have "from"
            'to': rep.end,
            'outdated_tenants': [],
            'externalData': False,
            'data': {},
        }

    def new_base(self, rep: ReportMetrics) -> MaestroReport:
        return {
            'receivers': self._receivers,
            'customer': rep.customer,
            'metadata': self.build_report_metadata(rep),
            'externalData': False,
            'data': {},
        }

    @staticmethod
    def linked_base(rep: ReportMetrics) -> MaestroLinkedTenantReport:
        return {
            'tenant_display_name': rep.project,
            'data': {},
        }

    @staticmethod
    def linked_base_cloud(cloud_data: dict) -> dict:
        return {
            'account_id': cloud_data['account_id'],
            'tenant_name': cloud_data['tenant_name'],
            'last_scan_date': cloud_data['last_scan_date'],
        }

    def _project_overview_custom(self, rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.PROJECT_OVERVIEW
        return {'tenant_display_name': rep.project, **data}


    def _project_overview_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> dict:
        new_data = {}
        assert rep.type == ReportType.PROJECT_OVERVIEW
        for cloud, c_data in data['data'].items():
            if not c_data:
                new_data[cloud] = {}
                continue

            new_data[cloud] = self.linked_base_cloud(c_data)
            new_data[cloud].update(
                {
                    'succeeded_scans': c_data['succeeded_scans'],
                    'failed_scans': c_data['failed_scans'],
                    'total_scans': c_data['total_scans'],
                }
            )

            high = medium = low = info = unknown = 0
            for r_data in c_data.get('regions_data', {}).values():
                high += r_data['severity_data'].get('High', 0)
                medium += r_data['severity_data'].get('Medium', 0)
                low += r_data['severity_data'].get('Low', 0)
                info += r_data['severity_data'].get('Info', 0)
                unknown += r_data['severity_data'].get('Unknown', 0)
            new_data[cloud]['resources_violated'] = {
                'High': high,
                'Medium': medium,
                'Low': low,
                'Info': info,
                'Unknown': unknown,
            }

        return {'data': new_data}


    def _project_compliance_custom(
        self, rep: ReportMetrics, data: dict
    ) -> dict:
        assert rep.type == ReportType.PROJECT_COMPLIANCE
        for t in data['data'].values():
            if not t.get('data'):
                continue
            t['regions_data'] = [
                {
                    'region': region,
                    'standards_data': [
                        {'name': name, 'value': round(value * 100, 2)}
                        for name, value in standards.items()
                    ],
                }
                for region, standards in t['data'].get('regions', {}).items()
            ]
            t['average_data'] = [
                {'name': name, 'value': round(value * 100, 2)}
                for name, value in t['data'].get('total', {}).items()
            ]
            t.pop('data')

        return {'tenant_display_name': rep.project, **data}


    def _project_compliance_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> dict:
        new_data = {}
        assert rep.type == ReportType.PROJECT_COMPLIANCE
        for cloud, c_data in data['data'].items():
            if not c_data:
                new_data[cloud] = {}
                continue

            new_data[cloud] = self.linked_base_cloud(c_data)

        return {'data': new_data}


    def _project_resources_custom(
        self, rep: ReportMetrics, data: dict
    ) -> dict:
        assert rep.type == ReportType.PROJECT_RESOURCES
        return {'tenant_display_name': rep.project, **data}

    def _project_resources_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> dict:
        res_count = \
            lambda i: sum(v['total_violated_resources'] for v in i.values())
        new_data = {}
        assert rep.type == ReportType.PROJECT_RESOURCES
        for cloud, c_data in data['data'].items():
            if not c_data:
                new_data[cloud] = {}
                continue

            new_data[cloud] = self.linked_base_cloud(c_data)

            high = medium = low = info = unknown = 0
            high_res = medium_res = low_res = info_res = unknown_res = 0
            for policy_data in c_data.get('data', []):
                severity = policy_data['severity']
                match severity:
                    case 'High':
                        high += 1
                        high_res = res_count(policy_data['regions_data'])
                    case 'Medium':
                        medium += 1
                        medium_res = res_count(policy_data['regions_data'])
                    case 'Low':
                        low += 1
                        low_res = res_count(policy_data['regions_data'])
                    case 'Info':
                        info += 1
                        info_res = res_count(policy_data['regions_data'])
                    case 'Unknown':
                        unknown += 1
                        unknown_res = res_count(policy_data['regions_data'])
            new_data[cloud]['rules_violated'] = {
                'High': high,
                'Medium': medium,
                'Low': low,
                'Info': info,
                'Unknown': unknown,
            }
            new_data[cloud]['violations_data'] = {
                'High': high_res,
                'Medium': medium_res,
                'Low': low_res,
                'Info': info_res,
                'Unknown': unknown_res,
            }

        return {'data': new_data}


    def _project_attacks_custom(self, rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.PROJECT_ATTACKS
        for t in data['data'].values():
            for attack in t.setdefault('attacks', []):
                attack['regions_data'] = [
                    {'region': region, 'severity_data': data}
                    for region, data in attack.pop('regions', {}).items()
                ]

        return {'tenant_display_name': rep.project, **data}

    def _project_attacks_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> dict:
        new_data = {}
        assert rep.type == ReportType.PROJECT_ATTACKS
        for cloud, c_data in data['data'].items():
            if not c_data:
                new_data[cloud] = {}
                continue

            new_data[cloud] = self.linked_base_cloud(c_data)

            high = medium = low = info = unknown = 0
            for attack in c_data.get('attacks', []):
                for regions_data in attack['regions'].values():
                    high += regions_data.get('High', 0)
                    medium += regions_data.get('Medium', 0)
                    low += regions_data.get('Low', 0)
                    info += regions_data.get('Info', 0)
                    unknown += regions_data.get('Unknown', 0)
            new_data[cloud]['attacks_data'] = {
                'High': high,
                'Medium': medium,
                'Low': low,
                'Info': info,
                'Unknown': unknown,
            }

        return {'data': new_data}


    def _project_finops_custom(self, rep: ReportMetrics, data: dict) -> dict:
        assert rep.type == ReportType.PROJECT_FINOPS
        for t in data['data'].values():
            for service_data in t.get('service_data', []):
                for rule_data in service_data.get('rules_data', []):
                    add_diff(rule_data, {})
                    # just to replace int leafs with {'value': leaf, 'diff': None}
        return {'tenant_display_name': rep.project, **data}


    def _project_finops_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> dict:
        res_count = \
            lambda i: sum(v['total_violated_resources'] for v in i.values())
        new_data = {}
        assert rep.type == ReportType.PROJECT_FINOPS
        for cloud, c_data in data['data'].items():
            if not c_data:
                new_data[cloud] = {}
                continue

            new_data[cloud] = self.linked_base_cloud(c_data)

            high = medium = low = info = unknown = 0
            high_res = medium_res = low_res = info_res = unknown_res = 0
            for s_data in c_data.get('service_data', []):
                for r_data in s_data.get('rules_data', []):
                    severity = r_data['severity']
                    match severity:
                        case 'High':
                            high += 1
                            high_res = res_count(r_data['regions_data'])
                        case 'Medium':
                            medium += 1
                            medium_res = res_count(r_data['regions_data'])
                        case 'Low':
                            low += 1
                            low_res = res_count(r_data['regions_data'])
                        case 'Info':
                            info += 1
                            info_res = res_count(r_data['regions_data'])
                        case 'Unknown':
                            unknown += 1
                            unknown_res = res_count(r_data['regions_data'])
                new_data[cloud]['rules_violated'] = {
                    'High': high,
                    'Medium': medium,
                    'Low': low,
                    'Info': info,
                    'Unknown': unknown,
                }
                new_data[cloud]['violations_data'] = {
                    'High': high_res,
                    'Medium': medium_res,
                    'Low': low_res,
                    'Info': info_res,
                    'Unknown': unknown_res,
                }

        return {'data': new_data}


    def _top_compliance_by_cloud(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_display_name')

        for cl, items in data['data'].items():
            old = map_by(previous_data.get('data', {}).get(cl, []), key)
            for tdn, new_data in map_by(items, key).items():
                old_data = old.get(tdn, {})
                add_diff(
                    new_data,
                    old_data,
                    exclude=('sort_by',),
                    callback=_compliance_diff_callback,
                )
                new_data['data'] = [
                    {'name': k, **v}
                    for k, v in new_data.get('data', {}).items()
                ]

        return data

    def _top_resources_by_cloud(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_display_name')

        for cl, items in data['data'].items():
            old = map_by(previous_data.get('data', {}).get(cl, []), key)
            for tdn, new_data in map_by(items, key).items():
                old_data = old.get(tdn, {})
                add_diff(
                    new_data.setdefault('data', {}),
                    old_data.setdefault('data', {}),
                )
        return data

    def _top_attacks_by_cloud(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_name')
        key_tactic = operator.itemgetter('tactic_id')
        for cl, items in data['data'].items():
            old = map_by(previous_data.get('data', {}).get(cl, []), key)
            for tn, new_data in map_by(items, key).items():
                old_data = old.get(tn, {})

                # that is crazy

                old_tactics = map_by(old_data.get('data', []), key_tactic)
                for tactic_id, new_tactics_data in map_by(
                    new_data.get('data', []), key_tactic
                ).items():
                    old_tactics_data = old_tactics.get(tactic_id, {})
                    add_diff(new_tactics_data, old_tactics_data)
        return data

    def _top_tenants_compliance(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_display_name')
        old = map_by(previous_data.get('data', []), key)
        for tdn, new_data in map_by(data.setdefault('data', []), key).items():
            old_data = old.get(tdn, {})
            add_diff(
                new_data,
                old_data,
                exclude=('sort_by',),
                callback=_compliance_diff_callback,
            )
            for d in new_data.get('data', {}).values():
                d['average_data'] = [
                    {'name': k, **v}
                    for k, v in d.get('average_data', {}).items()
                ]
        return data

    def _top_tenants_resources(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_display_name')
        old = map_by(previous_data.get('data', []), key)
        for tdn, new_data in map_by(data.setdefault('data', []), key).items():
            old_data = old.get(tdn, {})
            add_diff(
                new_data.setdefault('data', {}),
                old_data.setdefault('data', {}),
            )
        return data

    def _top_tenants_attacks(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tenant_display_name')
        key_tactic = operator.itemgetter('tactic_id')
        old = map_by(previous_data.get('data', []), key)
        for tdn, new_data in map_by(data.setdefault('data', []), key).items():
            old_data = old.get(tdn, {})

            for cl, items in new_data.get('data', {}).items():
                old_tactics = map_by(old_data.get(cl, []), key_tactic)
                for tactic_id, new_tactics_data in map_by(
                    items, key_tactic
                ).items():
                    old_tactics_data = old_tactics.get(tactic_id, {})
                    add_diff(new_tactics_data, old_tactics_data)
        return data

    def _c_level_overview_custom(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        previous_data = previous_data or {}
        for cl, cl_data in data.get('data', {}).items():
            add_diff(
                cl_data,
                previous_data.get('data', {}).get(cl, {}),
                exclude=(
                    'total_scanned_tenants',
                    ('license_properties', 'Number of licenses'),
                ),
            )
        return data

    def _c_level_attacks_custom(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        key = operator.itemgetter('tactic_id')
        for cl, cl_data in data.get('data', {}).items():
            old = map_by(previous_data.get('data', {}).get(cl, []), key)
            for tactic_id, new_data in map_by(cl_data, key).items():
                old_data = old.get(tactic_id, {})
                add_diff(new_data, old_data)

        return data

    def _c_level_compliance_custom(
        self, rep: ReportMetrics, data: dict, previous_data: dict
    ) -> dict:
        for cl, cl_data in data.get('data', {}).items():
            add_diff(
                cl_data,
                previous_data.get('data', {}).get(cl, {}),
                exclude=(
                    'total_scanned_tenants',
                    ('license_properties', 'Number of licenses'),
                ),
                callback=_compliance_diff_callback,
            )
            cl_data['average_data'] = [
                {'name': k, **v}
                for k, v in cl_data.get('average_data', {}).items()
            ]
        return data

    @staticmethod
    def build_report_metadata(rep: ReportMetrics) -> ReportMetadata:
        return ReportMetadata(
            type=rep.type,
            description=rep.type.description,
            version='2.0.0',  # maybe use version of sre
            created_at=rep.created_at,
            to=rep.end,
            from_=rep.start,
        )

    def convert(
        self, rep: ReportMetrics, data: dict, previous_data: dict | None = None
    ) -> MaestroReport:
        base = self.build_base(rep)
        previous_data = previous_data or {}
        match rep.type:
            case ReportType.OPERATIONAL_OVERVIEW:
                base = self.new_base(rep)
                custom = self._operational_overview_custom(rep, data)
            case ReportType.OPERATIONAL_RESOURCES:
                base = self.new_base(rep)
                custom = self._operational_resources_custom(rep, data)
            case ReportType.OPERATIONAL_RULES:
                custom = self._operational_rules_custom(rep, data)
            case ReportType.OPERATIONAL_FINOPS:
                base = self.new_base(rep)
                custom = self._operational_finops_custom(rep, data)
            case ReportType.OPERATIONAL_COMPLIANCE:
                custom = self._operational_compliance_custom(rep, data)
            case ReportType.OPERATIONAL_ATTACKS:
                base = self.new_base(rep)
                custom = self._operational_attacks_custom(rep, data)
            case ReportType.OPERATIONAL_KUBERNETES:
                custom = self._operational_k8s_custom(rep, data)
            case ReportType.OPERATIONAL_DEPRECATION:
                base = self.new_base(rep)
                custom = self._operational_deprecations_custom(rep, data)
            case ReportType.PROJECT_OVERVIEW:
                custom = self._project_overview_custom(rep, data)
            case ReportType.PROJECT_COMPLIANCE:
                custom = self._project_compliance_custom(rep, data)
            case ReportType.PROJECT_RESOURCES:
                custom = self._project_resources_custom(rep, data)
            case ReportType.PROJECT_ATTACKS:
                custom = self._project_attacks_custom(rep, data)
            case ReportType.PROJECT_FINOPS:
                custom = self._project_finops_custom(rep, data)
            case ReportType.DEPARTMENT_TOP_TENANTS_ATTACKS:
                custom = self._top_tenants_attacks(rep, data, previous_data)
            case ReportType.DEPARTMENT_TOP_TENANTS_RESOURCES:
                custom = self._top_tenants_resources(rep, data, previous_data)
            case ReportType.DEPARTMENT_TOP_TENANTS_COMPLIANCE:
                custom = self._top_tenants_compliance(rep, data, previous_data)
            case ReportType.DEPARTMENT_TOP_ATTACK_BY_CLOUD:
                custom = self._top_attacks_by_cloud(rep, data, previous_data)
            case ReportType.DEPARTMENT_TOP_RESOURCES_BY_CLOUD:
                custom = self._top_resources_by_cloud(rep, data, previous_data)
            case ReportType.DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD:
                custom = self._top_compliance_by_cloud(
                    rep, data, previous_data
                )
            case ReportType.C_LEVEL_OVERVIEW:
                custom = self._c_level_overview_custom(
                    rep, data, previous_data
                )
            case ReportType.C_LEVEL_ATTACKS:
                custom = self._c_level_attacks_custom(rep, data, previous_data)
            case ReportType.C_LEVEL_COMPLIANCE:
                custom = self._c_level_compliance_custom(
                    rep, data, previous_data
                )
            case _:
                raise
        base.update(custom)
        return base

    def convert_linked(
        self,
        rep: ReportMetrics,
        data: dict,
    ) -> MaestroLinkedTenantReport:
        base = self.linked_base(rep)
        match rep.type:
            case ReportType.PROJECT_OVERVIEW:
                custom = self._project_overview_linked(rep, data)
            case ReportType.PROJECT_COMPLIANCE:
                custom = self._project_compliance_linked(rep, data)
            case ReportType.PROJECT_RESOURCES:
                custom = self._project_resources_linked(rep, data)
            case ReportType.PROJECT_ATTACKS:
                custom = self._project_attacks_linked(rep, data)
            case ReportType.PROJECT_FINOPS:
                custom = self._project_finops_linked(rep, data)
            case _:
                raise
        base.update(custom)
        return base



class MaestroReportToS3Packer:
    """
    Holds logic how to compress some large reports to jsonl files specifically
    for Maestro
    """

    # actual Maestro RabbitMQ limit seems to be 5mb, but the data that we
    # send is converted/compressed somehow inside modular-sdk before being sent
    # to Maestro. So I put 4mb here (just in case)
    _default_size_limit = (1 << 20) * 4
    _encoder = msgspec.json.Encoder()

    __slots__ = '_s3', '_bucket', '_limit', '_mapping'

    def __init__(
        self,
        s3_client: S3Client,
        bucket: str,
        size_limit: int = _default_size_limit,
    ):
        self._s3 = s3_client
        self._bucket = bucket
        self._limit = size_limit
        self._mapping = {
            ReportType.OPERATIONAL_KUBERNETES: self._pack_k8s,
            ReportType.OPERATIONAL_FINOPS: self._pack_finops,
            ReportType.OPERATIONAL_RESOURCES: self._pack_resources,
            ReportType.OPERATIONAL_ATTACKS: self._pack_attacks,
        }

    def _pack_k8s(self, data: dict) -> bytearray:
        buf = bytearray()
        for line in data.get('policy_data', {}):
            resources = line.pop('resources', [])
            self._write_line(buf, line, b'policy')
            for resource in resources:
                self._write_line(buf, resource)
        for res in data.get('mitre_data', {}):
            attacks = res.pop('attacks', [])
            self._write_line(buf, res, b'resource')
            for attack in attacks:
                violations = attack.pop('violations', [])
                self._write_line(buf, attack, b'attack')
                for v in violations:
                    self._write_line(buf, v)
        # TODO: what about compliance
        return buf

    def _pack_finops(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        for line in data:
            rules = line.pop('rules_data', [])
            self._write_line(buf, line, b'service')
            for rule in rules:
                regions = rule.pop('regions_data', {})
                self._write_line(buf, rule, b'rule')
                for region, resources in regions.items():
                    self._write_line(buf, {'key': region}, b'region')
                    for resource in resources.get('resources', []):
                        self._write_line(buf, resource)
        return buf

    def _pack_resources(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        for line in data:
            regions = line.pop('regions_data', {})
            self._write_line(buf, line, b'policy')
            for region, resources in regions.items():
                self._write_line(buf, {'key': region}, b'region')
                for resource in resources.get('resources', []):
                    self._write_line(buf, resource)
        return buf

    def _pack_attacks(self, data: list[dict]) -> bytearray:
        buf = bytearray()
        # NOTE: maybe we should use temp file
        for res in data:
            attacks = res.pop('attacks', [])
            self._write_line(buf, res, b'resource')
            for attack in attacks:
                violations = attack.pop('violations', [])
                self._write_line(buf, attack, b'attack')
                for v in violations:
                    self._write_line(buf, v)
        return buf

    def _write_line(
        self, to: bytearray, data: dict | list, tag: bytes | None = None
    ) -> None:
        if tag:
            to.extend(tag)
        self._encoder.encode_into(data, to, len(to))
        to.extend(b'\n')

    def _is_too_big(self, data: dict | list) -> bool:
        return len(self._encoder.encode(data)) > self._limit

    def _write_to_s3(self, key: str, data: bytearray) -> None:
        self._s3.put_object(bucket=self._bucket, key=key, body=data)

    def pack(self, report: MaestroReport) -> MaestroReport:
        """
        This method has side effects: it can store some data to S3. It can
        change the given "report" dict in place and also returns it (just for
        convenience).
        """
        # TODO: this is a kludge, rewrite it when all reports have same format
        typ = getattr(report.get('metadata'), 'type', None)
        if not typ:
            return report
        if typ is ReportType.OPERATIONAL_KUBERNETES:
            # kludge, remove when fixed
            report.pop('metadata')
        if typ not in self._mapping:
            return report
        # typ in mapping
        data = report['data']
        if not self._is_too_big(data):
            return report
        _LOG.info(
            f'Report size is bigger than limit: {self._limit}. Writing to S3'
        )
        customer = report['customer']
        key = f'{customer}/{str(uuid.uuid4())}.jsonl'

        buf = self._mapping[typ](data)
        try:
            self._write_to_s3(key, buf)
        except ClientError:
            _LOG.exception('Could not write packed report to s3')
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send data to Maestro S3')
                .exc()
            )

        report['externalData'] = True
        report['externalDataKey'] = key
        report['externalDataBucket'] = self._bucket
        report['data'] = data.__class__()
        return report


class HighLevelReportsHandler(AbstractHandler):
    def __init__(
        self,
        report_metrics_service: ReportMetricsService,
        modular_client: Modular,
        environment_service: EnvironmentService,
        rabbitmq_service: RabbitMQService,
        assume_role_s3_client: S3Client,
        platform_service: PlatformService,
    ):
        self._rms = report_metrics_service
        self._mc = modular_client
        self._env = environment_service
        self._rmq = rabbitmq_service
        self._assume_role_s3 = assume_role_s3_client
        self._ps = platform_service

    @classmethod
    def build(cls) -> 'HighLevelReportsHandler':
        return cls(
            report_metrics_service=SP.report_metrics_service,
            modular_client=SP.modular_client,
            environment_service=SP.environment_service,
            rabbitmq_service=SP.rabbitmq_service,
            assume_role_s3_client=SP.assume_role_s3,
            platform_service=SP.platform_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_CLEVEL: {
                HTTPMethod.POST: self.post_c_level
            },
            Endpoint.REPORTS_PROJECT: {
                HTTPMethod.POST: self.post_project
            },
            Endpoint.REPORTS_DEPARTMENT: {
                HTTPMethod.POST: self.post_department
            },
            Endpoint.REPORTS_OPERATIONAL: {
                HTTPMethod.POST: self.post_operational
            },
        }

    @validate_kwargs
    def post_c_level(self, event: CLevelGetReportModel):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))
        now = utc_datetime()

        for report_type in event.new_types:
            rep = self._rms.get_exactly_for_customer(
                customer=event.customer_id,
                type_=report_type,
                start=report_type.start(now),
                end=report_type.end(now),
            )
            if not rep:
                _LOG.warning(
                    f'Cannot find {report_type} for '
                    f'{event.customer_id} for the current month'
                )
                continue

            previous_month = now + relativedelta(months=-1)
            previous = self._rms.get_exactly_for_customer(
                customer=event.customer_id,
                type_=report_type,
                start=report_type.start(previous_month),
                end=report_type.end(previous_month),
            )
            if not previous:
                _LOG.info(
                    'Previous clevel report not found, diffs will be empty'
                )
                previous_data = {}
            else:
                previous_data = self._rms.fetch_data(previous)
            current_data = self._fetch_and_validate_data(
                rep=rep,
                report_type=report_type,
                display_name=event.customer_id,
            )
            base = builder.convert(rep, current_data, previous_data)
            models.append(
                self._rmq.build_m3_json_model(
                    notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[report_type],
                    data=cast(dict[str, Any], base),
                )
            )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    @validate_kwargs
    def post_operational(
        self, event: OperationalGetReportModel, _tap: TenantsAccessPayload
    ):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()
        packer = MaestroReportToS3Packer(
            s3_client=self._assume_role_s3,
            bucket=self._env.get_recommendation_bucket(),
        )

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))
        types = event.new_types
        if ReportType.OPERATIONAL_OVERVIEW in types:
            # TODO: quick fix to put operational overview in the end
            _types = list(types)
            _types.remove(ReportType.OPERATIONAL_OVERVIEW)
            _types.append(ReportType.OPERATIONAL_OVERVIEW)
            types = tuple(_types)

        only_k8s = (
            len(types) == 1 and types[0] is ReportType.OPERATIONAL_KUBERNETES
        )
        # TODO: temp solution to disable deprecation reports for aws, google, k8s
        only_depr = (
            len(types) == 1 and types[0] is ReportType.OPERATIONAL_DEPRECATION
        )

        for tenant_name in event.tenant_names:
            if not _tap.is_allowed_for(tenant_name):
                raise (
                    ResponseFactory(HTTPStatus.FORBIDDEN)
                    .message(f'Action is forbidden for tenant {tenant_name}')
                    .exc()
                )
            tenant = self._mc.tenant_service().get(tenant_name)
            modular_helpers.assert_tenant_valid(tenant, event.customer)
            tenant = cast(Tenant, tenant)
            tenant_cloud = modular_helpers.tenant_cloud(tenant)

            if tenant_cloud is not Cloud.AZURE and only_depr:
                raise (
                    ResponseFactory(HTTPStatus.BAD_REQUEST)
                    .message('Deprecation reports are currently allowed only for AZURE tenants')
                    .exc()
                )

            for report_type in types:
                if report_type is ReportType.OPERATIONAL_KUBERNETES:
                    _LOG.debug('Specific handling for k8s reports')
                    k8s_datas = []
                    for platform in self._ps.query_by_tenant(tenant):
                        rep = self._rms.get_latest_for_platform(platform, report_type)
                        if not rep:
                            _LOG.warning(
                                f'Could not find data for platform {platform.id}'
                            )
                            continue
                        platform_display_name = f'{tenant.name} (platform {platform.id})'
                        fetched_data = self._fetch_and_validate_data(
                            rep=rep,
                            report_type=report_type,
                            display_name=platform_display_name,
                        )
                        data = builder.convert(rep, fetched_data)
                        data = packer.pack(data)
                        k8s_datas.append(data)
                    if only_k8s and not k8s_datas:
                        _LOG.debug(
                            f'Could not find any {report_type} for {tenant.name}'
                        )
                        raise (
                            ResponseFactory(HTTPStatus.NOT_FOUND)
                            .message(
                                f'Could not find any {report_type} for {tenant.name}'
                            )
                            .exc()
                        )
                    models.extend(
                        self._rmq.build_m3_json_model(
                            notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[
                                report_type
                            ],
                            data=i,
                        )
                        for i in k8s_datas
                    )
                    continue

                if (
                    report_type is ReportType.OPERATIONAL_DEPRECATION
                    and tenant_cloud is not Cloud.AZURE
                ):
                    _LOG.info(
                        f'Skipping deprecation report '
                        f'for non-AZURE tenant {tenant}'
                    )
                    continue

                _LOG.debug(f'Going to generate {report_type} for {tenant.name}')
                rep = self._rms.get_latest_for_tenant(
                    tenant=tenant,
                    type_=report_type,
                )
                rep = self._validate_report_exists(
                    rep=rep,
                    report_type=report_type,
                    display_name=tenant.name,
                )
                fetched_data = self._fetch_and_validate_data(
                    rep=rep,
                    report_type=report_type,
                    display_name=tenant.name,
                )
                data = builder.convert(rep, fetched_data)
                data = packer.pack(data)

                models.append(
                    self._rmq.build_m3_json_model(
                        notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[report_type],
                        data=cast(dict[str, Any], data),
                    )
                )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    @validate_kwargs
    def post_project(
        self, event: ProjectGetReportModel, _tap: TenantsAccessPayload
    ):
        models = []
        customer_id = event.customer_id
        rabbitmq = self._rmq.get_customer_rabbitmq(customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder(receivers=tuple(event.receivers))
        for display_name in event.tenant_display_names:
            _LOG.info(
                f'Going to generate reports for tenant with '
                f'display_name: {display_name}'
            )

            linked_tenants_dntl = set()
            if event.include_linked:
                # TODO consider getting linked tenants by case insensitive
                #  way(to cover display_name with different cases)
                _LOG.info('Retrieving linked tenants')
                # We need a set here to avoid duplicates because of tenants
                # for different clouds with the same display name
                linked_tenants_dntl = {
                    tenant.display_name_to_lower for tenant in
                        self._mc.tenant_service().i_get_tenant_by_customer(
                            customer_id=customer_id,
                            active=True,
                            linked_to=display_name.upper(),
                        )
                    if Cloud.parse(tenant.cloud) is not None
                }

            for report_type in event.new_types:
                _LOG.debug(f'Going to generate {report_type} for {display_name}')
                rep = self._rms.get_latest_for_project(
                    customer=customer_id,
                    project=display_name,
                    type_=report_type,
                )
                rep = self._validate_report_exists(
                    rep=rep,
                    report_type=report_type,
                    display_name=display_name,
                )
                fetched_data = self._fetch_and_validate_data(
                    rep=rep,
                    report_type=report_type,
                    display_name=display_name,
                )

                linked_data = self._collect_linked_tenants_data(
                    customer_name=customer_id,
                    linked_tenants_dntl=linked_tenants_dntl,
                    report_type=report_type,
                    builder=builder,
                )
                if linked_data:
                    fetched_data['linked_tenants_data'] = linked_data

                data = builder.convert(rep, fetched_data)

                models.append(
                    self._rmq.build_m3_json_model(
                        notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[report_type],
                        data=cast(dict[str, Any], data),
                    )
                )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    @validate_kwargs
    def post_department(self, event: DepartmentGetReportModel):
        models = []
        rabbitmq = self._rmq.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            raise self._rmq.no_rabbitmq_response().exc()

        builder = MaestroModelBuilder()
        now = utc_datetime()

        for report_type in event.new_types:
            _LOG.debug(
                f'Going to generate {report_type} for '
                f'{event.customer_id}'
            )
            rep = self._rms.get_exactly_for_customer(
                customer=event.customer_id,
                type_=report_type,
                start=report_type.start(now),
                end=report_type.end(now),
            )
            if not rep:
                _LOG.warning(
                    f'Cannot find {report_type} for '
                    f'{event.customer_id} for the current month'
                )
                continue
            previous_month = now + relativedelta(months=-1)
            previous = self._rms.get_exactly_for_customer(
                customer=event.customer_id,
                type_=report_type,
                start=report_type.start(previous_month),
                end=report_type.end(previous_month),
            )
            if not previous:
                _LOG.info(
                    'Previous clevel report not found, diffs will be empty'
                )
                previous_data = {}
            else:
                previous_data = self._rms.fetch_data(previous)
            current_data = self._fetch_and_validate_data(
                rep=rep,
                report_type=report_type,
                display_name=event.customer_id,
            )

            base = builder.convert(rep, current_data, previous_data)
            models.append(
                self._rmq.build_m3_json_model(
                    notification_type=SRE_REPORTS_TYPE_TO_M3_MAPPING[report_type],
                    data=cast(dict[str, Any], base),
                )
            )

        if not models:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    'No collected reports found to send. Update metrics first'
                )
                .exc()
            )
        code = self._rmq.send_to_m3(
            rabbitmq=rabbitmq, command=RabbitCommand.SEND_MAIL, models=models
        )
        if code != 200:
            raise (
                ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE)
                .message('Could not send message to RabbitMQ')
                .exc()
            )
        return build_response(
            code=HTTPStatus.ACCEPTED, content='Successfully sent'
        )

    def _validate_report_exists(
        self, 
        rep: ReportMetrics | None,
        report_type: ReportType,
        display_name: str,
    ) -> ReportMetrics:
        """Validates that report exists, raises NOT_FOUND if not. Returns validated rep."""
        if not rep:
            _LOG.debug(f'Could not find any {report_type} for {display_name}')
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(f'Could not find any {report_type} for {display_name}')
                .exc()
            )
        return rep

    def _fetch_and_validate_data(
        self,
        rep: ReportMetrics,
        report_type: ReportType,
        display_name: str,
    ) -> dict[str, Any]:
        """Fetches data from report and validates it exists, raises NOT_FOUND if not."""
        data = self._rms.fetch_data(rep)
        if not data:
            _LOG.warning(f'Could not find any {report_type} for {display_name}')
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(f'Could not find any {report_type} for {display_name}')
                .exc()
            )
        return cast(dict[str, Any], data)

    def _collect_linked_tenants_data(
        self,
        customer_name: str,
        linked_tenants_dntl: Iterable[str],
        report_type: ReportType,
        builder: MaestroModelBuilder,
    ) -> List[dict[str, Any]]:

        result = []
        for dntl in linked_tenants_dntl:
            _LOG.info(
                f'Collecting linked tenant: {dntl} '
                f'report {report_type} data'
            )
            rep = self._rms.get_latest_for_project(
                customer=customer_name,
                project=dntl,
                type_=report_type,
            )
            if not rep:
                _LOG.info(
                    f'Could not find report {report_type} for linked tenant '
                    f'{dntl}'
                )
                continue
            data = self._rms.fetch_data(rep)
            if not data:
                _LOG.info(
                    f'Could not find report {report_type} data for linked '
                    f'tenant {dntl}'
                )
                continue
            _LOG.debug('Linked tenant data collected successfully')

            data = builder.convert_linked(rep, data)

            result.append(data)

        return result
