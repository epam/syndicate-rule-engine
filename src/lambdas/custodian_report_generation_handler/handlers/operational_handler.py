from functools import cached_property
from http import HTTPStatus
import json
from datetime import datetime
import os
import sys
import tempfile
import uuid

from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from dateutil.relativedelta import relativedelta, SU
from helpers.time_helper import utc_datetime
from helpers import RequestContext
from helpers.constants import (
    ATTACK_VECTOR_TYPE,
    COMPLIANCE_TYPE,
    CUSTOMER_ATTR,
    CustodianEndpoint,
    DATA_ATTR,
    EXTERNAL_DATA_ATTR,
    EXTERNAL_DATA_BUCKET_ATTR,
    EXTERNAL_DATA_KEY_ATTR,
    FINOPS_TYPE,
    HTTPMethod,
    KUBERNETES_TYPE,
    LAST_SCAN_DATE,
    OUTDATED_TENANTS,
    OVERVIEW_TYPE,
    REGION_ATTR,
    RESOURCES_TYPE,
    RULE_TYPE,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from services.clients.s3 import ModularAssumeRoleS3Service, S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.rabbitmq_service import RabbitMQService
from services.setting_service import SettingsService
from validators.swagger_request_models import OperationalGetReportModel
from validators.utils import validate_kwargs
from services.reports_bucket import MetricsBucketKeysBuilder
from services.rbac_service import TenantsAccessPayload

_LOG = get_logger(__name__)

OVERVIEW_REPORT_TYPE = {'maestro': 'CUSTODIAN_OVERVIEW_REPORT',
                        'custodian': 'OVERVIEW'}
RULES_REPORT_TYPE = {'maestro': 'CUSTODIAN_RULES_REPORT',
                     'custodian': 'RULE'}
COMPLIANCE_REPORT_TYPE = {'maestro': 'CUSTODIAN_COMPLIANCE_REPORT',
                          'custodian': 'COMPLIANCE'}
RESOURCES_REPORT_TYPE = {'maestro': 'CUSTODIAN_RESOURCES_REPORT',
                         'custodian': 'RESOURCES'}
ATTACK_REPORT_TYPE = {'maestro': 'CUSTODIAN_ATTACKS_REPORT',
                      'custodian': 'ATTACK_VECTOR'}
FINOPS_REPORT_TYPE = {'maestro': 'CUSTODIAN_FINOPS_REPORT',
                      'custodian': 'FINOPS'}
KUBERNETES_REPORT_TYPE = {'maestro': 'CUSTODIAN_K8S_CLUSTER_REPORT',
                          'custodian': 'KUBERNETES'}
ACCOUNT_METRICS_PATH = '{customer}/accounts/{date}/{account_id}.json'
COMMAND_NAME = 'SEND_MAIL'

RULE_PREFIX = 'rule'
SERVICE_PREFIX = 'service'
REGION_PREFIX = 'region'
POLICY_PREFIX = 'policy'
TACTIC_PREFIX = 'tactic'
TECHNIQUE_PREFIX = 'technique'


class OperationalHandler(AbstractHandler):
    def __init__(self, tenant_service: TenantService,
                 environment_service: EnvironmentService,
                 s3_service: S3Client,
                 settings_service: SettingsService,
                 rabbitmq_service: RabbitMQService,
                 license_service: LicenseService,
                 assume_role_s3: ModularAssumeRoleS3Service):
        self.tenant_service = tenant_service
        self.environment_service = environment_service
        self.s3_service = s3_service
        self.settings_service = settings_service
        self.rabbitmq_service = rabbitmq_service
        self.license_service = license_service
        self.assume_role_s3 = assume_role_s3

        self.recommendation_bucket = self.environment_service.\
            get_recommendation_bucket()

    @classmethod
    def build(cls) -> 'OperationalHandler':
        return cls(
            tenant_service=SERVICE_PROVIDER.modular_client.tenant_service(),
            environment_service=SERVICE_PROVIDER.environment_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            s3_service=SERVICE_PROVIDER.s3,
            rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service,
            license_service=SERVICE_PROVIDER.license_service,
            assume_role_s3=SERVICE_PROVIDER.assume_role_s3
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_OPERATIONAL: {
                HTTPMethod.POST: self.generate_operational_reports,
            }
        }

    @staticmethod
    def get_report_date() -> datetime:
        now = utc_datetime()
        end = now + relativedelta(hour=0, minute=0, second=0, microsecond=0, weekday=SU(+1))
        return end

    @validate_kwargs
    def generate_operational_reports(self, event: OperationalGetReportModel,
                                     context: RequestContext,
                                     _tap: TenantsAccessPayload):
        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        tenant_names = event.tenant_names
        report_types = event.types
        receivers = event.receivers

        json_model = []
        errors = []
        _LOG.debug(f'Report type: {report_types if report_types else "ALL"}')
        for tenant_name in tenant_names:
            if not _tap.is_allowed_for(tenant_name):
                raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                    f'Action is forbidden for tenant {tenant_name}'
                ).exc()
            _LOG.debug(f'Retrieving tenant with name {tenant_name}')
            tenant = self.tenant_service.get(tenant_name)
            if not tenant or event.customer and tenant.customer_name != event.customer:
                _msg = f'Cannot find tenant with name \'{tenant_name}\''
                errors.append(_msg)
                continue
            if not tenant.is_active:
                _msg = f'Custodian is not activated for tenant ' \
                       f'\'{tenant_name}\''
                errors.append(_msg)
                continue
            tenant_metrics = self.s3_service.gz_get_json(
                bucket=metrics_bucket,
                key=MetricsBucketKeysBuilder(tenant).account_metrics(self.get_report_date())
            )
            if not tenant_metrics:
                _msg = f'There is no data for tenant {tenant_name} for the ' \
                       f'last week'
                errors.append(_msg)
                continue

            _LOG.debug('Retrieving tenant data')
            tenant_metrics[OUTDATED_TENANTS] = self._process_outdated_tenants(
                tenant_metrics.pop(OUTDATED_TENANTS, {}))

            overview_data = tenant_metrics.pop(OVERVIEW_TYPE, None)
            rule_data = tenant_metrics.pop(RULE_TYPE, None)
            resources_data = tenant_metrics.pop(RESOURCES_TYPE, None)
            compliance_data = tenant_metrics.pop(COMPLIANCE_TYPE, None)
            attack_data = tenant_metrics.pop(ATTACK_VECTOR_TYPE, None)
            finops_data = tenant_metrics.pop(FINOPS_TYPE, None)
            k8s_data = tenant_metrics.pop(KUBERNETES_TYPE, None)
            for _type, data in [(ATTACK_REPORT_TYPE, attack_data),
                                (COMPLIANCE_REPORT_TYPE, compliance_data),
                                (OVERVIEW_REPORT_TYPE, overview_data),
                                (RESOURCES_REPORT_TYPE, resources_data),
                                (RULES_REPORT_TYPE, rule_data),
                                (FINOPS_REPORT_TYPE, finops_data)]:
                if report_types and _type['custodian'] not in report_types:
                    data.clear()
                    continue
                request_data_format = self._collect_json_model_parameters(
                    data, tenant_metrics[CUSTOMER_ATTR], _type['custodian'])
                json_model.append(self.rabbitmq_service.build_m3_json_model(
                    _type['maestro'], {'receivers': list(receivers),
                                       **tenant_metrics, **request_data_format,
                                       'report_type': _type['custodian']}))
            if not report_types or \
                    KUBERNETES_REPORT_TYPE['custodian'] in report_types:
                if not k8s_data:
                    _msg = f'There is no kubernetes data for tenant ' \
                           f'{tenant_name}'
                    _LOG.warning(_msg)
                    errors.append(_msg)
                else:
                    for cluster, cluster_data in k8s_data.items():
                        region = cluster_data.pop(REGION_ATTR)
                        last_scan_date = cluster_data.pop(LAST_SCAN_DATE)
                        request_data_format = self._collect_json_model_parameters(
                            cluster_data, tenant_metrics[CUSTOMER_ATTR],
                            KUBERNETES_REPORT_TYPE['custodian'])
                        json_model.append(
                            self.rabbitmq_service.build_m3_json_model(
                                KUBERNETES_REPORT_TYPE['maestro'],
                                {'receivers': list(receivers),
                                 **tenant_metrics,
                                 **request_data_format,
                                 'cluster_id': cluster,
                                 REGION_ATTR: region,
                                 LAST_SCAN_DATE: last_scan_date,
                                 'report_type': KUBERNETES_REPORT_TYPE['custodian']}
                            )
                        )
        if errors:
            _LOG.warning(f"Found errors: {os.linesep.join(errors)}")
        if not json_model:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=os.linesep.join(errors)
            )

        _LOG.debug(f'Going to retrieve rabbit mq application by '
                   f'customer: {event.customer_id}')

        rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(
            event.customer_id)
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        code = self.rabbitmq_service.send_notification_to_m3(
            COMMAND_NAME, json_model, rabbitmq)
        if code != 200:
            return build_response(
                code=code,
                content=f'The request to send report'
                        f'{"s" if not report_types else ""} for '
                        f'{tenant_names} tenant'
                        f'{" was" if report_types else " were"} '
                        f'not send.'
            )
        return build_response(
            content=f'The request to send report'
                    f'{"s" if not report_types else ""} for '
                    f'{tenant_names} tenant'
                    f'{" was" if report_types else " were"} '
                    f'successfully created'
        )

    @staticmethod
    def _process_outdated_tenants(outdated_tenants: dict):
        tenants = []
        for cloud, data in outdated_tenants.items():
            tenants.extend(data.keys())
        return tenants

    def save_k8s_data_to_s3(self, content: dict, path: str, id_: str) -> str:
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'w') as file:
            for line in content.get('policy_data', {}):
                resources = line.pop('resources', [])
                file.write(self._build_policy_json_line(line))
                for resource in resources:
                    file.write(self._build_resource_json_line(resource))
            for tactic in content.get('mitre_data', {}):
                techniques = tactic.pop('techniques_data', [])
                file.write(self._build_tactic_json_line(tactic))
                for technique in techniques:
                    resources = technique.pop('resources', [])
                    file.write(self._build_technique_json_line(technique))
                    for resource in resources:
                        file.write(self._build_resource_json_line(resource))
        content.clear()
        _LOG.debug(f'Saving file {path}')
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'rb') as file:
            self.assume_role_s3.put_object(bucket=self.recommendation_bucket,
                                           key=path, body=file.read())
        return path

    def save_mitre_data_to_s3(self, content: dict, path: str, id_: str) -> str:
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'w') as file:
            for tactic in content:
                techniques = tactic.pop('techniques_data', [])
                file.write(self._build_tactic_json_line(tactic))
                for technique in techniques:
                    regions = technique.pop('regions_data', {})
                    file.write(self._build_technique_json_line(technique))
                    for region, resources in regions.items():
                        file.write(self._build_region_json_line(region))
                        for resource in resources.get('resources', []):
                            file.write(self._build_resource_json_line(resource))
        content.clear()
        _LOG.debug(f'Saving file {path}')
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'rb') as file:
            self.assume_role_s3.put_object(bucket=self.recommendation_bucket,
                                           key=path, body=file.read())
        return path

    def save_policy_data_to_s3(self, content: dict, path: str,
                               id_: str) -> str:
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'w') as file:
            for line in content:
                regions = line.pop('regions_data', {})
                file.write(self._build_policy_json_line(line))
                for region, resources in regions.items():
                    file.write(self._build_region_json_line(region))
                    for resource in resources.get('resources', []):
                        file.write(self._build_resource_json_line(resource))
        content.clear()
        _LOG.debug(f'Saving file {path}')
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'rb') as file:
            self.assume_role_s3.put_object(bucket=self.recommendation_bucket,
                                           key=path, body=file.read())
        return path

    def save_finops_data_to_s3(self, content: dict,  path: str,
                               id_: str) -> str:
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'w') as file:
            for line in content:
                rules = line.pop('rules_data', [])
                file.write(self._build_service_json_line(line))
                for rule in rules:
                    regions = rule.pop('regions_data', {})
                    file.write(self._build_rule_json_line(rule))
                    for region, resources in regions.items():
                        file.write(self._build_region_json_line(region))
                        for resource in resources.get('resources', []):
                            file.write(self._build_resource_json_line(resource))
        content.clear()
        _LOG.debug(f'Saving file {path}')
        with open(f'{tempfile.gettempdir()}{os.sep}{id_}.jsonl', 'rb') as file:
            self.assume_role_s3.put_object(bucket=self.recommendation_bucket,
                                           key=path, body=file.read())
        return path

    @staticmethod
    def _build_rule_json_line(line: str) -> str:
        return f'{RULE_PREFIX}{json.dumps(line, separators=(",", ":"))}\n'

    @staticmethod
    def _build_service_json_line(line: str) -> str:
        return f'{SERVICE_PREFIX}{json.dumps(line, separators=(",", ":"))}\n'

    @staticmethod
    def _build_region_json_line(region: str) -> str:
        return f'{REGION_PREFIX}' \
               f'{json.dumps({"key":region}, separators=(",", ":"))}\n'

    @staticmethod
    def _build_policy_json_line(line: dict) -> str:
        return f'{POLICY_PREFIX}{json.dumps(line, separators=(",", ":"))}\n'

    @staticmethod
    def _build_tactic_json_line(line: dict) -> str:
        return f'{TACTIC_PREFIX}{json.dumps(line, separators=(",", ":"))}\n'

    @staticmethod
    def _build_technique_json_line(line: dict) -> str:
        return f'{TECHNIQUE_PREFIX}{json.dumps(line, separators=(",", ":"))}\n'

    @staticmethod
    def _build_resource_json_line(line: dict) -> str:
        return f'{json.dumps(line, separators=(",", ":"))}\n'

    def _collect_json_model_parameters(self, data, customer: str,
                                       report_type: str) -> dict:
        report_type_method_mapping = {
            'KUBERNETES': self.save_k8s_data_to_s3,
            'FINOPS': self.save_finops_data_to_s3,
            'RESOURCES': self.save_policy_data_to_s3,
            'ATTACK_VECTOR': self.save_mitre_data_to_s3
        }

        # [cry] those are different sizes
        if sys.getsizeof(json.dumps(data)) > \
                self.settings_service.get_max_rabbitmq_size() and \
                report_type in report_type_method_mapping:
            id_ = str(uuid.uuid4())
            path = f'{customer}/{id_}.jsonl'
            data_path = report_type_method_mapping[report_type](
                data, path, id_)
            request_data_format = {
                EXTERNAL_DATA_ATTR: True,
                EXTERNAL_DATA_KEY_ATTR: data_path,
                EXTERNAL_DATA_BUCKET_ATTR: self.recommendation_bucket,
                DATA_ATTR: {} if isinstance(data, dict) else []
            }
        else:
            request_data_format = {
                DATA_ATTR: data,
                EXTERNAL_DATA_ATTR: False
            }
        return request_data_format
