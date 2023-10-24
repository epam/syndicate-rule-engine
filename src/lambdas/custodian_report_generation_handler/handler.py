import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Union, Dict, List, Tuple
from uuid import uuid4

from dateutil.relativedelta import relativedelta, SU
from modular_sdk.commons import ModularException
from modular_sdk.commons.constants import ApplicationType
from modular_sdk.commons.constants import RABBITMQ_TYPE
from modular_sdk.services.impl.maestro_rabbit_transport_service import \
    MaestroRabbitMQTransport
from http import HTTPStatus
from helpers import raise_error_response, get_logger, build_response, \
    filter_dict
from helpers.constants import HTTP_METHOD_ERROR, CUSTOMER_ATTR, END_DATE, \
    PARAM_REQUEST_PATH, PARAM_HTTP_METHOD, ACTION_PARAM_ERROR, \
    RULE_TYPE, OVERVIEW_TYPE, RESOURCES_TYPE, COMPLIANCE_TYPE, \
    TENANT_DISPLAY_NAME_ATTR, DATA_ATTR, ATTACK_VECTOR_TYPE, START_DATE, \
    TENANT_NAMES_ATTR, TENANT_DISPLAY_NAMES_ATTR, TACTICS_ID_MAPPING, \
    FINOPS_TYPE, HTTPMethod, OUTDATED_TENANTS
from helpers.difference import calculate_dict_diff
from helpers.time_helper import utc_datetime
from models.licenses import License
from models.modular.application import CustodianLicensesApplicationMeta
from services import SERVICE_PROVIDER
from services.abstract_api_handler_lambda import AbstractApiHandlerLambda
from services.abstract_lambda import AbstractLambda
from services.batch_results_service import BatchResultsService
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import LicenseService
from services.metrics_service import CustomerMetricsService
from services.metrics_service import TenantMetricsService
from services.modular_service import ModularService
from services.rabbitmq_service import RabbitMQService
from services.rule_meta_service import LazyLoadedMappingsCollector
from services.setting_service import SettingsService

ACCOUNT_METRICS_PATH = '{customer}/accounts/{date}/{account_id}.json'
TENANT_METRICS_PATH = '{customer}/tenants/{date}/{tenant_dn}.json'

COMMAND_NAME = 'SEND_MAIL'
TYPES_ATTR = 'types'
RECIEVERS_ATTR = 'recievers'

EVENT_DRIVEN_TYPE = {'maestro': 'CUSTODIAN_EVENT_DRIVEN_RESOURCES_REPORT',
                     'custodian': 'EVENT_DRIVEN'}
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
PROJECT_OVERVIEW_REPORT_TYPE = {'maestro': 'CUSTODIAN_PROJECT_OVERVIEW_REPORT',
                                'custodian': 'OVERVIEW'}
PROJECT_RESOURCES_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_PROJECT_RESOURCES_REPORT',
    'custodian': 'RESOURCES'}
PROJECT_COMPLIANCE_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_PROJECT_COMPLIANCE_REPORT',
    'custodian': 'COMPLIANCE'}
PROJECT_ATTACK_REPORT_TYPE = {'maestro': 'CUSTODIAN_PROJECT_ATTACKS_REPORT',
                              'custodian': 'ATTACK_VECTOR'}
PROJECT_FINOPS_REPORT_TYPE = {'maestro': 'CUSTODIAN_PROJECT_FINOPS_REPORT',
                              'custodian': 'FINOPS'}
TOP_RESOURCES_BY_CLOUD_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_RESOURCES_BY_CLOUD_REPORT',
    'custodian': 'TOP_RESOURCES_BY_CLOUD'}
TOP_TENANTS_RESOURCES_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_TENANTS_VIOLATED_RESOURCES_REPORT',
    'custodian': 'TOP_TENANTS_RESOURCES'}
TOP_TENANTS_COMPLIANCE_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_TENANTS_COMPLIANCE_REPORT',
    'custodian': 'TOP_TENANTS_COMPLIANCE'}
TOP_COMPLIANCE_BY_CLOUD_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_COMPLIANCE_BY_CLOUD_REPORT',
    'custodian': 'TOP_COMPLIANCE_BY_CLOUD'}
TOP_TENANTS_ATTACK_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_TENANTS_ATTACKS_REPORT',
    'custodian': 'TOP_TENANTS_ATTACKS'}
TOP_ATTACK_BY_CLOUD_REPORT_TYPE = {
    'maestro': 'CUSTODIAN_TOP_TENANTS_BY_CLOUD_ATTACKS_REPORT',
    'custodian': 'TOP_ATTACK_BY_CLOUD'}
CUSTOMER_OVERVIEW_REPORT_TYPE = 'CUSTODIAN_CUSTOMER_OVERVIEW_REPORT'
CUSTOMER_COMPLIANCE_REPORT_TYPE = 'CUSTODIAN_CUSTOMER_COMPLIANCE_REPORT'
CUSTOMER_ATTACKS_REPORT_TYPE = 'CUSTODIAN_CUSTOMER_ATTACKS_REPORT'

CLOUDS = ['aws', 'azure', 'google']

_LOG = get_logger(__name__)


class ReportGenerator(AbstractApiHandlerLambda):
    def __init__(self, s3_service: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 modular_service: ModularService,
                 tenant_metrics_service: TenantMetricsService,
                 customer_metrics_service: CustomerMetricsService,
                 license_service: LicenseService,
                 batch_results_service: BatchResultsService,
                 rabbitmq_service: RabbitMQService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.s3_service = s3_service
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.modular_service = modular_service
        self.tenant_metrics_service = tenant_metrics_service
        self.customer_metrics_service = customer_metrics_service
        self.license_service = license_service
        self.batch_results_service = batch_results_service
        self.rabbitmq_service = rabbitmq_service
        self.mappings_collector = mappings_collector

        self.REQUEST_PATH_HANDLER_MAPPING = {
            '/reports/operational': {
                HTTPMethod.GET: self.generate_operational_reports
            },
            '/reports/project': {
                HTTPMethod.GET: self.generate_project_reports
            },
            '/reports/department': {
                HTTPMethod.GET: self.generate_department_reports
            },
            '/reports/clevel': {
                HTTPMethod.GET: self.generate_c_level_reports
            },
            '/reports/event_driven': {
                HTTPMethod.GET: self.generate_event_driven_reports
            }
        }

        self.customer_license_mapping = {}
        self.customer_tenant_mapping = {}
        self.current_month = datetime.today().replace(day=1).date()

    def handle_request(self, event, context):
        request_path = event[PARAM_REQUEST_PATH]
        method_name = event[PARAM_HTTP_METHOD]
        handler_function = self.REQUEST_PATH_HANDLER_MAPPING.get(request_path)
        if not handler_function:
            return build_response(
                code=HTTPStatus.BAD_REQUEST.value,
                content=ACTION_PARAM_ERROR.format(endpoint=request_path)
            )
        handler_func = handler_function.get(method_name)
        response = None
        if handler_func:
            response = handler_func(event=event)
        return response or build_response(
            code=HTTPStatus.BAD_REQUEST.value,
            content=HTTP_METHOD_ERROR.format(
                method=method_name, resource=request_path
            )
        )

    def generate_event_driven_reports(self, event):
        licenses = self.license_service.get_event_driven_licenses()
        if not licenses:
            return build_response('There are no active event-driven licenses. '
                                  'Cannot build event-driven report')
        for l in licenses:
            _LOG.debug(f'Processing license {l.license_key}')
            quota = l.event_driven.quota
            if not quota:
                _LOG.warning(
                    f'There is no quota for ED notifications in license '
                    f'\'{l.license_key}\'. Cannot send emails')
                continue
            start_date, end_date = self._get_period(quota)
            if datetime.utcnow().timestamp() < end_date.timestamp() or (
                    l.event_driven.last_execution and
                    start_date.isoformat() <= l.event_driven.last_execution <= end_date.isoformat()):
                _LOG.debug(f'Skipping ED report for license {l.license_key}: '
                           f'timestamp now: {datetime.now().isoformat()}; '
                           f'expected end timestamp: {end_date.isoformat()}')
                continue
            _LOG.debug(f'Start timestamp: {start_date.isoformat()}; '
                       f'end timestamp {end_date.isoformat()}')
            for customer, data in l.customers.as_dict().items():
                _LOG.debug(f'Processing customer {customer}')
                tenants = data.get('tenants')
                if customer not in self.customer_license_mapping:
                    tenants = self.customer_license_mapping.setdefault(
                        customer, {}).setdefault('tenants', []) + tenants
                    self.customer_license_mapping[customer]['tenants'] = list(
                        set(tenants))
                    self.customer_license_mapping[customer][
                        START_DATE] = start_date.replace(
                        tzinfo=None).isoformat()
                    self.customer_license_mapping[customer][
                        END_DATE] = end_date.replace(tzinfo=None).isoformat()
                    self.customer_license_mapping[customer]['license'] = l

        if not self.customer_license_mapping or not any(
                self.customer_license_mapping.values()):
            return build_response(
                f'There are no any active event-driven licenses')

        for customer, info in self.customer_license_mapping.items():
            self.customer_tenant_mapping[customer] = {}
            results = self.batch_results_service.get_between_period_by_customer(
                customer_name=customer, tenants=info['tenants'],
                start=info[START_DATE], end=info[END_DATE], limit=100
            )
            for item in results:
                self.customer_tenant_mapping[customer].setdefault(
                    item.tenant_name, []).append(item.id)

        if not self.customer_tenant_mapping or not any(
                self.customer_tenant_mapping.values()):
            return build_response(
                f'There are no event-driven jobs for period '
                f'{start_date.isoformat()} to {end_date.isoformat()}')

        for customer, tenants in self.customer_tenant_mapping.items():
            rabbitmq = self.get_customer_rabbitmq(customer)
            if not rabbitmq:
                continue

            bucket_name = self.environment_service.default_reports_bucket_name()
            with ThreadPoolExecutor() as executor:
                for tenant, items in tenants.items():
                    _LOG.debug(f'Processing {len(items)} ED scan(s) of '
                               f'{tenant} tenant')
                    futures = []
                    tenant_item = next(self.modular_service.i_get_tenant(
                        [tenant]), None)
                    futures.append(executor.submit(self._process, bucket_name,
                                                   items))
                    for future in as_completed(futures):
                        new_resources = future.result()
                        if not new_resources:
                            _LOG.warning(
                                f'No new resources for tenant {tenant}')
                            continue
                        mitre_data = self._retrieve_mitre_data(new_resources)

                        data = {
                            'customer': customer,
                            'tenant_name': tenant,
                            'cloud': tenant_item.cloud,
                            'id': tenant_item.project,
                            'activated_regions': list(
                                self.modular_service.get_tenant_regions(
                                    tenant_item)),
                            'data': {
                                'policy_data': new_resources,
                                'mitre_data': mitre_data
                            },
                            'from': self.customer_license_mapping[customer][
                                START_DATE],
                            'to': self.customer_license_mapping[customer][
                                END_DATE]
                        }
                        _LOG.debug(f'Data: {data}')
                        json_model = self._build_json_model(
                            EVENT_DRIVEN_TYPE['maestro'],
                            {**data,
                             'report_type': EVENT_DRIVEN_TYPE['custodian']})
                        self._send_notification_to_m3(json_model, rabbitmq)
                        _LOG.debug(f'Notification for {tenant} tenant was '
                                   f'successfully send')

                    customer_license = self.customer_license_mapping[customer]
                    _LOG.debug(
                        f'Updating last report execution date for license '
                        f'{customer_license["license"].license_key}')
                    self.license_service.update_last_ed_report_execution(
                        customer_license['license'],
                        last_execution_date=customer_license[END_DATE])

        return build_response(
            content='Reports sending was successfully triggered'
        )

    def generate_operational_reports(self, event):
        date = self.settings_service.get_report_date_marker().get(
            'current_week_date')
        if not date:
            _LOG.warning('Missing \'current_week_date\' section in '
                         '\'REPORT_DATE_MARKER\' setting.')
            date = (datetime.today() + relativedelta(
                weekday=SU(0))).date().isoformat()

        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        tenant_names = list(filter(
            None, event.get(TENANT_NAMES_ATTR, '').split(', ')))
        report_types = list(filter(
            None, event.get(TYPES_ATTR, '').split(', ')))
        receivers = list(filter(
            None, event.get(RECIEVERS_ATTR, '').split(', ')))

        if not tenant_names:
            return raise_error_response(
                code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                content='"tenant_names" parameter cannot be empty'
            )

        json_model = []
        errors = []
        _LOG.debug(f'Report type: {report_types if report_types else "ALL"}')
        for tenant_name in tenant_names:
            _LOG.debug(f'Retrieving tenant with name {tenant_name}')
            tenant = self.modular_service.get_tenant(tenant_name)
            if not tenant:
                _msg = f'Cannot find tenant with name \'{tenant_name}\''
                errors.append(_msg)
                continue

            tenant_metrics = self.s3_service.get_file_content(
                bucket_name=metrics_bucket,
                full_file_name=ACCOUNT_METRICS_PATH.format(
                    customer=tenant.customer_name,
                    date=date, account_id=tenant.project))
            if not tenant_metrics:
                _msg = f'There is no data for tenant {tenant_name} for the ' \
                       f'last week'
                errors.append(_msg)
                continue

            _LOG.debug('Retrieving tenant data')
            tenant_metrics = json.loads(tenant_metrics)
            tenant_metrics[OUTDATED_TENANTS] = self._process_outdated_tenants(
                tenant_metrics.pop(OUTDATED_TENANTS, {}))

            overview_data = tenant_metrics.pop(OVERVIEW_TYPE, None)
            rule_data = tenant_metrics.pop(RULE_TYPE, None)
            resources_data = tenant_metrics.pop(RESOURCES_TYPE, None)
            compliance_data = tenant_metrics.pop(COMPLIANCE_TYPE, None)
            attack_data = tenant_metrics.pop(ATTACK_VECTOR_TYPE, None)
            finops_data = tenant_metrics.pop(FINOPS_TYPE, None)
            for _type, data in [(ATTACK_REPORT_TYPE, attack_data),
                                (COMPLIANCE_REPORT_TYPE, compliance_data),
                                (OVERVIEW_REPORT_TYPE, overview_data),
                                (RESOURCES_REPORT_TYPE, resources_data),
                                (RULES_REPORT_TYPE, rule_data),
                                (FINOPS_REPORT_TYPE, finops_data)]:
                if report_types and _type['custodian'] not in report_types:
                    continue
                json_model.append(self._build_json_model(
                    _type['maestro'], {RECIEVERS_ATTR: receivers,
                                       **tenant_metrics, DATA_ATTR: data,
                                       'report_type': _type['custodian']}))
        if errors:
            _LOG.error(f"Found errors: {os.linesep.join(errors)}")
        if not json_model:
            return build_response(
                content=os.linesep.join(errors)
            )

        rabbitmq = self.get_customer_rabbitmq(event[CUSTOMER_ATTR])
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        self._send_notification_to_m3(json_model, rabbitmq)
        return build_response(
            content=f'The request to send report'
                    f'{"s" if not report_types else ""} for '
                    f'{tenant_names} tenant'
                    f'{" was" if report_types else " were"} '
                    f'successfully created'
        )

    def generate_project_reports(self, event):
        if not (date := self.settings_service.get_report_date_marker().get(
                'current_week_date')):
            return raise_error_response(
                code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                content='Cannot send reports: '
                        'missing \'current_week_date\' section in '
                        '\'REPORT_DATE_MARKER\' setting.'
            )

        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        tenant_display_names = list(filter(
            None, event.get(TENANT_DISPLAY_NAMES_ATTR, '').split(', ')))
        report_types = list(filter(
            None, event.get(TYPES_ATTR, '').split(', ')))
        receivers = list(filter(
            None, event.get(RECIEVERS_ATTR, '').split(', ')))

        if not tenant_display_names:
            return raise_error_response(
                code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                content='"tenant_display_names" parameter cannot be empty'
            )

        json_model = []
        errors = []
        for display_name in tenant_display_names:
            _LOG.debug(
                f'Retrieving tenants with display name \'{display_name}\'')
            tenants = list(
                self.modular_service.i_get_tenant_by_display_name_to_lower(
                    display_name.lower()))
            if not tenants:
                _msg = \
                    f'Cannot find tenants with display name \'{display_name}\''
                _LOG.error(_msg)
                errors.append(_msg)
                continue

            tenant = list(tenants)[0]
            tenant_group_metrics = self.s3_service.get_file_content(
                bucket_name=metrics_bucket,
                full_file_name=TENANT_METRICS_PATH.format(
                    customer=tenant.customer_name,
                    date=date, tenant_dn=display_name))
            if not tenant_group_metrics:
                _msg = f'There is no data for \'{display_name}\' tenant ' \
                       f'group for the last week'
                _LOG.warning(_msg)
                errors.append(_msg)
                continue

            tenant_group_metrics = json.loads(tenant_group_metrics)
            tenant_group_metrics[OUTDATED_TENANTS] = self._process_outdated_tenants(
                tenant_group_metrics.pop(OUTDATED_TENANTS, {}))

            overview_data = tenant_group_metrics.pop(OVERVIEW_TYPE, None)
            resources_data = tenant_group_metrics.pop(RESOURCES_TYPE, None)
            compliance_data = tenant_group_metrics.pop(COMPLIANCE_TYPE, None)
            attack_data = tenant_group_metrics.pop(ATTACK_VECTOR_TYPE, None)
            finops_data = tenant_group_metrics.pop(FINOPS_TYPE, None)
            for _type, data in [
                    (PROJECT_ATTACK_REPORT_TYPE, attack_data),
                    (PROJECT_COMPLIANCE_REPORT_TYPE, compliance_data),
                    (PROJECT_OVERVIEW_REPORT_TYPE, overview_data),
                    (PROJECT_RESOURCES_REPORT_TYPE, resources_data),
                    (PROJECT_FINOPS_REPORT_TYPE, finops_data)]:
                if report_types and _type['custodian'] not in report_types:
                    continue
                json_model.append(self._build_json_model(
                    _type['maestro'], {RECIEVERS_ATTR: receivers,
                                       **tenant_group_metrics, DATA_ATTR: data,
                                       'report_type': _type['custodian']}))

        if not json_model:
            return build_response(
                code=HTTPStatus.BAD_REQUEST.value,
                content=';\n'.join(errors)
            )
        rabbitmq = self.get_customer_rabbitmq(event[CUSTOMER_ATTR])
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        self._send_notification_to_m3(json_model, rabbitmq)
        return build_response(
            content=f'The request to send reports for {tenant_display_names} '
                    f'tenant group were successfully created'
        )

    def generate_department_reports(self, event):
        top_compliance_by_tenant = []
        top_compliance_by_cloud = {c: [] for c in CLOUDS}
        top_resource_by_tenant = []
        top_resource_by_cloud = {c: [] for c in CLOUDS}
        top_attack_by_tenant = []
        top_attack_by_cloud = {c: [] for c in CLOUDS}
        report_type_mapping = {
            'RESOURCES_BY_CLOUD': {
                'report_type': TOP_RESOURCES_BY_CLOUD_REPORT_TYPE,
                'container': top_resource_by_cloud,
                OUTDATED_TENANTS: []
            },
            'COMPLIANCE_BY_TENANT': {
                'report_type': TOP_TENANTS_COMPLIANCE_REPORT_TYPE,
                'container': top_compliance_by_tenant,
                OUTDATED_TENANTS: []
            },
            'RESOURCES_BY_TENANT': {
                'report_type': TOP_TENANTS_RESOURCES_REPORT_TYPE,
                'container': top_resource_by_tenant,
                OUTDATED_TENANTS: []
            },
            'COMPLIANCE_BY_CLOUD': {
                'report_type': TOP_COMPLIANCE_BY_CLOUD_REPORT_TYPE,
                'container': top_compliance_by_cloud,
                OUTDATED_TENANTS: []
            },
            'ATTACK_BY_TENANT': {
                'report_type': TOP_TENANTS_ATTACK_REPORT_TYPE,
                'container': top_attack_by_tenant,
                OUTDATED_TENANTS: []
            },
            'ATTACK_BY_CLOUD': {
                'report_type': TOP_ATTACK_BY_CLOUD_REPORT_TYPE,
                'container': top_attack_by_cloud,
                OUTDATED_TENANTS: []
            }
        }
        previous_month = (self.current_month - timedelta(days=1)).replace(
            day=1)
        customer = event[CUSTOMER_ATTR]
        report_types = list(filter(
            None, event.get(TYPES_ATTR, '').split(', ')))

        top_tenants = self.tenant_metrics_service.list_by_date_and_customer(
            date=self.current_month.isoformat(), customer=customer)
        if len(top_tenants) == 0:
            return build_response(f'There are no metrics for customer '
                                  f'{customer} for the period from '
                                  f'{self.current_month.isoformat()} to '
                                  f'{previous_month}')
        for tenant in top_tenants:
            tenant = tenant.attribute_values
            _LOG.debug(f'Retrieving previous item for tenant '
                       f'{tenant.get("tenant_display_name")}, '
                       f'type {tenant.get("type")}')
            prev_item = self.tenant_metrics_service.get_by_tenant_date_type(
                tenant=tenant['tenant_display_name'],
                date=previous_month.isoformat(),
                top_type=tenant['type']
            )
            prev_item = self._get_attr_values(prev_item)

            tenant.pop('date')
            tenant.pop('id')

            # Define attribute dictionaries and mapping
            cloud_attrs = {
                'aws': tenant.get('aws'),
                'azure': tenant.get('azure'),
                'google': tenant.get('google')
            }
            prev_cloud_attrs = {
                'aws': prev_item.get('aws') if prev_item else {},
                'azure': prev_item.get('azure') if prev_item else {},
                'google': prev_item.get('google') if prev_item else {}}

            for c in CLOUDS:
                if isinstance(cloud_attrs[c], str):
                    cloud_attrs[c] = json.loads(cloud_attrs[c])
                if isinstance(prev_cloud_attrs[c], str):
                    prev_cloud_attrs[c] = json.loads(prev_cloud_attrs[c])

            # Calculate diff between current and previous dictionaries
            item_type = tenant.get('type')
            attribute_diff = cloud_attrs
            if item_type not in ('ATTACK_BY_CLOUD', 'ATTACK_BY_TENANT'):
                attribute_diff = calculate_dict_diff(cloud_attrs,
                                                     prev_cloud_attrs)

            # Process the item
            if '_BY_CLOUD' in item_type:
                self.process_department_item_by_cloud(
                    item_type, attribute_diff, report_type_mapping, tenant)
            else:
                report_type_mapping[item_type][OUTDATED_TENANTS].extend(
                    tenant.get(OUTDATED_TENANTS, []))
                report_type_mapping[item_type]['container'].append({
                    TENANT_DISPLAY_NAME_ATTR: tenant.pop(
                        TENANT_DISPLAY_NAME_ATTR),
                    'sort_by': tenant.pop('defining_attribute'),
                    DATA_ATTR: attribute_diff
                })
        rabbitmq = self.get_customer_rabbitmq(event[CUSTOMER_ATTR])
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        for _type, values in report_type_mapping.items():
            if report_types and report_type_mapping[_type]['report_type'][
                'custodian'] not in report_types:
                continue
            container = values['container']
            if (isinstance(container, list) and not container) or (
                    isinstance(container, dict) and not any(
                container.values())):
                _LOG.warning(f'No data for report type {_type}')
                continue
            json_model = self._build_json_model(
                report_type_mapping[_type]['report_type']['maestro'],
                {CUSTOMER_ATTR: customer, 'from': previous_month.isoformat(),
                 'to': self.current_month.isoformat(),
                 OUTDATED_TENANTS: values[OUTDATED_TENANTS],
                 'report_type': _type, DATA_ATTR: values['container']})
            self._send_notification_to_m3(json_model, rabbitmq)
            _LOG.debug(f'Notifications for {customer} customer have been '
                       f'sent successfully')
        return build_response(
            content=f'Reports sending for {customer} customer have been '
                    f'triggered successfully'
        )

    def generate_c_level_reports(self, event):
        report_type_mapping = {
            OVERVIEW_TYPE.upper(): CUSTOMER_OVERVIEW_REPORT_TYPE,
            COMPLIANCE_TYPE.upper(): CUSTOMER_COMPLIANCE_REPORT_TYPE,
            ATTACK_VECTOR_TYPE.upper(): CUSTOMER_ATTACKS_REPORT_TYPE
        }
        previous_month = (self.current_month - timedelta(days=1)).replace(
            day=1)

        customer = event[CUSTOMER_ATTR]
        report_types = list(filter(
            None, event.get(TYPES_ATTR, '').split(', ')))

        customer_metrics = self.customer_metrics_service.list_by_date_and_customer(
            date=self.current_month.isoformat(), customer=customer)
        _LOG.debug(f'Retrieved {len(customer_metrics)} items for customer '
                   f'{customer}')
        if len(customer_metrics) == 0:
            return build_response(f'There are no metrics for customer '
                                  f'{customer} for the period from '
                                  f'{self.current_month.isoformat()} to '
                                  f'{previous_month}')
        rabbitmq = self.get_customer_rabbitmq(customer)
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        for item in customer_metrics:
            item = item.attribute_values
            if report_types and item.get(TYPES_ATTR) not in report_types:
                continue

            if item.get('type') != ATTACK_VECTOR_TYPE.upper():
                prev_item = self.customer_metrics_service.get_by_customer_date_type(
                    customer=customer,
                    date=previous_month.isoformat(),
                    _type=item['type']
                )
                prev_item = self._get_attr_values(prev_item)

                # Define attribute dictionaries and mapping
                prev_cloud_attrs = {
                    'aws': self._get_attr_values(prev_item.get('aws')),
                    'azure': self._get_attr_values(prev_item.get('azure')),
                    'google': self._get_attr_values(prev_item.get('google'))
                }

                cloud_attrs = {
                    'aws': {'activated': False, 'license_properties': {},
                            **item.get('aws').attribute_values},
                    'azure': {'activated': False, 'license_properties': {},
                              **item.get('azure').attribute_values},
                    'google': {'activated': False, 'license_properties': {},
                               **item.get('google').attribute_values}}

                # Calculate diff between current and previous dictionaries
                attribute_diff = calculate_dict_diff(
                    cloud_attrs, prev_cloud_attrs,
                    exclude=['total_scanned_tenants'])
                applications = list(self.modular_service.get_applications(
                    customer=customer,
                    _type=ApplicationType.CUSTODIAN_LICENSES.value
                ))
            else:
                # attack report should not contain license info
                cloud_attrs = {
                    'aws': item.get('aws').attribute_values.get(
                        'data', []),
                    'azure': item.get('azure').attribute_values.get(
                        'data', []),
                    'google': item.get('google').attribute_values.get(
                        'data', [])}
                attribute_diff = cloud_attrs
                applications = []

            self._get_application_info(applications, attribute_diff)

            model = {
                DATA_ATTR: attribute_diff,
                CUSTOMER_ATTR: item.get(CUSTOMER_ATTR),
                'from': previous_month.isoformat(),
                'to': self.current_month.isoformat(),
                OUTDATED_TENANTS: item.get(OUTDATED_TENANTS, [])
            }

            model.update({'report_type': item.get('type')})
            json_model = self._build_json_model(
                report_type_mapping.get(item.get('type')), model)
            self._send_notification_to_m3(json_model, rabbitmq)

        _LOG.debug(f'Reports sending for {customer} customer have been '
                   f'triggered successfully')
        return build_response(
            content=f'Reports sending for {customer} customer have been '
                    f'triggered successfully'
        )

    @staticmethod
    def _get_license_info(_license: License) -> Dict:
        balance = _license.allowance.attribute_values['job_balance']
        time_range = _license.allowance.attribute_values['time_range']
        scan_frequency = f'{balance} scan{"" if balance == 1 else "s"} per ' \
                         f'{time_range}'
        return {
            'activated': True,
            'license_properties': {
                'Scans frequency': scan_frequency,
                'Expiration': _license.expiration,
                'Event-Driven mode': 'On' if _license.event_driven else 'Off'
            }
        }

    @staticmethod
    def _send_notification_to_m3(json_model: Union[list, dict],
                                 rabbitmq: MaestroRabbitMQTransport) -> None:
        try:
            code, status, response = rabbitmq.send_sync(
                command_name=COMMAND_NAME,
                parameters=json_model,
                is_flat_request=False, async_request=False,
                secure_parameters=None, compressed=True)
            _LOG.debug(f'Response code: {code}, response message: {response}')
        except ModularException as e:
            _LOG.error(f'Modular error: {e}')
            return build_response(
                code=HTTPStatus.SERVICE_UNAVAILABLE.value,
                content='An error occurred while sending the report. '
                        'Please contact the support team.'
            )
        except Exception as e:  # can occur in case access data is invalid
            _LOG.error(
                f'An error occurred trying to send a message to rabbit: {e}')
            return build_response(
                code=HTTPStatus.SERVICE_UNAVAILABLE.value,
                content='An error occurred while sending the report. '
                        'Please contact the support team.'
            )

    def _process(self, bucket_name, items):
        """Merges differences"""
        content = {}
        differences = self.s3_service.get_json_batch(
            bucket_name=bucket_name,
            keys=[f'{i}/difference.json.gz' for i in items]
        )
        for file in differences:
            _LOG.debug(f'Processing file {file[0]}')
            for rule, resource in file[1].items():
                report_fields = self.mappings_collector.human_data.get(
                    rule, {}).get('report_fields') or set()

                resource['severity'] = self.mappings_collector.severity.get(
                    rule, 'Unknown')
                resource.pop('report_fields', None)
                resource.pop('standard_points', None)
                resource.pop('resourceType', None)
                resource['regions_data'] = resource.pop('resources', {})
                resource[
                    'resource_type'] = self.mappings_collector.service.get(
                    rule)

                filtered_resources = {}
                for region, data in resource['regions_data'].items():
                    filtered_resources.setdefault(region, []).extend(
                        filter_dict(d, report_fields) for d in data)
                if filtered_resources:
                    resource['regions_data'] = filtered_resources

                if rule not in content:
                    content[rule] = resource
                else:
                    for region, data in resource['regions_data'].items():
                        content[rule]['regions_data'].setdefault(
                            region, []).extend(data)
        return [{'policy': k, **v} for k, v in content.items()]

    @staticmethod
    def _get_period(frequency: int, last_execution: str = None) -> \
            Tuple[datetime, datetime]:
        _LOG.debug('No last execution date')
        now = utc_datetime()
        minutes = frequency % 60
        last_execution = now.replace(
            second=0, microsecond=0,
            minute=((now.minute // minutes) * minutes)
            if minutes != 0 else 0) - timedelta(minutes=frequency)
        # else:
        #     last_execution = dateutil.parser.isoparse(last_execution)

        end = last_execution + timedelta(minutes=frequency)
        return last_execution, end

    def _retrieve_mitre_data(self, resources: list) -> List[dict]:
        mitre = {}
        result = []
        for resource in resources:
            severity = self.mappings_collector.severity.get(resource['policy'],
                                                            [])
            attack_vector = self.mappings_collector.mitre.get(
                resource['policy'], [])
            for attack in attack_vector:
                resources = sum(
                    [len(data) for data in resource['regions_data'].values()])
                severity_data = mitre.setdefault(attack, {}).setdefault(
                    severity, {'value': 0, 'diff': -1})
                severity_data['value'] += resources

        for tactic, data in mitre.items():
            result.append({'tactic_id': TACTICS_ID_MAPPING.get(tactic),
                           'tactic': tactic, 'severity_data': data})
        return result

    def _get_application_info(self, applications: list, attribute_diff: dict):
        for ap in applications:
            # todo pass several licenses to BE
            meta = CustodianLicensesApplicationMeta(
                **ap.meta.as_dict()
            )
            for cloud in CLOUDS:
                if not (l := meta.license_key(cloud)):
                    continue
                if attribute_diff[cloud].get('activated') is True:
                    continue
                license_item = self.license_service.get_license(l)
                if not license_item:
                    _LOG.warning(f'Invalid license key in Application '
                                 f'meta for cloud {cloud}')
                    continue
                attribute_diff[cloud].update(self._get_license_info(
                    license_item))

    @staticmethod
    def _get_attr_values(item, default={}):
        return item.attribute_values if item else default

    @staticmethod
    def _build_json_model(notification_type, data):
        return {
            'viewType': 'm3',
            'model': {
                "uuid": str(uuid4()),
                "notificationType": notification_type,
                "notificationAsJson": json.dumps(data,
                                                 separators=(",", ":")),
                "notificationProcessorTypes": ["MAIL"]
            }
        }

    def process_department_item_by_cloud(self, item_type, attribute_diff,
                                         report_type_mapping,
                                         tenant):
        if all(not attribute_diff.get(c) for c in CLOUDS):
            report_type_mapping.get(item_type).pop('container')
            return

        for cloud, value in attribute_diff.items():
            self.process_cloud_data(item_type, attribute_diff, cloud, value,
                                    report_type_mapping, tenant)

    def process_cloud_data(self, item_type, attribute_diff, cloud, value,
                           report_type_mapping, tenant):
        value = json.loads(value) if isinstance(value, str) else value
        if value:
            report_type_mapping[item_type][OUTDATED_TENANTS].extend(
                tenant.get(OUTDATED_TENANTS, []))
            data = attribute_diff[cloud]['average_data'] \
                if item_type == 'COMPLIANCE_BY_CLOUD' else attribute_diff[
                cloud]
            if item_type == 'ATTACK_BY_CLOUD':
                self.add_attack_by_cloud_data(report_type_mapping, tenant,
                                              cloud, data)
            else:
                self.add_other_data(report_type_mapping, tenant, item_type,
                                    cloud, data)

    @staticmethod
    def add_attack_by_cloud_data(report_type_mapping, tenant, cloud, data):
        report_type_mapping['ATTACK_BY_CLOUD']['container'][cloud].append({
            TENANT_DISPLAY_NAME_ATTR: tenant.pop(TENANT_DISPLAY_NAME_ATTR),
            'sort_by': tenant.pop('defining_attribute'),
            **data
        })

    @staticmethod
    def add_other_data(report_type_mapping, tenant, item_type, cloud, data):
        report_type_mapping[item_type]['container'][cloud].append({
            TENANT_DISPLAY_NAME_ATTR: tenant.pop(TENANT_DISPLAY_NAME_ATTR),
            'sort_by': tenant.pop('defining_attribute'),
            DATA_ATTR: data
        })

    def get_customer_rabbitmq(self, customer):
        application = self.rabbitmq_service.get_rabbitmq_application(
            customer)
        if not application:
            _LOG.warning(f'No application with type {RABBITMQ_TYPE} found '
                         f'for customer {customer}')
            return
        rabbitmq = self.rabbitmq_service.build_maestro_mq_transport(
            application)
        if not rabbitmq:
            _LOG.warning(f'Could not build rabbit client from application '
                         f'for customer {customer}')
            return
        return rabbitmq

    @staticmethod
    def _process_outdated_tenants(outdated_tenants: dict):
        tenants = []
        for cloud, data in outdated_tenants.items():
            tenants.extend(list(data.keys()))
        return tenants


HANDLER = ReportGenerator(
    environment_service=SERVICE_PROVIDER.environment_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    s3_service=SERVICE_PROVIDER.s3(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    tenant_metrics_service=SERVICE_PROVIDER.tenant_metrics_service(),
    customer_metrics_service=SERVICE_PROVIDER.customer_metrics_service(),
    license_service=SERVICE_PROVIDER.license_service(),
    batch_results_service=SERVICE_PROVIDER.batch_results_service(),
    rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service(),
    mappings_collector=SERVICE_PROVIDER.mappings_collector()
)


def lambda_handler(event, context):
    if event.get(PARAM_REQUEST_PATH) == '/reports/event_driven':
        return AbstractLambda.lambda_handler(HANDLER, event, context)

    return HANDLER.lambda_handler(event=event, context=context)
