from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import inspect
from functools import cached_property
from http import HTTPStatus
from dateutil.relativedelta import SU, relativedelta
import json
from typing import Mapping, TYPE_CHECKING

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.commons.exception import ModularException
from modular_sdk.models.application import Application
from modular_sdk.models.tenant import Tenant
from modular_sdk.modular import Modular

from helpers import RequestContext, filter_dict
from helpers.constants import (
    ATTACK_VECTOR_TYPE,
    COMPLIANCE_TYPE,
    CUSTOMER_ATTR,
    CustodianEndpoint,
    DATA_ATTR,
    END_DATE,
    FINOPS_TYPE,
    HTTPMethod,
    OUTDATED_TENANTS,
    OVERVIEW_TYPE,
    RESOURCES_TYPE,
    ReportDispatchStatus,
    RuleDomain,
    START_DATE,
    TACTICS_ID_MAPPING,
    TENANT_DISPLAY_NAME_ATTR,
)
from helpers.difference import calculate_dict_diff
from helpers.lambda_response import (
    CustodianException,
    ReportNotSendException,
    ResponseFactory,
    build_response,
)
from helpers.log_helper import get_logger, hide_secret_values
from helpers.time_helper import utc_datetime
from lambdas.custodian_report_generation_handler.handlers.diagnostic_handler import (
    DiagnosticHandler,
)
from lambdas.custodian_report_generation_handler.handlers.operational_handler import (
    OperationalHandler,
)
from lambdas.custodian_report_generation_handler.handlers.retry_handler import (
    RetryHandler,
)
from models.batch_results import BatchResults
from services import SERVICE_PROVIDER
from services.abs_lambda import (
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    EventProcessorLambdaHandler,
    ExpandEnvironmentEventProcessor,
    ProcessedEvent,
    RestrictCustomerEventProcessor,
    RestrictTenantEventProcessor,
)
from services.batch_results_service import BatchResultsService
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.license_service import License, LicenseService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.metrics_service import CustomerMetricsService
from services.metrics_service import TenantMetricsService
from services.modular_helpers import get_tenant_regions
from services.rabbitmq_service import RabbitMQService
from services.report_statistics_service import ReportStatisticsService
from services.reports_bucket import TenantReportsBucketKeysBuilder, MetricsBucketKeysBuilder
from services.ruleset_service import RulesetService
from services.setting_service import SettingsService
from services.sharding import ShardsCollection, ShardsCollectionFactory, ShardsS3IO
from validators.registry import permissions_mapping
from validators.swagger_request_models import (
    CLevelGetReportModel,
    DepartmentGetReportModel,
    ProjectGetReportModel,
)
from validators.utils import validate_kwargs

if TYPE_CHECKING:
    from scheduler import APJobScheduler

TENANT_METRICS_PATH = '{customer}/tenants/{date}/{tenant_dn}.json'
COMMAND_NAME = 'SEND_MAIL'
ED_API_ENDPOINT = '/reports/event_driven'

EVENT_DRIVEN_TYPE = {'maestro': 'CUSTODIAN_EVENT_DRIVEN_RESOURCES_REPORT',
                     'custodian': 'EVENT_DRIVEN'}
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


class ReportGenerator(EventProcessorLambdaHandler):
    handlers = (
        OperationalHandler,
        DiagnosticHandler,
        RetryHandler
    )
    processors = (
        ExpandEnvironmentEventProcessor.build(),
        ApiGatewayEventProcessor(permissions_mapping),
        RestrictCustomerEventProcessor.build(),
        CheckPermissionEventProcessor.build(),
        RestrictTenantEventProcessor.build()
    )

    def __init__(self, s3_service: S3Client,
                 environment_service: EnvironmentService,
                 settings_service: SettingsService,
                 modular_client: Modular,
                 tenant_metrics_service: TenantMetricsService,
                 customer_metrics_service: CustomerMetricsService,
                 license_service: LicenseService,
                 rabbitmq_service: RabbitMQService,
                 batch_results_service: BatchResultsService,
                 mappings_collector: LazyLoadedMappingsCollector,
                 report_statistics_service: ReportStatisticsService,
                 ruleset_service: RulesetService):
        self.s3_service = s3_service
        self.environment_service = environment_service
        self.settings_service = settings_service
        self.modular_client = modular_client
        self.tenant_metrics_service = tenant_metrics_service
        self.customer_metrics_service = customer_metrics_service
        self.license_service = license_service
        self.batch_results_service = batch_results_service
        self.rabbitmq_service = rabbitmq_service
        self.report_statistics_service = report_statistics_service
        self.mappings_collector = mappings_collector
        self.ruleset_service = ruleset_service

        self.customer_license_mapping = {}
        self.customer_tenant_mapping = {}
        self.current_month = datetime.today().replace(day=1).date()

    def lambda_handler(self, event: dict, context: RequestContext):
        """
        Overriding lambda handler for this specific lambda because it
        requires some additional exceptions handling logic
        :param event:
        :param context:
        :return:
        """
        _LOG.info(f'Starting request: {context.aws_request_id}')
        # This is the only place where we print the event. Do not print it
        # somewhere else
        _LOG.debug('Incoming event')
        _LOG.debug(json.dumps(hide_secret_values(event)))

        try:
            processed, context = self._process_event(event, context)
            return self.handle_request(event=processed, context=context)
        except ModularException as e:
            _LOG.warning('Modular exception occurred', exc_info=True)
            return ResponseFactory(int(e.code)).message(e.content).build()
        except ReportNotSendException as e:
            _LOG.warning('Send report error occurred. '
                         'Re-raising it so that step function could catch',
                         exc_info=True)
            # ReportNotSendException won't be raised in _process_event.
            # Can cast, can be sure it'll exist
            raise e
        except TimeoutError as e:
            _LOG.warning('Timeout error occurred. Probably retry')
            raise e
        except CustodianException as e:
            _LOG.warning(f'Application exception occurred: {e}')
            return e.build()
        except Exception:  # noqa
            _LOG.exception('Unexpected exception occurred')
            return ResponseFactory(
                HTTPStatus.INTERNAL_SERVER_ERROR
            ).default().build()

    @classmethod
    def build(cls) -> 'ReportGenerator':
        return cls(
            environment_service=SERVICE_PROVIDER.environment_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            s3_service=SERVICE_PROVIDER.s3,
            modular_client=SERVICE_PROVIDER.modular_client,
            tenant_metrics_service=SERVICE_PROVIDER.tenant_metrics_service,
            customer_metrics_service=SERVICE_PROVIDER.customer_metrics_service,
            license_service=SERVICE_PROVIDER.license_service,
            batch_results_service=SERVICE_PROVIDER.batch_results_service,
            rabbitmq_service=SERVICE_PROVIDER.rabbitmq_service,
            mappings_collector=SERVICE_PROVIDER.mappings_collector,
            report_statistics_service=SERVICE_PROVIDER.report_statistics_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        data = {
            CustodianEndpoint.REPORTS_PROJECT: {
                HTTPMethod.POST: self.generate_project_reports
            },
            CustodianEndpoint.REPORTS_DEPARTMENT: {
                HTTPMethod.POST: self.generate_department_reports
            },
            CustodianEndpoint.REPORTS_CLEVEL: {
                HTTPMethod.POST: self.generate_c_level_reports
            },
            CustodianEndpoint.REPORTS_EVENT_DRIVEN: {
                HTTPMethod.GET: self.generate_event_driven_reports
            },
        }
        for handler in self.handlers:
            data.update(handler.build().mapping)
        return data

    def handle_request(self, event: ProcessedEvent, context: RequestContext):
        func = self.mapping.get(event['resource'], {}).get(event['method'])
        if not func:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        stat_item = self.report_statistics_service.create_from_processed_event(
            event=event
        )
        if not self.settings_service.get_send_reports():
            _LOG.debug('Saving report item with PENDING status because '
                       'sending reports is disabled')
            stat_item.status = ReportDispatchStatus.PENDING.value
            self.report_statistics_service.save(stat_item)
            return build_response(
                code=HTTPStatus.OK,
                content='The ability to send reports is disabled. Your '
                        'request has been queued and will be sent after a '
                        'while.'
            )

        match event['method']:
            case HTTPMethod.GET:
                body = event['query']
            case _:
                body = event['body']
        params = dict(event=body, context=context, **event['path_params'])
        parameters = inspect.signature(func).parameters
        if '_pe' in parameters:
            # pe - Processed Event: in case we need to access some raw data
            # inside a handler.
            _LOG.debug('Expanding handler payload with raw event')
            params['_pe'] = event
        if '_tap' in parameters:
            # _tap - in case we need to know what tenants are allowed
            # inside a specific handler
            _LOG.debug('Expanding handler payload with tenant access data')
            params['_tap'] = event['tenant_access_payload']
        # add event['additional_kwargs'] support if needed

        try:
            response = func(**params)
            stat_item.status = ReportDispatchStatus.SUCCEEDED.value
            self.report_statistics_service.save(stat_item)
            return response
        except (ReportNotSendException, CustodianException) as e:
            self.report_statistics_service.create_failed(
                event=event,
                exception=e
            )
            raise e
        except Exception as e:
            stat_item.status = ReportDispatchStatus.FAILED.value
            stat_item.reason = str(e)
            self.report_statistics_service.save(stat_item)
            raise e

    def generate_event_driven_reports(self, event, context):
        licenses = self.license_service.get_event_driven_licenses()
        for l in licenses:
            _LOG.debug(f'Processing license {l.license_key}')
            quota = l.event_driven.get('quota')
            if not quota:
                _LOG.warning(
                    f'There is no quota for ED notifications in license '
                    f'\'{l.license_key}\'. Cannot send emails')
                continue
            start_date, end_date = self._get_period(quota)
            if datetime.utcnow().timestamp() < end_date.timestamp() or (
                    l.event_driven.get('last_execution') and
                    start_date.isoformat() <= l.event_driven.get('last_execution') <= end_date.isoformat()):
                _LOG.debug(f'Skipping ED report for license {l.license_key}: '
                           f'timestamp now: {datetime.now().isoformat()}; '
                           f'expected end timestamp: {end_date.isoformat()}')
                continue
            _LOG.debug(f'Start timestamp: {start_date.isoformat()}; '
                       f'end timestamp {end_date.isoformat()}')
            for customer, data in l.customers.items():
                _LOG.debug(f'Processing customer {customer}')
                tenants = data.get('tenants')
                if customer not in self.customer_license_mapping:
                    tenants = self.customer_license_mapping.setdefault(
                        customer, {}).setdefault('tenants', []) + tenants
                    self.customer_license_mapping[customer]['tenants'] = list(
                        set(tenants))
                    self.customer_license_mapping[customer][
                        START_DATE] = start_date.replace(
                        tzinfo=None).isoformat()  # SHARDS TODO why?
                    self.customer_license_mapping[customer][
                        END_DATE] = end_date.replace(tzinfo=None).isoformat()
                    self.customer_license_mapping[customer]['license'] = l

        if not self.customer_license_mapping or not any(
                self.customer_license_mapping.values()):
            return build_response(
                f'There are no any active event-driven licenses'
            )

        for customer, info in self.customer_license_mapping.items():
            self.customer_tenant_mapping[customer] = {}
            results = self.batch_results_service.get_by_customer_name(
                customer_name=customer,
                start=utc_datetime(info[START_DATE]),
                end=utc_datetime(info[END_DATE]),
                limit=100,
                filter_condition=BatchResults.tenant_name.is_in(*info['tenants'])
            )
            for item in results:
                self.customer_tenant_mapping[customer].setdefault(
                    item.tenant_name, []).append(item)
        if not self.customer_tenant_mapping or not any(
                self.customer_tenant_mapping.values()):
            # probably no need
            return build_response(
                f'There are no event-driven jobs for period '
                f'{start_date.isoformat()} to {end_date.isoformat()}')

        for customer, tenants in self.customer_tenant_mapping.items():
            rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(customer)
            if not rabbitmq:
                continue
            bucket_name = self.environment_service.default_reports_bucket_name()
            for tenant_name, results in tenants.items():
                _LOG.debug(f'Processing {len(results)} ED scan(s) of '
                           f'{tenant_name} tenant')
                tenant_item = self.modular_client.tenant_service().get(tenant_name)

                collections = []
                with ThreadPoolExecutor() as ex:
                    futures = [ ex.submit(self._fetch_difference, tenant_item, bucket_name, result) for result in results ]
                    for future in as_completed(futures):
                        collections.append(future.result())
                latest = ShardsCollectionFactory.from_tenant(tenant_item)
                latest.io = ShardsS3IO(
                    bucket=bucket_name,
                    key=TenantReportsBucketKeysBuilder(tenant_item).latest_key(),
                    client=self.s3_service
                )
                latest.fetch_meta()
                new_resources = self.merge_collections(
                    collections, latest.meta
                )
                if not new_resources:
                    _LOG.warning(
                        f'No new resources for tenant {tenant_name}')
                    continue
                mitre_data = self._retrieve_mitre_data(new_resources)

                data = {
                    'customer': customer,
                    'tenant_name': tenant_name,
                    'cloud': tenant_item.cloud,
                    'id': tenant_item.project,
                    'activated_regions': list(get_tenant_regions(tenant_item)),
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
                json_model = self.rabbitmq_service.build_m3_json_model(
                    EVENT_DRIVEN_TYPE['maestro'],
                    {**data,
                     'report_type': EVENT_DRIVEN_TYPE['custodian']})
                self.rabbitmq_service.send_notification_to_m3(
                    COMMAND_NAME, json_model, rabbitmq)
                _LOG.debug(f'Notification for {tenant_name} tenant was '
                           f'successfully send')

            customer_license = self.customer_license_mapping[customer]
            _LOG.debug(
                f'Updating last report execution date for license '
                f'{customer_license["license"].license_key}')
            customer_license['license'].event_driven['last_execution'] = customer_license[END_DATE]

        return build_response(
            content='Reports sending was successfully triggered'
        )

    @staticmethod
    def get_report_date() -> datetime:
        now = utc_datetime()
        end = now + relativedelta(hour=0, minute=0, second=0, microsecond=0, weekday=SU(+1))
        return end

    @validate_kwargs
    def generate_project_reports(self, event: ProjectGetReportModel,
                                 context: RequestContext):
        date = self.get_report_date()

        metrics_bucket = self.environment_service.get_metrics_bucket_name()
        tenant_display_names = event.tenant_display_names
        report_types = event.types
        receivers = event.receivers

        json_model = []
        errors = []
        for display_name in tenant_display_names:
            _LOG.debug(
                f'Retrieving tenants with display name \'{display_name}\'')
            ts = self.modular_client.tenant_service()
            tenants = list(ts.i_get_by_dntl(display_name.lower()))
            if not tenants:
                _msg = \
                    f'Cannot find tenants with display name \'{display_name}\''
                _LOG.error(_msg)
                errors.append(_msg)
                continue

            tenant = list(tenants)[0]
            tenant_group_metrics = self.s3_service.gz_get_json(
                bucket=metrics_bucket,
                key=MetricsBucketKeysBuilder(tenant).tenant_metrics(date)
            )
            if not tenant_group_metrics:
                _msg = f'There is no data for \'{display_name}\' tenant ' \
                       f'group for the last week'
                _LOG.warning(_msg)
                errors.append(_msg)
                continue

            tenant_group_metrics[
                OUTDATED_TENANTS] = self._process_outdated_tenants(
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
                json_model.append(self.rabbitmq_service.build_m3_json_model(
                    _type['maestro'], {'receivers': list(receivers),
                                       **tenant_group_metrics, DATA_ATTR: data,
                                       'report_type': _type['custodian']}))

        if not json_model:
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=';\n'.join(errors)
            )
        rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(
            event.customer_id
        )
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        self.rabbitmq_service.send_notification_to_m3(
            COMMAND_NAME, json_model, rabbitmq)
        return build_response(
            content=f'The request to send reports for {tenant_display_names} '
                    f'tenant group were successfully created'
        )

    @validate_kwargs
    def generate_department_reports(self, event: DepartmentGetReportModel,
                                    context: RequestContext):
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
        customer = event.customer_id
        report_types = event.types

        top_tenants = self.tenant_metrics_service.list_by_date_and_customer(
            date=self.current_month.isoformat(), customer=customer)
        if len(top_tenants) == 0:
            return build_response(f'There are no metrics for customer '
                                  f'{customer} for the period from '
                                  f'{previous_month} to '
                                  f'{self.current_month.isoformat()}')
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
        rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(event.customer_id)
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        for _type, values in report_type_mapping.items():
            if report_types and report_type_mapping[_type]['report_type']['custodian'] not in report_types:
                continue
            container = values['container']
            if (isinstance(container, list) and not container) or (
                    isinstance(container, dict) and not any(container.values())):
                _LOG.warning(f'No data for report type {_type}')
                continue
            json_model = self.rabbitmq_service.build_m3_json_model(
                report_type_mapping[_type]['report_type']['maestro'],
                {CUSTOMER_ATTR: customer, 'from': previous_month.isoformat(),
                 'to': self.current_month.isoformat(),
                 OUTDATED_TENANTS: values[OUTDATED_TENANTS],
                 'report_type': _type, DATA_ATTR: values['container']})
            self.rabbitmq_service.send_notification_to_m3(
                COMMAND_NAME, json_model, rabbitmq)
            _LOG.debug(f'Notifications for {customer} customer have been '
                       f'sent successfully')
        return build_response(
            content=f'Reports sending for {customer} customer have been '
                    f'triggered successfully'
        )

    @validate_kwargs
    def generate_c_level_reports(self, event: CLevelGetReportModel,
                                 context: RequestContext):
        report_type_mapping = {
            OVERVIEW_TYPE.upper(): CUSTOMER_OVERVIEW_REPORT_TYPE,
            COMPLIANCE_TYPE.upper(): CUSTOMER_COMPLIANCE_REPORT_TYPE,
            ATTACK_VECTOR_TYPE.upper(): CUSTOMER_ATTACKS_REPORT_TYPE
        }
        previous_month = (self.current_month - timedelta(days=1)).replace(
            day=1)

        customer = event.customer_id
        report_types = event.types

        customer_metrics = self.customer_metrics_service.list_by_date_and_customer(
            date=self.current_month.isoformat(), customer=customer)
        _LOG.debug(f'Retrieved {len(customer_metrics)} items for customer '
                   f'{customer}')
        if len(customer_metrics) == 0:
            return build_response(f'There are no metrics for customer '
                                  f'{customer} for the period from '
                                  f'{previous_month} to '
                                  f'{self.current_month.isoformat()}')
        rabbitmq = self.rabbitmq_service.get_customer_rabbitmq(customer)
        if not rabbitmq:
            return self.rabbitmq_service.no_rabbit_configuration()
        for item in customer_metrics:
            item = item.attribute_values
            if report_types and item.get('type') not in report_types:
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
                applications = list(self.modular_client.application_service().list(
                    customer=customer,
                    _type=ApplicationType.CUSTODIAN_LICENSES.value,
                    deleted=False
                ))
                self._get_application_info(applications, attribute_diff)
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

            model = {
                DATA_ATTR: attribute_diff,
                CUSTOMER_ATTR: item.get(CUSTOMER_ATTR),
                'from': previous_month.isoformat(),
                'to': self.current_month.isoformat(),
                OUTDATED_TENANTS: item.get(OUTDATED_TENANTS, [])
            }

            model.update({'report_type': item.get('type')})
            _LOG.debug('Sending clevel model to m3')
            _LOG.debug(json.dumps(model))
            json_model = self.rabbitmq_service.build_m3_json_model(
                report_type_mapping.get(item.get('type')), model)
            self.rabbitmq_service.send_notification_to_m3(
                COMMAND_NAME, json_model, rabbitmq)

        _LOG.debug(f'Reports sending for {customer} customer have been '
                   f'triggered successfully')
        return build_response(
            content=f'Reports sending for {customer} customer have been '
                    f'triggered successfully'
        )

    @staticmethod
    def _get_license_info(_license: License) -> dict:
        balance = _license.allowance.get('job_balance')
        time_range = _license.allowance.get('time_range')
        scan_frequency = f'{balance} scan{"" if balance == 1 else "s"} per ' \
                         f'{time_range}'
        expiration = None
        if exp := _license.expiration:
            # the returned object is displayed directly, so we make
            # human-formatting here
            expiration = exp.strftime('%b %d, %Y %H:%M:%S %Z')
        return {
            'activated': True,
            'license_properties': {
                'Scans frequency': scan_frequency,
                'Expiration': expiration,
                'Event-Driven mode': 'On' if _license.event_driven else 'Off'
            }
        }

    def _fetch_difference(self, tenant: Tenant, bucket_name: str,
                          result: BatchResults) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=bucket_name,
            key=TenantReportsBucketKeysBuilder(tenant).ed_job_difference(result),
            client=self.s3_service,  # it's a client
        )
        collection.fetch_all()
        return collection

    def merge_collections(self, collections: list[ShardsCollection],
                          meta: dict) -> list[dict]:
        content = {}
        human_data = self.mappings_collector.human_data
        severity = self.mappings_collector.severity
        service = self.mappings_collector.service
        for collection in collections:
            for _, shard in collection:
                for part in shard:
                    rf = human_data.get(part.policy, {}).get(
                        'report_fields') or set()
                    data = content.setdefault(part.policy, {
                        'severity': severity.get(part.policy) or 'Unknown',
                        'description': meta.get(part.policy).get('description'),
                        'resource_type': service.get(part.policy),
                        'regions_data': {}
                    })
                    data['regions_data'].setdefault(part.location, []).extend(
                        filter_dict(r, rf) for r in part.resources
                    )
        return [{'policy': k, **v} for k, v in content.items()]

    @staticmethod
    def _get_period(frequency: int, last_execution: str = None) -> \
            tuple[datetime, datetime]:
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

    def _retrieve_mitre_data(self, resources: list) -> list[dict]:
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

    def _get_application_info(self, applications: list[Application],
                              attribute_diff: dict):
        """
        Previously we could have multiple licenses inside one application
        (split by cloud). That division was just verbal because nothing was
        preventing us from creating a license that has rulesets for multiple
        clouds. Although, we did create only cloud-specific licenses.
        Currently, one application is one license, and we have no
        straightforward way of knowing the cloud of that license. But I
        don't want to change the format of report or whatever this data
        is going to. So I just use this workaround. Basically the same,
        even better that it was
        :param applications:
        :param attribute_diff:
        :return:
        """
        rulesets = {}  # cache
        for ap in applications:
            lic = License(ap)
            # here is the faulty thing, but we are not supposed to create
            # licenses that contain rulesets of different clouds
            ruleset_id = next(iter(lic.ruleset_ids), None)
            if not ruleset_id:
                continue
            if ruleset_id not in rulesets:
                rulesets[ruleset_id] = self.ruleset_service.by_lm_id(ruleset_id)
            ruleset = rulesets[ruleset_id]
            if not ruleset:
                continue
            if ruleset.cloud == RuleDomain.KUBERNETES:
                # skip license because c-level report currently does not
                # support k8s
                continue
            cloud = ruleset.cloud.lower()
            if cloud == 'gcp':
                cloud = 'google'  # need more kludges...
            if attribute_diff.setdefault(cloud, {}).get('activated') is True:
                continue
            attribute_diff[cloud].update(self._get_license_info(lic))

    @staticmethod
    def _get_attr_values(item, default: dict = None):
        default = default or {}
        return item.attribute_values if item else default

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

    @staticmethod
    def _process_outdated_tenants(outdated_tenants: dict):
        tenants = []
        for cloud, data in outdated_tenants.items():
            tenants.extend(data.keys())
        return tenants


class ReportGeneratorNoProcessors(ReportGenerator):
    """
    Only for retry and event-driven
    """
    processors = (
        ExpandEnvironmentEventProcessor.build(),  # does not change event so can be used
    )

    def handle_request(self, event: dict, context: RequestContext):
        resource = event.get('requestContext', {}).get('resourcePath')
        if resource == CustodianEndpoint.REPORTS_EVENT_DRIVEN:
            _LOG.info('Executing event-driven handler')
            return self.generate_event_driven_reports(event, context)
        if resource == CustodianEndpoint.REPORTS_RETRY:
            _LOG.info('Executing retry handler')
            return self.mapping[CustodianEndpoint.REPORTS_RETRY][HTTPMethod.POST](event, context)
        raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()


HANDLER = ReportGenerator.build()
HANDLER_NO_PROCESSORS = ReportGeneratorNoProcessors.build()


def lambda_handler(event, context):
    resource = event.get('requestContext', {}).get('resourcePath')
    if resource in (CustodianEndpoint.REPORTS_RETRY.value,
                    CustodianEndpoint.REPORTS_EVENT_DRIVEN.value):
        _LOG.debug('Retry or event driven request came. '
                   'Using handler with no processors')
        return HANDLER_NO_PROCESSORS.lambda_handler(event, context)

    return HANDLER.lambda_handler(event=event, context=context)
