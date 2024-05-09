from functools import cached_property
from http import HTTPStatus

from handlers.credentials_handler import CredentialsHandler
from handlers.customer_handler import CustomerHandler
from handlers.defect_dojo_handler import DefectDojoHandler
from handlers.license_handler import LicenseHandler
from handlers.license_manager_setting_handler import \
    LicenseManagerConfigHandler, LicenseManagerClientHandler
from handlers.mail_setting_handler import MailSettingHandler
from handlers.platforms_handler import PlatformsHandler
from handlers.policy_handler import PolicyHandler
from handlers.rabbitmq_handler import RabbitMQHandler
from handlers.report_status_handler import ReportStatusHandlerHandler
from handlers.role_handler import RoleHandler
from handlers.rule_handler import RuleHandler
from handlers.rule_meta_handler import RuleMetaHandler
from handlers.rule_source_handler import RuleSourceHandler
from handlers.ruleset_handler import RulesetHandler
from handlers.self_integration_handler import SelfIntegrationHandler
from handlers.send_report_setting_handler import \
    ReportsSendingSettingHandler
from handlers.tenant_handler import TenantHandler
from helpers.constants import HTTPMethod, CustodianEndpoint
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SERVICE_PROVIDER
from services.abs_lambda import (
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    RestrictCustomerEventProcessor,
    ExpandEnvironmentEventProcessor,
    ApiEventProcessorLambdaHandler,
    RestrictTenantEventProcessor
)
from services.clients.lambda_func import LambdaClient
from services.rule_source_service import RuleSourceService
from validators.registry import permissions_mapping
from validators.swagger_request_models import RuleUpdateMetaPostModel, \
    BaseModel
from validators.utils import validate_kwargs

RULE_META_UPDATER_LAMBDA_NAME = 'caas-rule-meta-updater'
CONFIGURATION_BACKUPPER_LAMBDA_NAME = 'caas-configuration-backupper'
CONFIGURATION_METRICS_UPDATER_NAME = 'caas-metrics-updater'

_LOG = get_logger('custodian-configuration-api-handler')


class ConfigurationApiHandler(ApiEventProcessorLambdaHandler):
    processors = (
        ExpandEnvironmentEventProcessor.build(),
        ApiGatewayEventProcessor(permissions_mapping),
        RestrictCustomerEventProcessor.build(),
        CheckPermissionEventProcessor.build(),
        RestrictTenantEventProcessor.build()
    )
    handlers = (
        PolicyHandler,
        RoleHandler,
        RuleHandler,
        RulesetHandler,
        RuleMetaHandler,
        PlatformsHandler,
        MailSettingHandler,
        CustomerHandler,
        RabbitMQHandler,
        DefectDojoHandler,
        ReportsSendingSettingHandler,
        TenantHandler,
        LicenseHandler,
        ReportStatusHandlerHandler,
        SelfIntegrationHandler,
        LicenseManagerClientHandler,
        LicenseManagerConfigHandler,
        RuleSourceHandler,
        CredentialsHandler
    )

    def __init__(self, lambda_client: LambdaClient,
                 rule_source_service: RuleSourceService):
        self.lambda_client = lambda_client
        self.rule_source_service = rule_source_service

    @cached_property
    def mapping(self):
        res = {
            CustodianEndpoint.RULE_META_UPDATER: {
                HTTPMethod.POST: self.invoke_rule_meta_updater
            },
            CustodianEndpoint.METRICS_UPDATE: {
                HTTPMethod.POST: self.update_metrics
            },
        }
        for h in self.handlers:
            res.update(h.build().mapping)
        return res

    @validate_kwargs
    def invoke_rule_meta_updater(self, event: RuleUpdateMetaPostModel):

        customer = event.customer or SYSTEM_CUSTOMER

        rs_service = self.rule_source_service

        rule_source_id = event.rule_source_id
        rule_sources = []
        if rule_source_id:
            rule_source = rs_service.get(
                rule_source_id=rule_source_id)
            if not rule_source or customer != SYSTEM_CUSTOMER and rule_source.customer != customer:
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=f'The requested Rule Source \'{rule_source_id}\' '
                            f'is not found'
                )
            rule_sources.append(rule_source)
        else:
            rule_sources = rs_service.list_rule_sources(customer=customer)

        ids_to_sync = []
        responses: str | list = []
        for rule_source in rule_sources:
            log_head = f'RuleSource:{rule_source.id!r} of ' \
                       f'{rule_source.customer!r} customer'
            if rs_service.is_allowed_to_sync(rule_source):
                _LOG.debug(f'{log_head} is allowed to update')
                ids_to_sync.append(rule_source.id)
                response = rs_service.build_update_event_response(
                    rule_source=rule_source
                )
            else:
                _LOG.warning(f'{log_head} is not allowed to update')
                response = rs_service.build_update_event_response(
                    rule_source=rule_source, forbidden=True
                )
            responses.append(response)

        if ids_to_sync:
            self.lambda_client.invoke_function_async(
                RULE_META_UPDATER_LAMBDA_NAME,
                event={'rule_source_ids': ids_to_sync}
            )
            _LOG.debug(f'{RULE_META_UPDATER_LAMBDA_NAME} has been triggered')
        else:
            _LOG.warning(
                f'No rule-sources allowed to update. '
                f'{RULE_META_UPDATER_LAMBDA_NAME} has not been triggered')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='No rule sources were found'
            )

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=responses
        )

    @validate_kwargs
    def update_metrics(self, event: BaseModel):
        response = self.lambda_client.invoke_function_async(
            CONFIGURATION_METRICS_UPDATER_NAME, event={'data_type': 'tenants'})
        if response.get('StatusCode') == HTTPStatus.ACCEPTED:
            _LOG.debug(
                f'{CONFIGURATION_BACKUPPER_LAMBDA_NAME} has been triggered')
            return build_response(
                code=HTTPStatus.ACCEPTED,
                content='Metrics update has been submitted'
            )
        _LOG.error(
            f'Response code is not {HTTPStatus.ACCEPTED}. '
            f'Response: {response}.\n{CONFIGURATION_BACKUPPER_LAMBDA_NAME} '
            f'has not been triggered')
        return build_response(
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content='Internal Server Error')


API_HANDLER = ConfigurationApiHandler(
    lambda_client=SERVICE_PROVIDER.lambda_client,
    rule_source_service=SERVICE_PROVIDER.rule_source_service
)


def lambda_handler(event, context):
    return API_HANDLER.lambda_handler(event=event, context=context)