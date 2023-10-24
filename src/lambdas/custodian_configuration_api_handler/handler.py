from http import HTTPStatus
from typing import List, Union

from handlers.abstracts.abstract_handler import AbstractHandler
from handlers.applications_handler import ApplicationsHandler
from handlers.credentials_manager_handler import CredentialsManagerHandler
from handlers.customer_handler import instantiate_customer_handler
from handlers.findings_handler import FindingsHandler
from handlers.license_handler import LicenseHandler
from handlers.license_manager_setting_handler import \
    LicenseManagerConfigHandler, LicenseManagerClientHandler
from handlers.mail_setting_handler import MailSettingHandler
from handlers.parents_handler import ParentsHandler
from handlers.platforms_handler import PlatformsHandler
from handlers.policy_handler import PolicyHandler
from handlers.rabbitmq_handler import RabbitMQHandler
from handlers.role_handler import RoleHandler
from handlers.rule_handler import RuleHandler
from handlers.rule_meta_handler import RuleMetaHandler
from handlers.rule_source_handler import RuleSourceHandler
from handlers.ruleset_handler import RulesetHandler
from handlers.tenants import instantiate_tenants_handler
from handlers.user_customer_handler import UserCustomerHandler
from handlers.user_role_handler import UserRoleHandler
from handlers.user_tenants_handler import UserTenantsHandler
from helpers import build_response
from helpers.constants import HTTPMethod, ACTION_PARAM_ERROR, \
    HTTP_METHOD_ERROR, CUSTOMER_ATTR, PARAM_REQUEST_PATH, PARAM_HTTP_METHOD, \
    RULE_SOURCE_ID_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from services import SERVICE_PROVIDER
from services.abstract_api_handler_lambda import AbstractApiHandlerLambda
from services.clients.lambda_func import LambdaClient
from services.clients.smtp import SMTPClient
from services.rule_source_service import RuleSourceService

RULE_META_UPDATER_LAMBDA_NAME = 'caas-rule-meta-updater'
CONFIGURATION_BACKUPPER_LAMBDA_NAME = 'caas-configuration-backupper'
CONFIGURATION_METRICS_UPDATER_NAME = 'caas-metrics-updater'

_LOG = get_logger('custodian-configuration-api-handler')


class ConfigurationApiHandler(AbstractApiHandlerLambda):

    def __init__(self, handlers: List[AbstractHandler],
                 lambda_client: LambdaClient,
                 rule_source_service: RuleSourceService):
        self.REQUEST_PATH_HANDLER_MAPPING = {
            '/rules/update-meta': {
                HTTPMethod.POST: self.invoke_rule_meta_updater
            },
            '/backup': {
                HTTPMethod.POST: self.backup_configuration
            },
            '/metrics/update': {
                HTTPMethod.POST: self.update_metrics
            },
        }
        for handler in handlers:
            self.REQUEST_PATH_HANDLER_MAPPING.update(
                handler.define_action_mapping())
        self.lambda_client = lambda_client
        self.rule_source_service = rule_source_service

    def handle_request(self, event, context):
        request_path = event[PARAM_REQUEST_PATH]
        method_name = event[PARAM_HTTP_METHOD]
        action_mapping = self.REQUEST_PATH_HANDLER_MAPPING.get(
            request_path)
        if not action_mapping:
            _LOG.warning(
                ACTION_PARAM_ERROR.format(endpoint=request_path))
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content='Service is temporarily unavailable')
        handler_func = action_mapping.get(method_name)
        if not handler_func:
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=HTTP_METHOD_ERROR.format(method=method_name,
                                                 resource=request_path))
        return handler_func(event=event)

    def invoke_rule_meta_updater(self, event):
        _LOG.debug(f'Invoke meta updater event: {event}')

        customer = event.get(CUSTOMER_ATTR) or SYSTEM_CUSTOMER

        rs_service = rule_source_service

        rule_source_id = event.get(RULE_SOURCE_ID_ATTR)
        rule_sources = []
        if rule_source_id:
            rule_source = rule_source_service.get(
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

        rule_sources = rs_service.filter_by_tenants(entities=rule_sources)

        ids_to_sync = []
        responses: Union[str, List] = []
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

    def update_metrics(self, event):
        _LOG.debug(f'Invoke metrics updater event: {event}')
        response = self.lambda_client.invoke_function_async(
            CONFIGURATION_METRICS_UPDATER_NAME, event={'data_type': 'tenants'})
        if response.get('StatusCode') == HTTPStatus.ACCEPTED:
            _LOG.debug(
                f'{CONFIGURATION_BACKUPPER_LAMBDA_NAME} has been triggered')
            return build_response(
                code=HTTPStatus.OK,
                content='Metrics update has been submitted'
            )
        _LOG.error(
            f'Response code is not {HTTPStatus.ACCEPTED}. '
            f'Response: {response}.\n{CONFIGURATION_BACKUPPER_LAMBDA_NAME} '
            f'has not been triggered')
        return build_response(
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content='Internal Server Error')

    def backup_configuration(self, event):
        _LOG.debug(f'Invoke backupper event: {event}')
        response = self.lambda_client.invoke_function_async(
            CONFIGURATION_BACKUPPER_LAMBDA_NAME)
        if response.get('StatusCode') == HTTPStatus.ACCEPTED:
            _LOG.debug(
                f'{CONFIGURATION_BACKUPPER_LAMBDA_NAME} has been triggered')
            return build_response(
                code=HTTPStatus.OK,
                content=f'Backup event has been submitted'
            )
        _LOG.error(
            f'Response code is not {HTTPStatus.ACCEPTED}. '
            f'Response: {response}.\n{CONFIGURATION_BACKUPPER_LAMBDA_NAME} '
            f'has not been triggered')
        return build_response(
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content='Internal Server Error')


modular_service = SERVICE_PROVIDER.modular_service()
rule_service = SERVICE_PROVIDER.rule_service()
cached_iam_service = SERVICE_PROVIDER.iam_cache_service()
access_control_service = SERVICE_PROVIDER.access_control_service()
user_service = SERVICE_PROVIDER.user_service()
ssm_service = SERVICE_PROVIDER.ssm_service()
settings_service = SERVICE_PROVIDER.settings_service()
credential_manager_service = SERVICE_PROVIDER.credential_manager_service()
ruleset_service = SERVICE_PROVIDER.ruleset_service()
rule_source_service = SERVICE_PROVIDER.rule_source_service()
license_service = SERVICE_PROVIDER.license_service()
license_manager_service = SERVICE_PROVIDER.license_manager_service()
lambda_client = SERVICE_PROVIDER.lambda_func()
findings_service = SERVICE_PROVIDER.findings_service()
key_management_service = SERVICE_PROVIDER.key_management_service()

customer_handler = instantiate_customer_handler(
    modular_service=modular_service, user_service=user_service
)
tenant_handler = instantiate_tenants_handler(
    modular_service=modular_service,
    environment_service=SERVICE_PROVIDER.environment_service()
)
ruleset_handler = RulesetHandler.build()
rule_source_handler = RuleSourceHandler(
    rule_source_service=rule_source_service,
    modular_service=modular_service,
    rule_service=rule_service,
    restriction_service=SERVICE_PROVIDER.restriction_service()
)
credentials_manager_handler = CredentialsManagerHandler(
    credential_manager_service=credential_manager_service,
    user_service=user_service,
    modular_service=modular_service
)
role_handler = RoleHandler(
    cached_iam_service=cached_iam_service
)
policy_handler = PolicyHandler(
    cached_iam_service=cached_iam_service,
    access_control_service=access_control_service
)

rule_service = RuleHandler(
    rule_service=rule_service,
    modular_service=modular_service,
    rule_source_service=rule_source_service
)

user_customer_handler = UserCustomerHandler(
    modular_service=modular_service,
    user_service=user_service
)
user_role_handler = UserRoleHandler(
    user_service=user_service,
    cached_iam_service=cached_iam_service
)
user_tenants_handler = UserTenantsHandler(
    modular_service=modular_service,
    user_service=user_service
)
license_handler = LicenseHandler(
    self_service=license_service,
    ruleset_service=ruleset_service,
    lambda_client=lambda_client,
    license_manager_service=license_manager_service,
    modular_service=modular_service
)
findings_handler = FindingsHandler(
    service=findings_service,
    modular_service=modular_service
)
mail_setting_handler = MailSettingHandler(
    settings_service=settings_service,
    smtp_client=SMTPClient(),
    ssm_client=SERVICE_PROVIDER.ssm()
)
lm_config_handler = LicenseManagerConfigHandler(
    settings_service=settings_service
)
lm_client_handler = LicenseManagerClientHandler(
    settings_service=settings_service,
    license_manager_service=license_manager_service,
    key_management_service=key_management_service
)
rule_meta_handler = RuleMetaHandler.build()
application_handler = ApplicationsHandler.build()
parent_handler = ParentsHandler.build()
rabbitmq_handler = RabbitMQHandler.build()
platforms_handler = PlatformsHandler.build()

API_HANDLER = ConfigurationApiHandler(
    handlers=[customer_handler, tenant_handler,
              role_handler, policy_handler, rule_service,
              user_customer_handler, user_role_handler,
              credentials_manager_handler,
              ruleset_handler, rule_source_handler, license_handler,
              findings_handler, user_tenants_handler, mail_setting_handler,
              lm_config_handler, lm_client_handler, application_handler,
              parent_handler, rabbitmq_handler, rule_meta_handler,
              platforms_handler],
    lambda_client=SERVICE_PROVIDER.lambda_func(),
    rule_source_service=SERVICE_PROVIDER.rule_source_service())


def lambda_handler(event, context):
    return API_HANDLER.lambda_handler(event=event, context=context)
