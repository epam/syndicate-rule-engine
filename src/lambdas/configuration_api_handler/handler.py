from functools import cached_property
from http import HTTPStatus

from handlers.chronicle_handler import ChronicleHandler
from handlers.credentials_handler import CredentialsHandler
from handlers.customer_handler import CustomerHandler
from handlers.defect_dojo_handler import DefectDojoHandler
from handlers.license_handler import LicenseHandler
from handlers.license_manager_setting_handler import (
    LicenseManagerClientHandler,
    LicenseManagerConfigHandler,
)
from handlers.mail_setting_handler import MailSettingHandler
from handlers.platforms_handler import PlatformsHandler
from handlers.policy_handler import PolicyHandler
from handlers.rabbitmq_handler import RabbitMQHandler
from handlers.reports import ReportStatusHandlerHandler
from handlers.resource_exception_handler import ResourceExceptionHandler
from handlers.resource_handler import ResourceHandler
from handlers.role_handler import RoleHandler
from handlers.rule_handler import RuleHandler
from handlers.rule_source_handler import RuleSourceHandler
from handlers.ruleset_handler import RulesetHandler
from handlers.self_integration_handler import SelfIntegrationHandler
from handlers.send_report_setting_handler import ReportsSendingSettingHandler
from handlers.tenant_handler import TenantHandler
from helpers.constants import (
    CUSTOMER_ATTR,
    GIT_PROJECT_ID_ATTR,
    RULE_SOURCE_ID_ATTR,
    STATUS_ATTR,
    Endpoint,
    HTTPMethod,
    RuleSourceSyncingStatus,
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SystemCustomer
from models.rule_source import RuleSource
from onprem.tasks import collect_metrics, run_update_metadata, sync_rulesource
from services import SERVICE_PROVIDER
from services.abs_lambda import (
    ApiEventProcessorLambdaHandler,
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    ExpandEnvironmentEventProcessor,
    RestrictCustomerEventProcessor,
    RestrictTenantEventProcessor,
)
from services.rule_source_service import RuleSourceService
from validators.registry import permissions_mapping
from validators.swagger_request_models import (
    BaseModel,
    RuleUpdateMetaPostModel,
)
from validators.utils import validate_kwargs


_LOG = get_logger(__name__)


STATUS_MESSAGE_UPDATE_EVENT_SUBMITTED = 'Rule update event has been submitted'
STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN = \
    'Rule source is currently being updated. ' \
    'Rule update event has not been submitted'


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
        CredentialsHandler,
        ChronicleHandler,
        ResourceHandler,
        ResourceExceptionHandler
    )

    def __init__(self, rule_source_service: RuleSourceService):
        self.rule_source_service = rule_source_service

    @cached_property
    def mapping(self):
        res = {
            Endpoint.RULE_META_UPDATER: {
                HTTPMethod.POST: self.invoke_rule_meta_updater
            },
            Endpoint.METRICS_UPDATE: {
                HTTPMethod.POST: self.update_metrics
            },
            Endpoint.METADATA_UPDATE: {
                HTTPMethod.POST: self.update_metadata_handler
            },
        }
        for h in self.handlers:
            res.update(h.build().mapping)
        return res

    @validate_kwargs
    def invoke_rule_meta_updater(self, event: RuleUpdateMetaPostModel):

        customer = event.customer or SystemCustomer.get_name()

        rs_service = self.rule_source_service

        rule_source_id = event.rule_source_id
        if rule_source_id:
            rule_source = rs_service.get_nullable(rule_source_id)
            if not rule_source or customer != SystemCustomer.get_name() and rule_source.customer != customer:
                return build_response(
                    code=HTTPStatus.NOT_FOUND,
                    content=f'The requested Rule Source \'{rule_source_id}\' '
                            f'is not found'
                )
            rule_sources = [rule_source]
        else:
            rule_sources = rs_service.query(customer=customer)

        ids_to_sync = []
        responses = []
        for rule_source in rule_sources:
            log_head = f'RuleSource:{rule_source.id!r} of ' \
                       f'{rule_source.customer!r} customer'
            if rs_service.is_allowed_to_sync(rule_source):
                _LOG.debug(f'{log_head} is allowed to update')
                ids_to_sync.append(rule_source.id)
                response = self.build_update_event_response(
                    rule_source=rule_source
                )
                self.rule_source_service.update_latest_sync(
                    item=rule_source,
                    current_status=RuleSourceSyncingStatus.SYNCING
                )
            else:
                _LOG.warning(f'{log_head} is not allowed to update')
                response = self.build_update_event_response(
                    rule_source=rule_source, forbidden=True
                )
            responses.append(response)

        if ids_to_sync:
            sync_rulesource.delay(ids_to_sync)
        else:
            _LOG.warning('No rule-sources allowed to update')
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
        collect_metrics.delay()
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content='Metrics update has been submitted'
        )

    @validate_kwargs
    def update_metadata_handler(
        self, 
        event: BaseModel,
    ):
        run_update_metadata.delay()
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content='Metadata update has been submitted',
        )

    @staticmethod
    def build_update_event_response(rule_source: RuleSource,
                                    forbidden: bool = False) -> dict:
        message = STATUS_MESSAGE_UPDATE_EVENT_FORBIDDEN if forbidden \
            else STATUS_MESSAGE_UPDATE_EVENT_SUBMITTED
        return {
            RULE_SOURCE_ID_ATTR: rule_source.id,
            CUSTOMER_ATTR: rule_source.customer,
            GIT_PROJECT_ID_ATTR: rule_source.git_project_id,
            STATUS_ATTR: message
        }


API_HANDLER = ConfigurationApiHandler(
    rule_source_service=SERVICE_PROVIDER.rule_source_service
)


def lambda_handler(event, context):
    return API_HANDLER.lambda_handler(event=event, context=context)
