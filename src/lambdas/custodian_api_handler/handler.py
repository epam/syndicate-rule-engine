from functools import cached_property

from typing_extensions import Self

from handlers import Mapping
from helpers import RequestContext
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handlers.batch_result_handler import (
    BatchResultsHandler,
)
from lambdas.custodian_api_handler.handlers.events_handler import EventsHandler
from lambdas.custodian_api_handler.handlers.health_check_handler import (
    HealthCheckHandler,
)
from lambdas.custodian_api_handler.handlers.job_handler import JobHandler
from lambdas.custodian_api_handler.handlers.metrics_status_handler import (
    MetricsStatusHandler,
)
from lambdas.custodian_api_handler.handlers.new_swagger_handler import \
    SwaggerHandler
from lambdas.custodian_api_handler.handlers.users_handler import UsersHandler
from services.abs_lambda import (
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    RestrictCustomerEventProcessor,
    ExpandEnvironmentEventProcessor,
    ApiEventProcessorLambdaHandler,
    RestrictTenantEventProcessor
)
from validators.registry import permissions_mapping

_LOG = get_logger(__name__)


class ApiHandler(ApiEventProcessorLambdaHandler):
    processors = (
        ExpandEnvironmentEventProcessor.build(),
        ApiGatewayEventProcessor(permissions_mapping),
        RestrictCustomerEventProcessor.build(),
        CheckPermissionEventProcessor.build(),
        RestrictTenantEventProcessor.build()
    )

    def __init__(self):
        self.additional_handlers = [
            BatchResultsHandler,
            UsersHandler,
            JobHandler,
            HealthCheckHandler,
            MetricsStatusHandler,
            EventsHandler,
            SwaggerHandler
        ]

    @classmethod
    def build(cls) -> Self:
        return cls()

    @cached_property
    def mapping(self) -> Mapping:
        res = {}
        for handler in self.additional_handlers:
            res.update(
                handler.build().mapping
            )
        return res


API_HANDLER = ApiHandler.build()


def lambda_handler(event: dict, context: RequestContext):
    return API_HANDLER.lambda_handler(event=event, context=context)
