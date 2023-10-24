from http import HTTPStatus
from typing import List

from handlers.abstracts.abstract_handler import AbstractHandler
from handlers.compliance_handler import JobsComplianceHandler, \
    EntityComplianceHandler
from handlers.details_handler import JobsDetailsHandler, EntityDetailsHandler
from handlers.digest_handler import JobsDigestHandler, EntityDigestHandler
from handlers.errors_handler import JobsErrorsHandler, EntityErrorsHandler
from handlers.push_handler import SiemPushHandler
from handlers.resource_report_handler import ResourceReportHandler
from handlers.rules_handler import JobsRulesHandler, EntityRulesHandler
from helpers import build_response
from helpers.constants import ACTION_PARAM_ERROR, HTTP_METHOD_ERROR, \
    PARAM_REQUEST_PATH, PARAM_HTTP_METHOD
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER
from services.abstract_api_handler_lambda import AbstractApiHandlerLambda

_LOG = get_logger('custodian-report-generator')


# TODO merge this lambda with report_generation_handler


class ReportGenerator(AbstractApiHandlerLambda):

    def __init__(self, handlers: List[AbstractHandler]):
        self.REQUEST_PATH_HANDLER_MAPPING = {}
        for handler in handlers:
            self.REQUEST_PATH_HANDLER_MAPPING.update(
                handler.define_action_mapping()
            )

    def handle_request(self, event, context):
        request_path = event[PARAM_REQUEST_PATH]
        method_name = event[PARAM_HTTP_METHOD]
        handler_functions = self.REQUEST_PATH_HANDLER_MAPPING.get(request_path)
        if not handler_functions:
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=ACTION_PARAM_ERROR.format(endpoint=request_path)
            )
        handler_func = handler_functions.get(method_name)
        response = None
        if handler_func:
            response = handler_func(event=event)
        return response or build_response(
            code=HTTPStatus.BAD_REQUEST,
            content=HTTP_METHOD_ERROR.format(
                method=method_name, resource=request_path
            )
        )


HANDLERS: List[AbstractHandler] = [
    Handler(
        ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service(),
        modular_service=SERVICE_PROVIDER.modular_service(),
        report_service=SERVICE_PROVIDER.report_service()
    )
    for Handler in (
        JobsDigestHandler, EntityDigestHandler,
        JobsDetailsHandler, EntityDetailsHandler,
        JobsErrorsHandler, EntityErrorsHandler,
        JobsRulesHandler, EntityRulesHandler
    )
]
HANDLERS.extend(
    [
        EntityComplianceHandler(
            ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service(),
            modular_service=SERVICE_PROVIDER.modular_service(),
            report_service=SERVICE_PROVIDER.report_service(),
            findings_service=SERVICE_PROVIDER.findings_service(),
            coverage_service=SERVICE_PROVIDER.coverage_service(),

        ),
        JobsComplianceHandler(
            ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service(),
            modular_service=SERVICE_PROVIDER.modular_service(),
            report_service=SERVICE_PROVIDER.report_service(),
            coverage_service=SERVICE_PROVIDER.coverage_service(),
        ),
        ResourceReportHandler.build()
    ]
)

HANDLERS.append(
    SiemPushHandler(
        ambiguous_job_service=SERVICE_PROVIDER.ambiguous_job_service(),
        modular_service=SERVICE_PROVIDER.modular_service(),
        report_service=SERVICE_PROVIDER.report_service(),
        ssm_client=SERVICE_PROVIDER.ssm()
    )
)

REPORT_GENERATOR = ReportGenerator(handlers=HANDLERS)


def lambda_handler(event, context):
    return REPORT_GENERATOR.lambda_handler(event=event, context=context)
