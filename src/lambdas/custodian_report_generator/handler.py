from functools import cached_property

from handlers.compliance_handler import ComplianceReportHandler
from handlers.details_handler import DetailedReportHandler
from handlers.digest_handler import DigestReportHandler
from handlers.errors_handler import ErrorsReportHandler
from handlers.findings_handler import FindingsReportHandler
from handlers.push_handler import SiemPushHandler
from handlers.resource_report_handler import ResourceReportHandler
from handlers.rules_handler import JobsRulesHandler
from helpers.log_helper import get_logger
from handlers.raw_report_handler import RawReportHandler
from services.abs_lambda import (
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    RestrictCustomerEventProcessor,
    ExpandEnvironmentEventProcessor,
    ApiEventProcessorLambdaHandler,
    RestrictTenantEventProcessor
)
from validators.registry import permissions_mapping

_LOG = get_logger('caas-report-generator')


# TODO merge this lambda with report_generation_handler


class ReportGenerator(ApiEventProcessorLambdaHandler):
    processors = (
        ExpandEnvironmentEventProcessor.build(),
        ApiGatewayEventProcessor(permissions_mapping),
        RestrictCustomerEventProcessor.build(),
        CheckPermissionEventProcessor.build(),
        RestrictTenantEventProcessor.build()
    )
    handlers = (
        ComplianceReportHandler,
        ResourceReportHandler,
        JobsRulesHandler,
        DetailedReportHandler,
        DigestReportHandler,
        ErrorsReportHandler,
        SiemPushHandler,
        FindingsReportHandler,
        RawReportHandler
    )

    @cached_property
    def mapping(self):
        res = {}
        for h in self.handlers:
            res.update(h.build().mapping)
        return res


REPORT_GENERATOR = ReportGenerator()


def lambda_handler(event, context):
    return REPORT_GENERATOR.lambda_handler(event=event, context=context)
