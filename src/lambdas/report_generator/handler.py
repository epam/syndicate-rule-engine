from functools import cached_property

from handlers.reports import (
    ComplianceReportHandler,
    DetailedReportHandler,
    DigestReportHandler,
    ErrorsReportHandler,
    FindingsReportHandler,
    HighLevelReportsHandler,
    JobsRulesHandler,
    RawReportHandler,
    ResourceReportHandler,
    SiemPushHandler,
)
from helpers.log_helper import get_logger
from services.abs_lambda import (
    ApiEventProcessorLambdaHandler,
    ApiGatewayEventProcessor,
    CheckPermissionEventProcessor,
    ExpandEnvironmentEventProcessor,
    RestrictCustomerEventProcessor,
    RestrictTenantEventProcessor,
)
from validators.registry import permissions_mapping


_LOG = get_logger(__name__)


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
        RawReportHandler,
        HighLevelReportsHandler
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
