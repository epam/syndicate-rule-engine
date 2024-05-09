from functools import cached_property
from http import HTTPStatus
import time

from handlers import AbstractHandler, Mapping
from helpers import RequestContext, batches, to_api_gateway_event
from helpers.constants import (
    ALL_ATTR,
    CustodianEndpoint,
    HTTPMethod,
    ReportDispatchStatus,
)
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from models.report_statistics import ReportStatistics
from services import SERVICE_PROVIDER
from services.report_statistics_service import ReportStatisticsService

SEND_REPORTS_STATE_MACHINE = 'send_reports'
CUSTOMER_NAME_ATTR = 'customer_name'

_LOG = get_logger(__name__)


class RetryHandler(AbstractHandler):
    def __init__(self, report_statistics_service: ReportStatisticsService,
                 step_function_client, setting_service):
        self.report_statistics_service = report_statistics_service
        self.step_function_client = step_function_client
        self.setting_service = setting_service
        self.entity_report_mapping = {}  # this cache won't work if different lambda executions

    @classmethod
    def build(cls) -> 'RetryHandler':
        return cls(
            report_statistics_service=SERVICE_PROVIDER.report_statistics_service,
            step_function_client=SERVICE_PROVIDER.step_function,
            setting_service=SERVICE_PROVIDER.settings_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_RETRY: {
                HTTPMethod.POST: self.post
            }
        }

    def post(self, event: dict, context: RequestContext):
        self.entity_report_mapping.clear()  # why?
        for batch in batches(self.report_statistics_service.iter_pending(), 10):
            self.process_pending_reports(batch, context)
        return build_response(
            code=HTTPStatus.OK,
            content=f'Reports for '
                    f'{", ".join(self.entity_report_mapping.keys())} '
                    f'were triggered.'
        )

    def process_pending_reports(self, items: list[ReportStatistics],
                                context: RequestContext):
        for item in items:
            self.invoke_pending_reports(item)
            time.sleep(self.setting_service.get_retry_interval())
            if context.get_remaining_time_in_millis() <= 90:
                raise TimeoutError()

    def invoke_pending_reports(self, item: ReportStatistics) -> None:
        _LOG.debug(f'Processing {item.id} item')
        entity = item.tenant or item.customer_name
        report_type = item.type if item.type else ALL_ATTR
        level = item.level
        if self.entity_report_mapping.get(entity, {}).get(level, {}) and \
                report_type in self.entity_report_mapping[entity][level]:
            _LOG.debug(f'{level.capitalize()}-level report for '
                       f'{entity} has already been submitted (type: '
                       f'{report_type}.')
            self.report_statistics_service.update(
                item, status=ReportDispatchStatus.DUPLICATE
            )
            return
        item_event = item.event.as_dict()
        if not item_event:
            _LOG.warning(f'Failed report with id `{item.id}` does not '
                         f'contain event. Cannot resend report.')
            self.report_statistics_service.update(
                item,
                status=ReportDispatchStatus.FAILED,
                reason='The report item does not contain an event. '
                       'Unable to resend report'
            )
            return
        event = to_api_gateway_event(item_event)

        _LOG.info(f'Invoking step function for retry with event: {event}')
        is_success = self.step_function_client.invoke(
            state_machine_name=SEND_REPORTS_STATE_MACHINE,
            event=event
        )
        if not is_success:
            _LOG.debug(f'No response from step-function '
                       f'{SEND_REPORTS_STATE_MACHINE}')
            self.report_statistics_service.update(
                item,
                status=ReportDispatchStatus.FAILED,
                reason='Cannot resend report due to step '
                       'function malfunction'

            )
            return
        _LOG.debug(f'Submitted {report_type.capitalize()}-level '
                   f'report for {entity}.')
        self.entity_report_mapping.setdefault(entity, {}).setdefault(
            level, set()).add(report_type)
        _LOG.debug('Changing status of the old item to \'RETRIED\'')
        self.report_statistics_service.update(
            item,
            status=ReportDispatchStatus.RETRIED
        )
