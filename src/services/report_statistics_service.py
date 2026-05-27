import json
import uuid
from enum import Enum

from pynamodb.pagination import ResultIterator

from helpers import get_logger
from helpers.constants import ReportDispatchStatus
from helpers.lambda_response import ReportNotSendException, SREException
from helpers.time_helper import utc_iso
from models.report_statistics import ReportStatistics
from services.abs_lambda import ProcessedEvent
from services.setting_service import SettingsService

_LOG = get_logger(__name__)


class ReportStatisticsService:
    def __init__(self, setting_service: SettingsService):
        self._setting_service = setting_service

    @staticmethod
    def job_id_from_execution_id(ex_id: str | None) -> str | None:
        if not ex_id:
            return
        return ex_id.split(':')[-1]

    @staticmethod
    def _prepare_event(event: ProcessedEvent) -> dict:
        processed = {}
        for k, v in event.items():
            if k in ('tenant_access_payload', 'additional_kwargs'):
                continue
            match v:
                case Enum():
                    processed[k] = v.value
                case _:
                    processed[k] = v
        return processed

    def create_from_processed_event(self, event: ProcessedEvent
                                    ) -> ReportStatistics:
        ex_id = event['body'].get('execution_job_id')
        job_id = self.job_id_from_execution_id(ex_id)
        attempt = event['body'].get('attempt', 0)

        body = event['body']
        tenant = ','.join(
            body.get('tenant_names') or body.get('tenant_display_names') or ()
        )
        types = ','.join(body.get('types') or ('ALL', ))
        return ReportStatistics(
            id=job_id or str(uuid.uuid4()),  # it actually must always exist
            triggered_at=utc_iso(),
            attempt=attempt,
            user=event['cognito_user_id'],
            level=event['resource'].split('/')[-1],
            type=types,
            status=ReportDispatchStatus.PENDING.value,
            customer_name=event['cognito_customer'],
            tenant=tenant,
            event=self._prepare_event(event)
        )

    def create_failed(self, event: ProcessedEvent,
                      exception: ReportNotSendException | SREException
                      ) -> ReportStatistics:
        item = self.create_from_processed_event(event)
        item.status = ReportDispatchStatus.FAILED.value
        content = exception.response.content
        if isinstance(content, dict) and 'message' in content:
            item.reason = content['message']
        else:
            item.reason = json.dumps(content, separators=(',', ':'))
        item.save()
        if item.attempt == self._setting_service.get_max_attempt_number():
            _LOG.info('Max number of report retries reached. Disabling them')
            self._setting_service.disable_send_reports()
        return item

    def save(self, item: ReportStatistics):
        item.save()

    @staticmethod
    def iter_by_id(job_id: str, customer: str | None = None,
                   limit: int | None = None) -> ResultIterator[ReportStatistics]:
        fc = None
        if customer:
            fc = (ReportStatistics.customer_name == customer)
        return ReportStatistics.query(
            hash_key=job_id,
            limit=limit,
            scan_index_forward=False,
            filter_condition=fc
        )

    @staticmethod
    def iter_by_customer(customer_name: str, triggered_at: str | None,
                         end_date: str | None
                         ) -> ResultIterator[ReportStatistics]:
        rkc = None
        if triggered_at and end_date:
            rkc = ReportStatistics.triggered_at.between(
                lower=triggered_at,
                upper=end_date
            )
        elif triggered_at:
            rkc = (ReportStatistics.triggered_at >= triggered_at)
        elif end_date:
            rkc = (ReportStatistics.triggered_at < end_date)
        return ReportStatistics.customer_name_triggered_at_index.query(
            hash_key=customer_name,
            range_key_condition=rkc
        )

    @staticmethod
    def iter_pending(page_size: int = 10) -> ResultIterator[ReportStatistics]:
        return ReportStatistics.status_index.query(
            hash_key=ReportDispatchStatus.PENDING.value,
            page_size=page_size
        )

    @staticmethod
    def update(item: ReportStatistics,
               status: ReportDispatchStatus | None = None,
               reason: str | None = None) -> None:
        actions = []
        if status:
            actions.append(ReportStatistics.status.set(status.value))
        if reason:
            actions.append(ReportStatistics.reason.set(reason))
        if actions:
            item.update(actions=actions)

    @staticmethod
    def dto(item: ReportStatistics) -> dict:
        return {
            'id': item.id,
            'triggered_at': item.triggered_at,
            'attempt': item.attempt,
            'types': item.type.split(',') if item.type else [],
            'level': item.level,
            'status': item.status,
            'customer': item.customer_name,
            'tenants': item.tenant.split(',') if item.tenant else [],
            'reason': item.reason
        }
