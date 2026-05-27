from http import HTTPStatus

from handlers import AbstractHandler, Mapping
from helpers import get_logger
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from services import SP
from services.report_statistics_service import ReportStatisticsService
from validators.swagger_request_models import ReportStatusGetModel
from validators.utils import validate_kwargs


_LOG = get_logger(__name__)


class ReportStatusHandlerHandler(AbstractHandler):

    def __init__(self, report_statistics_service: ReportStatisticsService):
        self.report_statistics_service = report_statistics_service

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_STATUS: {
                HTTPMethod.GET: self.get_status
            }
        }

    @classmethod
    def build(cls) -> 'ReportStatusHandlerHandler':
        return cls(
            report_statistics_service=SP.report_statistics_service
        )

    @validate_kwargs
    def get_status(self, event: ReportStatusGetModel):
        _LOG.debug(f'Retrieving items from SREReportStatistics table with id '
                   f'{event.job_id} and customer {event.customer}')
        items = self.report_statistics_service.iter_by_id(
            job_id=event.job_id,
            customer=event.customer,
            limit=1 if not event.complete else None
        )
        return build_response(
            code=HTTPStatus.OK,
            content=map(self.report_statistics_service.dto, items)
        )
