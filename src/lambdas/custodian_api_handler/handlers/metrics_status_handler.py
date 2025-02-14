from datetime import date, datetime

from modular_sdk.models.job import Job
from modular_sdk.modular import Modular

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from services import SP
from services.reports import ReportMetricsService
from validators.swagger_request_models import MetricsStatusGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class MetricsStatusHandler(AbstractHandler):
    def __init__(
        self,
        modular_client: Modular,
        report_metrics_service: ReportMetricsService,
    ):
        self._mc = modular_client
        self._rms = report_metrics_service

    @classmethod
    def build(cls) -> 'MetricsStatusHandler':
        return cls(
            modular_client=SP.modular_client,
            report_metrics_service=SP.report_metrics_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {CustodianEndpoint.METRICS_STATUS: {HTTPMethod.GET: self.get}}

    @validate_kwargs
    def get(self, event: MetricsStatusGetModel):
        from_ = event.start_iso
        to = event.end_iso
        rkc = None

        if from_ and to:
            rkc = Job.started_at.between(from_, to)
        elif from_:
            rkc = Job.started_at >= from_
        elif to:
            rkc = Job.started_at < to
        _LOG.debug(f'Range key condition: {rkc}')

        # TODO api add job_service with corresponding methods
        items = list(
            Job.job_started_at_index.query(
                hash_key='metrics',
                limit=1 if rkc is None else 10,
                range_key_condition=rkc,
                scan_index_forward=False,
            )
        )

        if not items:
            _LOG.warning('Cannot find metrics update job')
        response = []
        for item in items:
            response.append(self.get_metrics_status_dto(item))
        return build_response(content=response)

    @staticmethod
    def get_metrics_status_dto(item: Job) -> dict:
        started_at = item.started_at
        if isinstance(started_at, datetime | date):
            started_at = utc_iso(started_at)
        return {'started_at': started_at, 'state': item.state}
