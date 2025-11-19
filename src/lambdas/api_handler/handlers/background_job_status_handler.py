from datetime import date, datetime

from modular_sdk.models.job import Job
from modular_sdk.modular import Modular

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod, BackgroundJobName
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from services import SP
from services.reports import ReportMetricsService
from validators.swagger_request_models import BackgroundJobStatusGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class BackgroundJobStatusHandler(AbstractHandler):
    """
    Unified endpoint to get the status of all background jobs
    Support background job types: metrics and metadata via the path parameter: /{background_job_name}/status
    """
    
    def __init__(
        self,
        modular_client: Modular,
        report_metrics_service: ReportMetricsService,
    ):
        self._mc = modular_client
        self._rms = report_metrics_service

    @classmethod
    def build(cls) -> 'BackgroundJobStatusHandler':
        return cls(
            modular_client=SP.modular_client,
            report_metrics_service=SP.report_metrics_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.BACKGROUND_JOB_STATUS: {HTTPMethod.GET: self.get}}

    @validate_kwargs
    def get(
        self,
        event: BackgroundJobStatusGetModel,
        background_job_name: BackgroundJobName,
    ):
        from_ = event.start_iso
        to = event.end_iso
        rkc = None

        if from_ and to:
            rkc = Job.started_at.between(
                lower=utc_iso(from_), 
                upper=utc_iso(to),
            )
        elif from_:
            rkc = Job.started_at >= utc_iso(from_)
        elif to:
            rkc = Job.started_at < utc_iso(to)
        _LOG.debug(f'Range key condition: {rkc}')

        # TODO: api add job_service with corresponding methods
        items = list(
            Job.job_started_at_index.query(
                hash_key=background_job_name,
                limit=1 if rkc is None else 10,
                range_key_condition=rkc,
                scan_index_forward=False,
            )
        )

        if not items:
            _LOG.warning('Cannot find metrics update job')
        return build_response(
            content=[self._to_background_job_status_dto(item) for item in items]
        )

    @staticmethod
    def _to_background_job_status_dto(item: Job) -> dict:
        started_at = item.started_at
        if isinstance(started_at, datetime | date):
            started_at = utc_iso(started_at)
        return {
            'started_at': started_at, 
            'state': item.state,
        }
