from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP
from services.service_job_service import ServiceJobService
from validators.swagger_request_models import ServiceJobStatusGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class ServiceJobStatusHandler(AbstractHandler):
    """
    Unified handler for retrieving the status of service jobs.
    
    Supports multiple service job types (e.g., metrics, metadata) through
    the query parameter `type`. Allows optional filtering by
    date range using `start_iso` and `end_iso` query parameters.
    
    Endpoint: /service-job/status?type={service_job_type}
    Method: GET
    
    Returns a list of job statuses with their start time and current state.
    """
    
    def __init__(self, service_job_service: ServiceJobService):
        self._service_job_service = service_job_service

    @classmethod
    def build(cls) -> 'ServiceJobStatusHandler':
        return cls(
            service_job_service=SP.service_job_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.SERVICE_JOB_STATUS: {HTTPMethod.GET: self.get}}

    @validate_kwargs
    def get(self, event: ServiceJobStatusGetModel):
        service_job_type = event.type
        from_ = event.start_iso
        to = event.end_iso

        limit = 1 if not (from_ or to) else 10

        items = list(
            self._service_job_service.get_by_type(
                service_job_type=service_job_type,
                start=from_,
                end=to,
                limit=limit,
                ascending=False,
            )
        )

        if not items:
            _LOG.warning(f'Cannot find {service_job_type.value} job')
            
        return build_response(
            content=[self._service_job_service.to_dto(item) for item in items]
        )
