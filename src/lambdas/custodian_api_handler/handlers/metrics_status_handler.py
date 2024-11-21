from datetime import date, datetime

from modular_sdk.models.job import Job
from modular_sdk.modular import Modular

from helpers.constants import CustodianEndpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from handlers import AbstractHandler, Mapping
from services import SERVICE_PROVIDER
from services.environment_service import EnvironmentService
from validators.swagger_request_models import MetricsStatusGetModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class MetricsStatusHandler(AbstractHandler):
    def __init__(self, modular_client: Modular,
                 environment_service: EnvironmentService):
        self.modular_client = modular_client
        self.environment_service = environment_service

    @classmethod
    def build(cls) -> 'MetricsStatusHandler':
        return cls(
            modular_client=SERVICE_PROVIDER.modular_client,
            environment_service=SERVICE_PROVIDER.environment_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.METRICS_STATUS: {
                HTTPMethod.GET: self.get
            }
        }

    @validate_kwargs
    def get(self, event: MetricsStatusGetModel):
        from_ = event.start_iso
        to = event.end_iso
        rkc = None
        component_name = (self.modular_client.
                          environment_service().component())

        if from_ and to:
            rkc = (Job.started_at.between(from_, to))
        elif from_:
            rkc = (Job.started_at >= from_)
        elif to:
            rkc = (Job.started_at < to)
        _LOG.debug(f'Range key condition: {rkc}')

        # TODO api add job_service with corresponding methods
        items = list(Job.job_started_at_index.query(
            hash_key=component_name, limit=1 if rkc is None else 10,
            range_key_condition=rkc, scan_index_forward=False))

        if not items:
            _LOG.error(f'Cannot find metrics update job with component name: '
                       f'{component_name}')
        response = []
        for item in items:
            _LOG.debug(f'Retrieved ModularJobs item: {item.to_json()}')
            response.append(self.get_metrics_status_dto(item))
        return build_response(content=response)

    @staticmethod
    def get_metrics_status_dto(item: Job):
        started_at = item.started_at
        if isinstance(started_at, datetime | date):
            started_at = started_at.isoformat(sep=' ', timespec="seconds")
        return {
            'started_at': started_at,
            'state': item.state
        }
