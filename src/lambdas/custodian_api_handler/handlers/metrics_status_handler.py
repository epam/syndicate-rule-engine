from functools import cached_property

from modular_sdk.models.job import Job

from helpers import build_response
from helpers.constants import GET_METHOD
from helpers.log_helper import get_logger
from lambdas.custodian_api_handler.handlers import Mapping
from services import SERVICE_PROVIDER
from services.environment_service import EnvironmentService
from services.modular_service import ModularService

_LOG = get_logger(__name__)


class MetricsStatusHandler:
    def __init__(self, modular_service: ModularService,
                 environment_service: EnvironmentService):
        self.modular_service = modular_service
        self.environment_service = environment_service

    @classmethod
    def build(cls) -> 'MetricsStatusHandler':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service(),
            environment_service=SERVICE_PROVIDER.environment_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            '/metrics/status': {
                GET_METHOD: self.get
            }
        }

    def get(self, event: dict) -> dict:
        component_name = self.environment_service.component_name()
        item = list(Job.job_started_at_index.query(
            hash_key=component_name, limit=1,
            scan_index_forward=False))
        if not item:
            _msg = 'Cannot find latest metrics update job'
            _LOG.error(_msg + f' with component name: {component_name}')
            return build_response(_msg)
        item = item[0]
        started_at = item.attribute_values.get("started_at")
        state = item.attribute_values.get("state")
        _LOG.debug(f'Retrieved ModularJobs item: {item.to_json()}')
        return build_response(
            content=f'Last metrics update was started at {started_at} with '
                    f'status {state}')
