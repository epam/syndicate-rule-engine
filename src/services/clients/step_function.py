from abc import ABC

from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class AbstractStepFunctionClient(ABC):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    def invoke(self, state_machine_name: str, event: dict,
               job_id: str | None = None) -> bool:
        pass


class ScriptClient(AbstractStepFunctionClient):
    def invoke(self, state_machine_name, event: dict, job_id: str = None):
        _LOG.warning('Step function client is not implemented for on-prem')
        return False
