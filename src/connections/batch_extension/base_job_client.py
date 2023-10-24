import os

from services.clients.batch import BatchClient
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from helpers.constants import ENV_SERVICE_MODE, DOCKER_SERVICE_MODE, \
    SAAS_SERVICE_MODE, ENV_MAX_NUMBER_OF_JOBS_ON_PREM
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

SERVICE_MODE = os.getenv(ENV_SERVICE_MODE) or SAAS_SERVICE_MODE
MAX_NUMBER_OF_JOBS = int(os.environ.get(ENV_MAX_NUMBER_OF_JOBS_ON_PREM, 4))


def subprocess_handler_builder():
    handler = None

    def init_handler():
        assert SERVICE_MODE == DOCKER_SERVICE_MODE, \
            "You can init subprocess handler only if SERVICE_MODE=docker"
        nonlocal handler
        if handler:
            return handler
        from connections.batch_extension.batch_to_subprocess_adapter import \
            BatchToSubprocessAdapter
        handler = BatchToSubprocessAdapter(
            max_number_of_jobs=MAX_NUMBER_OF_JOBS)
        _LOG.info('Subprocess connection was successfully initialized')
        return handler
    return init_handler


SUBPROCESS_HANDLER = subprocess_handler_builder()


class BaseBatchClient(BatchClient):

    def __init__(self, environment_service: EnvironmentService,
                 sts_client: StsClient):
        super().__init__(environment_service=environment_service,
                         sts_client=sts_client)

    def submit_job(self, job_name: str, job_queue: str, job_definition: str,
                   command: str, size: int = None, depends_on: list = None,
                   parameters=None, retry_strategy: int = None,
                   timeout: int = None, environment_variables: dict = None):
        if self._environment.is_docker():
            return SUBPROCESS_HANDLER().submit_job(
                job_name=job_name,
                command=command,
                environment_variables=environment_variables
            )
        return super().submit_job(
            job_name=job_name,
            job_queue=job_queue,
            job_definition=job_definition,
            command=command,
            size=size,
            depends_on=depends_on,
            parameters=parameters,
            retry_strategy=retry_strategy,
            timeout=timeout,
            environment_variables=environment_variables
        )

    def terminate_job(self, job_id: str, reason: str = 'Terminating job.'):
        if self._environment.is_docker():
            return SUBPROCESS_HANDLER().terminate_job(job_id=job_id,
                                                      reason=reason)
        return super().terminate_job(job_id=job_id, reason=reason)

    def describe_jobs(self, jobs):
        if self._environment.is_docker():
            return SUBPROCESS_HANDLER().describe_jobs(jobs)
        return super().describe_jobs(jobs)
