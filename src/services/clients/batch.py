import uuid

from typing_extensions import TYPE_CHECKING, NotRequired, TypedDict

from helpers import title_keys
from helpers.constants import BatchJobEnv, JobState
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from onprem.celery import app as celery_app
from onprem.tasks import run_executor
from services import SP
from services.clients import Boto3ClientWrapper
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService

if TYPE_CHECKING:
    from models.job import Job

_LOG = get_logger(__name__)


class BatchJob(TypedDict):
    jobArn: str
    jobName: str
    jobId: str
    jobQueue: str
    status: str
    createdAt: int
    startedAt: int
    stoppedAt: int
    celeryTaskId: NotRequired[str]
    # ... and other params


class BatchClient(Boto3ClientWrapper):
    service_name = 'batch'

    def __init__(
        self, environment_service: EnvironmentService, sts_client: StsClient
    ):
        self._environment = environment_service
        self._sts_client = sts_client

    @classmethod
    def build(cls) -> 'BatchClient':
        return cls(
            environment_service=SP.environment_service, sts_client=SP.sts
        )

    def build_queue_arn(self, queue_name: str) -> str:
        """
        Builds queue arn based on ones name. We might as well use
        batch `describe_job_queues` action
        """
        return (
            f'arn:aws:batch:{self._environment.aws_region()}:'
            f'{self._sts_client.get_account_id()}:job-queue/{queue_name}'
        )

    def get_custodian_job_queue_arn(self) -> str:
        """
        Retrieves Custodian's job queue arn
        """
        return self.build_queue_arn(self._environment.get_batch_job_queue())

    def get_custodian_job_definition_arn(self) -> str:
        """
        Retrieves the latest active Custodian's job definition
        """
        job_definitions = self.get_job_definition_by_name(
            self._environment.get_batch_job_def()
        )
        # we can be sure that there is at least one job def, otherwise
        # the service is not properly configured
        return job_definitions[0]['jobDefinitionArn']

    def get_job_definition_by_name(self, job_def_name):
        _LOG.debug(f'Retrieving last job definition with name {job_def_name}')
        return self.client.describe_job_definitions(
            jobDefinitionName=job_def_name, status='ACTIVE', maxResults=1
        )['jobDefinitions']

    def terminate_job(self, job: 'Job', reason: str):
        return self.client.terminate_job(jobId=job.batch_job_id, reason=reason)

    @staticmethod
    def build_container_overrides(
        command: str | list = None,
        environment: dict = None,
        titled: bool = False,
    ) -> dict:
        """
        Builds container overrides dict for AWS Batch
        :parameter command: Union[str, list]
        :parameter environment: dict
        :parameter titled: bool
        :return: dict
        """
        result = {'containerOverrides': {}}
        if command:
            result['containerOverrides']['command'] = (
                command.split() if isinstance(command, str) else command
            )
        if environment:
            result['containerOverrides']['environment'] = [
                {'name': key, 'value': value}
                for key, value in environment.items()
            ]
        if titled:
            result = title_keys(result)
        return result

    def submit_job(
        self,
        job_name: str,
        job_queue: str,
        job_definition: str,
        command: str = None,
        size: int = None,
        depends_on: list = None,
        parameters=None,
        retry_strategy: int = None,
        timeout: int = None,
        environment_variables: dict = None,
    ) -> BatchJob:
        params = {
            'jobName': job_name,
            'jobQueue': job_queue,
            'jobDefinition': job_definition,
        }
        if size:
            params['arrayProperties'] = {'size': size}
        if depends_on:
            params['dependsOn'] = depends_on
        if parameters:
            params['parameters'] = parameters
        if retry_strategy:
            params['retryStrategy'] = {'attempts': retry_strategy}
        if timeout:
            params['timeout'] = {'attemptDurationSeconds': timeout}
        container_overrides = self.build_container_overrides(
            command=command, environment=environment_variables
        )
        params.update(container_overrides)
        return self.client.submit_job(**params)


class CeleryJobClient:
    @classmethod
    def build(cls) -> 'CeleryJobClient':
        return cls()

    def submit_job(
        self,
        job_name: str,
        environment_variables: dict[str, str] | None = None,
        **kwargs,
    ) -> BatchJob:
        job_id = str(uuid.uuid4())
        envs = {
            # **os.environ,
            **(environment_variables or {}),
            BatchJobEnv.JOB_ID.value: job_id,
            BatchJobEnv.SUBMITTED_AT.value: utc_iso(),
        }
        res = run_executor.delay(envs)
        return {
            'jobId': job_id,
            'jobName': job_name,
            'celeryTaskId': res.id,
            'status': JobState.SUBMITTED.value,
        }

    def terminate_job(self, job: 'Job', reason: str):
        if not job.celery_task_id:
            _LOG.warning(
                f'Job {job.id} does not contain celery task id. Cannot terminate'
            )
            return
        # TODO: handle terminate signal inside the job.
        celery_app.control.revoke(job.celery_task_id, terminate=True)


# TODO: remove
class SubprocessBatchClient:
    def __init__(self):
        self._jobs = {}  # job_id to Job
