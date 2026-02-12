from __future__ import annotations

import uuid
from typing import Optional

from typing_extensions import TYPE_CHECKING, Any, NotRequired, Self, TypedDict

from helpers import title_keys
from helpers.constants import JobState
from helpers.log_helper import get_logger
from onprem.celery import app as celery_app
from onprem.tasks import run_standard_job
from services import SP
from services.clients import Boto3ClientWrapper
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService


if TYPE_CHECKING:
    from models.job import Job

_LOG = get_logger(__name__)


class CeleryJob(TypedDict):
    """
    Asynchronous job response from Celery.
    """

    jobId: Optional[str]
    jobName: str
    celeryTaskId: str
    status: str


class BatchJob(TypedDict):
    """
    Asynchronous job response from AWS Batch/Celery.
    """

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
    """
    Client for submitting and terminating AWS Batch jobs.
    """

    service_name = "batch"

    def __init__(
        self,
        environment_service: EnvironmentService,
        sts_client: StsClient,
    ) -> None:
        self._environment = environment_service
        self._sts_client = sts_client

    @classmethod
    def build(cls) -> Self:
        return cls(
            environment_service=SP.environment_service,
            sts_client=SP.sts,
        )

    def build_queue_arn(self, queue_name: str) -> str:
        """
        Builds queue arn based on ones name. We might as well use
        batch `describe_job_queues` action
        """
        return (
            f"arn:aws:batch:{self._environment.aws_region()}:"
            f"{self._sts_client.get_account_id()}:job-queue/{queue_name}"
        )

    def get_custodian_job_queue_arn(self) -> str:
        """
        Retrieves Custodian's job queue arn
        """
        queue_name = self._environment.get_batch_job_queue()
        if not queue_name:
            raise ValueError("Batch job queue name is not set")
        return self.build_queue_arn(queue_name)

    def get_custodian_job_definition_arn(self) -> str:
        """
        Retrieves the latest active Custodian's job definition
        """
        job_definitions = self.get_job_definition_by_name(
            self._environment.get_batch_job_def()
        )
        # we can be sure that there is at least one job def, otherwise
        # the service is not properly configured
        return job_definitions[0]["jobDefinitionArn"]

    def get_job_definition_by_name(self, job_def_name):
        _LOG.debug(f"Retrieving last job definition with name {job_def_name}")
        return self.client.describe_job_definitions(
            jobDefinitionName=job_def_name, status="ACTIVE", maxResults=1
        )["jobDefinitions"]

    def terminate_job(self, job: Job, reason: str):
        return self.client.terminate_job(jobId=job.batch_job_id, reason=reason)

    @staticmethod
    def build_container_overrides(
        command: str | list | None = None,
        environment: dict | None = None,
        titled: bool = False,
    ) -> dict | list:
        """
        Builds container overrides dict for AWS Batch
        :parameter command: Union[str, list]
        :parameter environment: dict
        :parameter titled: bool
        :return: dict
        """
        result = {"containerOverrides": {}}
        if command:
            result["containerOverrides"]["command"] = (
                command.split() if isinstance(command, str) else command
            )
        if environment:
            result["containerOverrides"]["environment"] = [
                {"name": key, "value": value} for key, value in environment.items()
            ]
        if titled:
            result = title_keys(result)
        return result

    def submit_job(
        self,
        job_id: str,
        job_name: str,
        job_queue: str,
        job_definition: str,
        command: str | None = None,
        size: int | None = None,
        depends_on: list | None = None,
        parameters=None,
        retry_strategy: int | None = None,
        timeout: int | None = None,
        environment_variables: dict | None = None,
    ) -> BatchJob:
        params: dict[str, Any] = {
            "jobId": job_id,
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
        }
        if size:
            params["arrayProperties"] = {"size": size}
        if depends_on:
            params["dependsOn"] = depends_on
        if parameters:
            params["parameters"] = parameters
        if retry_strategy:
            params["retryStrategy"] = {"attempts": retry_strategy}
        if timeout:
            params["timeout"] = {"attemptDurationSeconds": timeout}
        container_overrides = self.build_container_overrides(
            command=command, environment=environment_variables
        )
        params.update(container_overrides)
        return self.client.submit_job(**params)


class CeleryJobClient:
    """
    Client for submitting and terminating Celery jobs.
    """

    service_name = "celery"

    @classmethod
    def build(cls) -> Self:
        return cls()

    def submit_job(
        self,
        job_id: str | list[str],
        job_name: str,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> CeleryJob:
        res = run_standard_job.apply_async((job_id,), soft_time_limit=timeout)
        return {
            "jobId": None,  # JobID is only available for AWS Batch jobs
            "jobName": job_name,
            "celeryTaskId": res.id,
            "status": JobState.SUBMITTED.value,
        }

    def terminate_job(self, job: Job, reason: str):
        if not job.celery_task_id:
            _LOG.warning(
                f"Job {job.id} does not contain celery task id. Cannot terminate"
            )
            return
        celery_app.control.revoke(job.celery_task_id, terminate=True)
