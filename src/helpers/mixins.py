from helpers.log_helper import get_logger
from models.job import Job
from services.clients.batch import (
    BatchClient,
    BatchJob,
    CeleryJob,
    CeleryJobClient,
)
from services.environment_service import EnvironmentService


_LOG = get_logger(__name__)


class SubmitJobToBatchMixin:
    """
    Mixin for submitting jobs to batch
    """

    _batch_client: BatchClient | CeleryJobClient
    _environment_service: EnvironmentService

    def _submit_jobs_to_batch(
        self,
        jobs: list[Job],
        timeout: int | None = None,
    ) -> BatchJob | CeleryJob:
        job_ids = [job.id for job in jobs]
        job_name = [job.tenant_name for job in jobs]
        job_name = "".join(
            ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in job_name
        )

        if isinstance(self._batch_client, BatchClient):
            raise NotImplementedError("Batch client is not supported")
        else:
            _LOG.debug(f"Going to submit Celery job with name {job_name}")
            response = self._batch_client.submit_job(
                job_id=job_ids,
                job_name=job_name,
                timeout=timeout,
            )
            _LOG.debug(f"Celery job was submitted: {response}")
        
        return response
   
    def _submit_job_to_batch(
        self,
        tenant_name: str,
        job: Job,
        timeout: int | None = None,
    ) -> BatchJob | CeleryJob:
        job_name = f"{tenant_name}-{job.submitted_at}"
        job_name = "".join(
            ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in job_name
        )

        response: BatchJob | CeleryJob
        if isinstance(self._batch_client, BatchClient):
            _LOG.debug(f"Going to submit AWS Batch job with name {job_name}")
            response = self._batch_client.submit_job(
                job_id=job.id,
                job_name=job_name,
                job_queue=self._environment_service.get_batch_job_queue(),
                job_definition=self._environment_service.get_batch_job_def(),
                timeout=timeout,
            )
            _LOG.debug(f"AWS Batch job was submitted: {response}")
        else:
            _LOG.debug(f"Going to submit Celery job with name {job_name}")
            response = self._batch_client.submit_job(
                job_id=job.id,
                job_name=job_name,
                timeout=timeout,
            )
            _LOG.debug(f"Celery job was submitted: {response}")

        return response
