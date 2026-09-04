from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from typing_extensions import Any, Self, TypedDict

from helpers.constants import JobState
from helpers.log_helper import get_logger
from onprem.celery import app as celery_app

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
        as_event_driven: bool = False,
        **kwargs: Any,
    ) -> CeleryJob:
        if as_event_driven:
            from onprem.tasks import run_reactive_job

            func = run_reactive_job
        else:
            from onprem.tasks import run_standard_job

            func = run_standard_job

        res = func.apply_async((job_id,), soft_time_limit=timeout)
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

    def is_job_execution_active(self, job: Job) -> bool:
        return self.is_celery_task_active(job.celery_task_id)

    @staticmethod
    def _celery_task_in_inspect(
        task_id: str,
        insp: Any,
        workers_up: bool,
    ) -> bool:
        if not insp or not workers_up:
            _LOG.warning(
                'Celery inspect: no reachable workers; assuming task %s not active',
                task_id,
            )
            return False
        for payload in (
            insp.active() or {},
            insp.reserved() or {},
            insp.scheduled() or {},
        ):
            for tasks in payload.values():
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    if task.get('id') == task_id:
                        return True
                    req = task.get('request')
                    if isinstance(req, dict) and req.get('id') == task_id:
                        return True
        return False

    @staticmethod
    def is_celery_task_active(task_id: str | None) -> bool:
        """
        True if the task is still in-flight on some worker (active, reserved, or
        scheduled). If inspect cannot reach any worker, returns False so hosts
        that restarted can reconcile stuck RUNNING jobs.

        When ``task_ignore_result`` is enabled, Celery uses :class:`~celery.backends.base.DisabledBackend`
        and :class:`~celery.result.AsyncResult` cannot read status; this path uses
        broker inspect only.
        """
        from celery.backends.base import DisabledBackend
        from celery.result import AsyncResult

        timeout = 0.5

        if not task_id:
            return False
        if isinstance(celery_app.backend, DisabledBackend):
            insp = celery_app.control.inspect(timeout=timeout)
            workers_up = bool(
                insp and (insp.stats() or insp.ping()),
            )
            return CeleryJobClient._celery_task_in_inspect(
                task_id, insp, workers_up
            )

        result = AsyncResult(task_id, app=celery_app)
        try:
            if result.ready():
                return False
        except (AttributeError, NotImplementedError) as e:
            _LOG.warning(
                'Celery result backend cannot read task %s status (%s); '
                'using inspect only',
                task_id,
                e,
            )
            insp = celery_app.control.inspect(timeout=timeout)
            workers_up = bool(
                insp and (insp.stats() or insp.ping()),
            )
            return CeleryJobClient._celery_task_in_inspect(
                task_id, insp, workers_up
            )

        insp = celery_app.control.inspect(timeout=timeout)
        workers_up = bool(
            insp and (insp.stats() or insp.ping()),
        )
        if result.state == 'PENDING' and workers_up:
            return True
        return CeleryJobClient._celery_task_in_inspect(
            task_id, insp, workers_up
        )
