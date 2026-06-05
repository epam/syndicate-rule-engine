"""Reactive (event-driven) job Celery tasks."""

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

from executor.job.execution.orchestrator import run_reactive_job

from ._common import setup_job_execution_context

if TYPE_CHECKING:
    from celery import Task

_LOG = get_logger(__name__)


def task_reactive_job(self: 'Task | None', job_id: str) -> None:
    """Runs a single reactive job by id."""
    ctx = setup_job_execution_context(job_id)
    if not ctx:
        return
    with ctx:
        run_reactive_job(ctx)
