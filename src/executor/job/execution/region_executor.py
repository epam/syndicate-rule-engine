"""
Region-level job execution in subprocess.

Cloud Custodian keeps consuming RAM for some reason. After 9th-10th region
scanned the used memory can be more than 1GI, and it isn't freed. We execute
scan for each region in a separate process consequently. When one process
finished its memory is freed (results are flushed to files).

Uses billiard instead of multiprocessing to allow daemonic Celery workers
to spawn child processes.
"""

import os
from pathlib import Path
from typing import NamedTuple

import billiard as multiprocessing

from executor.helpers.constants import AWS_DEFAULT_REGION
from executor.job.job_failure import failure_detail_from_exception
from executor.job.policies.loader import PoliciesLoader
from executor.job.policies.runners import Runner
from executor.job.types import PolicyDict
from executor.plugins import register_all
from helpers.constants import Cloud, Env
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class RegionScanResult(NamedTuple):
    """Pickle-friendly result from :func:`process_job_concurrent`."""

    n_successful: int
    failed: dict | None
    load_error_detail: str | None


def job_initializer(envs: dict):
    _LOG.info(
        f'Initializing subprocess for a region: {multiprocessing.current_process()}'
    )
    os.environ.update(envs)
    os.environ.setdefault('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION)


def process_job_concurrent(
    items: list[PolicyDict],
    work_dir: Path,
    cloud: Cloud,
    region: str,
) -> RegionScanResult:
    if Env.ENABLE_CUSTOM_CC_PLUGINS.is_set():
        register_all()

    _LOG.debug(f'Running scan process for region {region}')
    loader = PoliciesLoader(
        cloud=cloud,
        output_dir=work_dir,
        regions={region},
        cache_period=120,
    )
    try:
        _LOG.debug(f'Going to load {len(items)} policies dicts')
        policies = loader.load_from_policies(items)
    except Exception as exc:
        _LOG.exception(
            f'Unexpected error occurred trying to load policies for region {region}'
        )
        return RegionScanResult(0, None, failure_detail_from_exception(exc))

    _LOG.info(
        f'{len(policies)} policies instances were loaded and due '
        f'to be executed (one policy instance per available region)'
    )
    _LOG.info('Starting runner')
    runner = Runner.factory(cloud, policies)
    runner.start()
    _LOG.info('Runner has finished')

    return RegionScanResult(runner.n_successful, runner.failed, None)
