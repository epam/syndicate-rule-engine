import uuid
from unittest.mock import MagicMock

import pytest

from helpers.constants import JobState
from services.job_lock import TenantSettingJobLock


def _id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def lock() -> TenantSettingJobLock:
    return TenantSettingJobLock(f'TENANT-{uuid.uuid4().hex}')


@pytest.fixture
def handler():
    from lambdas.api_handler.handlers.job_handler import JobHandler

    return JobHandler(
        **{
            name: MagicMock()
            for name in JobHandler.__init__.__code__.co_varnames[
                1 : JobHandler.__init__.__code__.co_argcount
            ]
        }
    )


def test_acquired_lock_blocks_overlapping_regions(lock):
    first = _id()
    lock.acquire(job_id=first, regions={'eu-west-1', 'eu-central-1'})
    assert lock.locked_for({'eu-west-1'}) == first
    assert lock.locked_for({'us-east-1'}) is None


def test_handler_releases_lock_when_submit_fails(handler, lock):
    job = MagicMock(id=_id())
    lock.acquire(job_id=job.id, regions={'eu-west-1'})
    handler._submit_job_to_batch = MagicMock(side_effect=RuntimeError)

    with pytest.raises(RuntimeError):
        handler._submit_locked_job(lock._tenant_name, job)

    assert lock.locked_for({'eu-west-1'}) is None
    kwargs = handler._job_service.update.call_args.kwargs
    assert kwargs['status'] == JobState.FAILED
