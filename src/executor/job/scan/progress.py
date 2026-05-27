from typing import Any

from helpers.constants import GLOBAL_REGION
from helpers.time_helper import utc_iso
from models.job import Job
from executor.job.scan.types import ScanCheckpoint
from validators.swagger_response_models import ScanProgress


def new_empty_scan_checkpoint() -> dict[str, Any]:
    """Initial checkpoint for a newly created job (no regions completed yet)."""
    return {
        'checkpoint_version': 0,
        'completed_regions': [],
        'updated_at': utc_iso(),
    }


def scan_checkpoint_from_job(job: Job) -> ScanCheckpoint | None:
    if not job.scan_checkpoint:
        return None
    return ScanCheckpoint(
        checkpoint_version=int(job.scan_checkpoint.checkpoint_version),
        completed_regions=[str(r) for r in job.scan_checkpoint.completed_regions],
        updated_at=job.scan_checkpoint.updated_at,
    )


def all_scan_regions(job: Job) -> list[str]:
    return sorted(set(job.regions) | {GLOBAL_REGION})


def pending_scan_regions(job: Job, checkpoint: ScanCheckpoint | None) -> list[str]:
    all_r = all_scan_regions(job)
    if not checkpoint:
        return list(all_r)
    done = set(checkpoint["completed_regions"])
    return [r for r in all_r if r not in done]


def scan_is_resumable(job: Job) -> bool:
    cp = scan_checkpoint_from_job(job)
    if not cp:
        return False
    return bool(pending_scan_regions(job, cp))


def scan_progress_dto(job: Job) -> ScanProgress | None:
    cp = scan_checkpoint_from_job(job)
    if not cp:
        return None
    return ScanProgress(
        checkpoint_version=cp["checkpoint_version"],
        completed_regions=list(cp["completed_regions"]),
        updated_at=cp["updated_at"],
        pending_regions=pending_scan_regions(job, cp),
    )
