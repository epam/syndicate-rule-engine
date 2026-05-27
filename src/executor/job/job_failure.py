"""
Structured job failures: stable codes for monitoring/API and human-readable reasons.

Use :class:`JobExecutionError` (see ``executor.job.types``) to raise from orchestration code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import Mapping

from executor.helpers.constants import (
    ACCESS_DENIED_ERROR_CODE,
    INVALID_CREDENTIALS_ERROR_CODES,
)
from helpers.constants import Cloud

_DETAIL_MAX_LEN = 480


class JobErrorCode(str, Enum):
    """Stable machine-facing codes; persist or expose alongside user text."""

    NO_CREDENTIALS = "NO_CREDENTIALS"
    NO_SUCCESSFUL_POLICIES = "NO_SUCCESSFUL_POLICIES"
    LM_DID_NOT_ALLOW = "LM_DID_NOT_ALLOW"
    METADATA_UPDATE_FAILED = "METADATA_UPDATE_FAILED"
    TENANT_NOT_FOUND = "TENANT_NOT_FOUND"
    TIMEOUT = "TIMEOUT"
    INTERNAL = "INTERNAL"
    INVALID_CLOUD_CREDENTIALS = "INVALID_CLOUD_CREDENTIALS"
    CLOUD_ACCESS_DENIED = "CLOUD_ACCESS_DENIED"
    CLOUD_PROVIDER_ERROR = "CLOUD_PROVIDER_ERROR"
    WORKER_LOST = "WORKER_LOST"


@dataclass(slots=True)
class JobFailure:
    code: JobErrorCode
    message: str
    detail: str | None = None
    exit_code: int = 1
    context: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def standard(
        cls,
        code: JobErrorCode,
        *,
        detail: str | None = None,
        exit_code: int | None = None,
        context: Mapping[str, str] | None = None,
    ) -> JobFailure:
        """
        Build a failure with the canonical user message for ``code``.

        Exit code is 2 for :attr:`JobErrorCode.LM_DID_NOT_ALLOW`, otherwise 1,
        unless overridden.
        """
        ec = (
            exit_code
            if exit_code is not None
            else (2 if code == JobErrorCode.LM_DID_NOT_ALLOW else 1)
        )
        ctx = dict(context) if context else {}
        return cls(
            code=code,
            message=_DEFAULT_MESSAGES[code],
            detail=detail,
            exit_code=ec,
            context=ctx,
        )

    def to_reason(self) -> str:
        if self.detail:
            return f"{self.message}: {self.detail}"
        return self.message

    def log_extras(self) -> dict[str, str]:
        out: dict[str, str] = {"job_error_code": self.code.value}
        out.update(dict(self.context))
        return out


def classify_exception(
    exc: BaseException,
) -> JobFailure:
    """
    Map an unexpected exception to :class:`JobFailure`.
    """

    if isinstance(exc, _worker_lost_exception_types()):
        return JobFailure.standard(JobErrorCode.WORKER_LOST)

    code = _client_error_code(exc)
    if code is not None:
        if code in _invalid_credential_codes_union():
            return JobFailure.standard(
                JobErrorCode.INVALID_CLOUD_CREDENTIALS,
                detail=code,
            )
        if code in _access_denied_codes_union():
            return JobFailure.standard(
                JobErrorCode.CLOUD_ACCESS_DENIED,
                detail=code,
            )
        return JobFailure.standard(
            JobErrorCode.CLOUD_PROVIDER_ERROR,
            detail=code,
        )

    detail = failure_detail_from_exception(exc)
    return JobFailure.standard(JobErrorCode.INTERNAL, detail=detail)


def default_message_for(code: JobErrorCode) -> str:
    """User-facing base message for a job error code."""
    return _DEFAULT_MESSAGES[code]


def failure_detail_from_exception(exc: BaseException) -> str:
    """Short, non-traceback summary for the job record (truncated only)."""
    name = type(exc).__name__
    msg = str(exc).strip() or "(no message)"
    msg = " ".join(msg.split())
    text = f"{name}: {msg}"
    return text[:_DETAIL_MAX_LEN]


_DEFAULT_MESSAGES: dict[JobErrorCode, str] = {
    JobErrorCode.NO_CREDENTIALS: "Could not resolve any credentials",
    JobErrorCode.NO_SUCCESSFUL_POLICIES: "All policies have failed",
    JobErrorCode.LM_DID_NOT_ALLOW: "License manager did not allow this job",
    JobErrorCode.METADATA_UPDATE_FAILED: "Failed to update metadata",
    JobErrorCode.TENANT_NOT_FOUND: "Tenant not found",
    JobErrorCode.TIMEOUT: "Task timeout exceeded",
    JobErrorCode.INTERNAL: "Internal executor error",
    JobErrorCode.INVALID_CLOUD_CREDENTIALS: "Invalid or expired cloud credentials",
    JobErrorCode.CLOUD_ACCESS_DENIED: "Access denied by cloud provider",
    JobErrorCode.CLOUD_PROVIDER_ERROR: "Cloud provider API error",
    JobErrorCode.WORKER_LOST: "Job worker was lost or terminated unexpectedly",
}


def _client_error_code(exc: BaseException) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    err = response.get("Error")
    if not isinstance(err, dict):
        return None
    code = err.get("Code")
    return str(code) if code is not None else None


@lru_cache
def _invalid_credential_codes_union() -> frozenset[str]:
    return frozenset().union(*INVALID_CREDENTIALS_ERROR_CODES.values())


@lru_cache
def _access_denied_codes_union() -> frozenset[str]:
    return frozenset().union(*ACCESS_DENIED_ERROR_CODE.values())


@lru_cache
def _worker_lost_exception_types() -> tuple[type[BaseException], ...]:
    types_: list[type[BaseException]] = []
    for mod, name in (
        ("celery.exceptions", "WorkerLostError"),
        ("billiard.exceptions", "WorkerLostError"),
    ):
        try:
            m = __import__(mod, fromlist=[name])
            t = getattr(m, name, None)
            if isinstance(t, type):
                types_.append(t)
        except ImportError:
            continue
    return tuple(types_)
