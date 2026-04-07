"""Tests for structured job failures and exception classification."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from executor.job.job_failure import (
    JobErrorCode,
    JobFailure,
    classify_exception,
    default_message_for,
    failure_detail_from_exception,
)
from executor.job.types import JobExecutionError


def test_job_failure_to_reason_with_detail() -> None:
    f = JobFailure.standard(JobErrorCode.NO_CREDENTIALS, detail="missing")
    assert (
        f.to_reason() == f"{default_message_for(JobErrorCode.NO_CREDENTIALS)}: missing"
    )


def test_job_failure_to_reason_without_detail() -> None:
    f = JobFailure.standard(JobErrorCode.TIMEOUT)
    assert f.to_reason() == default_message_for(JobErrorCode.TIMEOUT)


def test_job_failure_log_extras() -> None:
    f = JobFailure(
        code=JobErrorCode.INTERNAL,
        message=default_message_for(JobErrorCode.INTERNAL),
        context={"region": "eu-west-1"},
    )
    assert f.log_extras()["job_error_code"] == JobErrorCode.INTERNAL.value
    assert f.log_extras()["region"] == "eu-west-1"


def test_job_failure_standard_lm_exit_code() -> None:
    f = JobFailure.standard(JobErrorCode.LM_DID_NOT_ALLOW, detail="quota")
    assert f.exit_code == 2
    assert "quota" in f.to_reason()


def test_job_execution_error_wraps_failure() -> None:
    inner = JobFailure.standard(JobErrorCode.NO_CREDENTIALS)
    exc = JobExecutionError(inner)
    assert exc.failure.code == JobErrorCode.NO_CREDENTIALS


def test_classify_client_error_invalid_credentials() -> None:
    e = ClientError(
        {"Error": {"Code": "ExpiredToken", "Message": "x"}},
        "AssumeRole",
    )
    f = classify_exception(e)
    assert f.code == JobErrorCode.INVALID_CLOUD_CREDENTIALS
    assert f.detail == "ExpiredToken"


def test_classify_client_error_access_denied() -> None:
    e = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}},
        "GetObject",
    )
    f = classify_exception(e)
    assert f.code == JobErrorCode.CLOUD_ACCESS_DENIED


def test_classify_client_error_other_aws_code() -> None:
    e = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "x"}},
        "HeadBucket",
    )
    f = classify_exception(e)
    assert f.code == JobErrorCode.CLOUD_PROVIDER_ERROR
    assert f.detail == "NoSuchBucket"


def test_classify_generic_exception() -> None:
    f = classify_exception(RuntimeError("boom"))
    assert f.code == JobErrorCode.INTERNAL
    assert "RuntimeError" in (f.detail or "")
    assert "boom" in (f.detail or "")


def test_failure_detail_from_exception_truncates() -> None:
    long_msg = "x" * 2000
    d = failure_detail_from_exception(ValueError(long_msg))
    assert len(d) <= 480 + len("ValueError: ")


@pytest.mark.parametrize(
    "mod_name",
    ("celery.exceptions", "billiard.exceptions"),
)
def test_classify_worker_lost_if_available(mod_name: str) -> None:
    try:
        mod = __import__(mod_name, fromlist=["WorkerLostError"])
        wl = getattr(mod, "WorkerLostError")
    except ImportError:
        pytest.skip(f"{mod_name} not available")
    f = classify_exception(wl())
    assert f.code == JobErrorCode.WORKER_LOST
