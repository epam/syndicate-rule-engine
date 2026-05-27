from typing_extensions import NotRequired, TypedDict

from executor.job.job_failure import JobFailure


class ModeDict(TypedDict):
    type: str


class PolicyDict(TypedDict, total=False):
    name: str
    resource: str
    comment: NotRequired[str]
    description: str
    mode: ModeDict


class JobExecutionError(Exception):
    """Raised when a job should fail with a structured :class:`JobFailure`."""

    def __init__(self, failure: JobFailure, /) -> None:
        self.failure = failure
        super().__init__(failure.to_reason())
