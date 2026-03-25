from typing_extensions import NotRequired, TypedDict

from executor.helpers.constants import ExecutorError


class ModeDict(TypedDict):
    type: str


class PolicyDict(TypedDict, total=False):
    name: str
    resource: str
    comment: NotRequired[str]
    description: str
    mode: ModeDict


class ExecutorException(Exception):
    def __init__(self, error: ExecutorError):
        self.error = error
