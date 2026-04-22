from typing import TypeAlias, TypedDict

from helpers.constants import PolicyErrorType

# (region, policy) -> (error_type, message, traceback lines)
FailedPoliciesMap: TypeAlias = dict[
    tuple[str, str], tuple[PolicyErrorType, str | None, list[str]]
]


class ScanCheckpoint(TypedDict):
    checkpoint_version: int
    completed_regions: list[str]
    updated_at: str
