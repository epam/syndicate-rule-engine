from __future__ import annotations

import msgspec

from helpers.constants import PolicyErrorType
from executor.job.scan.types import FailedPoliciesMap


class FailedPolicyRowWire(msgspec.Struct):
    region: str
    policy: str
    error_type: str
    message: str | None
    traceback: list[str]


class FailedPoliciesSidecar(msgspec.Struct):
    rows: list[FailedPolicyRowWire]


def encode_failed_policies(failed: FailedPoliciesMap) -> bytes:
    rows = [
        FailedPolicyRowWire(
            region=region,
            policy=policy,
            error_type=et.value,
            message=msg,
            traceback=tb,
        )
        for (region, policy), (et, msg, tb) in failed.items()
    ]
    return msgspec.json.encode(FailedPoliciesSidecar(rows=rows))


def decode_failed_policies(raw: bytes) -> FailedPoliciesMap:
    if not raw:
        return {}
    sidecar = msgspec.json.decode(raw, type=FailedPoliciesSidecar)
    return {
        (r.region, r.policy): (
            PolicyErrorType(r.error_type),
            r.message,
            list(r.traceback),
        )
        for r in sidecar.rows
    }
