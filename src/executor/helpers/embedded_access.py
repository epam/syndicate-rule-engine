"""Detect access-denied errors embedded in AWS resource payloads.

Some AWS APIs (notably Lambda GetFunctionConfiguration / ListFunctions)
return partial resource documents with an ``Environment.Error`` block when
the caller lacks ``kms:Decrypt`` on the function's CMK, instead of raising
``ClientError``. Cloud Custodian caches those unfiltered resources before
filters run; this helper reads that cache so the executor can classify the
failure as ACCESS rather than INTERNAL.
"""

from __future__ import annotations

from typing import Any

from c7n.policy import Policy

from executor.helpers.constants import ACCESS_DENIED_ERROR_CODE
from helpers.constants import Cloud
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def find_embedded_env_access_denied(policy: Policy) -> str | None:
    """Return Lambda Environment.Error message if AccessDenied is in c7n cache.

    Inspects unfiltered resources saved by Cloud Custodian before
    ``filter_resources``. Returns the first matching error message, or
    ``None`` when none found / cache unavailable.
    """
    access_codes = ACCESS_DENIED_ERROR_CODE.get(Cloud.AWS, ())
    for resource in _cached_resources(policy):
        message = _env_access_denied_message(resource, access_codes)
        if message:
            return message
    return None


def _cached_resources(policy: Policy) -> list[dict[str, Any]]:
    try:
        rm = policy.resource_manager
        query = rm.source.get_query_params(None)
        cache_key = rm.get_cache_key(query)
        with rm._cache:
            resources = rm._cache.get(cache_key)
    except Exception:
        _LOG.debug(
            'Could not read c7n resource cache for embedded access check',
            exc_info=True,
        )
        return []
    if not isinstance(resources, list):
        return []
    return resources


def _env_access_denied_message(
    resource: dict[str, Any],
    access_codes: set[str] | frozenset[str] | tuple[str, ...],
) -> str | None:
    environment = resource.get('Environment')
    if not isinstance(environment, dict):
        return None
    error = environment.get('Error')
    if not isinstance(error, dict):
        return None
    code = error.get('ErrorCode')
    if code not in access_codes:
        return None
    message = error.get('Message')
    if isinstance(message, str) and message.strip():
        return message
    return code if isinstance(code, str) else None
