"""
Utilities for computing rule fingerprints.

A fingerprint is a canonical identifier for a rule based on its content
(resource type + filters). Rules that have the same resource and filters
but different names/IDs/categories will produce the same fingerprint,
enabling deduplication and cross-category tracking.
"""

import hashlib
import json
from typing import Any

from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def _normalize_value(value: Any) -> Any:
    """
    Recursively normalize a value for consistent hashing.
    - dicts: sorted by key
    - lists/tuples: each element normalized (order preserved since filter
      order matters in Cloud Custodian)
    - strings: stripped and lowercased
    - everything else: as-is
    """
    if isinstance(value, dict):
        return {
            k: _normalize_value(v)
            for k, v in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, str):
        return value.strip().lower()
    return value


def _normalize_filters(filters: list[Any]) -> list[Any]:
    """
    Normalize a list of Cloud Custodian filters for consistent hashing.

    Each filter can be either a string (e.g. 'cross-account') or a dict.
    Dicts are recursively normalized. Strings are lowered and stripped.
    """
    result = []
    for f in filters:
        result.append(_normalize_value(f))
    return result


def compute_rule_fingerprint(resource: str, filters: list[Any]) -> str:
    """
    Compute a SHA-256 based fingerprint for a rule.

    The fingerprint is derived from the resource type and the normalized
    filters. Two rules with the same resource type and the same filters
    (regardless of name, category, or description) will produce the same
    fingerprint.

    Returns the first 16 hex characters of the SHA-256 digest.
    """
    canonical = {
        "resource": resource.strip().lower(),
        "filters": _normalize_filters(filters),
    }
    # sort_keys=True for deterministic JSON serialization
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return digest[:16]
