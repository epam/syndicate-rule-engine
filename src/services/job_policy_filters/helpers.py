import copy
from typing import Any

from services.job_policy_filters.types import (
    APPEND_TYPE,
    PolicyScanEntry,
)


def apply_scan_entry(
    custodian_policy: dict[str, Any],
    entry: PolicyScanEntry,
) -> dict[str, Any]:
    """
    Deep-copy ``custodian_policy`` and overlay ``query`` / ``filters`` from ``entry`` by ``merge_type``.

    When ``merge_type`` is ``append``, ``entry`` query / filters are appended to any
    existing policy query / filters.
    """
    data = copy.deepcopy(custodian_policy)

    entry_query = list(entry.query)
    query_merge = entry.query_merge
    if query_merge == APPEND_TYPE:
        existing = list(custodian_policy.get('query') or [])
        data['query'] = existing + entry_query
    else:
        raise ValueError(f'Unsupported query merge type: {query_merge}')

    entry_filters = [f.to_dict() for f in entry.filters]
    filters_merge = entry.filters_merge
    if filters_merge == APPEND_TYPE:
        existing = list(data.get('filters') or [])
        data['filters'] = existing + entry_filters
    else:
        raise ValueError(f'Unsupported filters merge type: {filters_merge}')

    return data
