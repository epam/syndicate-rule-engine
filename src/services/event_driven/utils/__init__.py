"""Utility helpers for event-driven package."""

import hashlib
import json
from enum import Enum
from typing import Any, Generator, Iterable

from helpers.log_helper import get_logger
from models.event import EventRecordAttribute

_LOG = get_logger(__name__)


def _to_json_compatible(value: Any) -> Any:
    if isinstance(value, EventRecordAttribute):
        return _to_json_compatible(value.as_dict())
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(v) for v in value]
    return value


def digest_(event_record: EventRecordAttribute) -> str:
    payload = _to_json_compatible(event_record)
    digest_source = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(digest_source.encode("utf-8")).hexdigest()


def without_duplicates(
    it: Iterable[EventRecordAttribute],
) -> Generator[EventRecordAttribute, None, int]:
    emitted: set[str] = set()
    for item in it:
        digest = digest_(item)
        if digest in emitted:
            _LOG.warning(f"Skipping duplicate event with digest {digest}: {item}")
            continue
        emitted.add(digest)
        yield item
    return len(emitted)
