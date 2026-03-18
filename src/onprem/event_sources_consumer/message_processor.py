"""
Processes messages from event sources and ingests them via EventIngestService.
Vendor is auto-detected per event (queue not bound to vendor).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR
from helpers.log_helper import get_logger
from models.event import EventRecordAttribute

from .connectors import Message

if TYPE_CHECKING:
    from services.event_driven import (
        EventDrivenRulesService,
        EventIngestService,
    )

_LOG = get_logger(__name__)


def process_message(
    message: Message,
    event_ingest_service: EventIngestService,
    ed_rules_service: EventDrivenRulesService,
) -> None:
    """
    Process a single message: normalize to events, auto-detect vendor per event,
    filter by rules, ingest (grouped by vendor).
    """
    body = message.body
    if isinstance(body, (str, bytes)):
        try:
            body = (
                json.loads(body)
                if isinstance(body, str)
                else json.loads(body.decode())
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            _LOG.warning("Could not parse message body as JSON: %s", e)
            raise
    events = _normalize_to_events(body)
    if not events:
        _LOG.debug("No events to process in message %s", message.message_id)
        return
    by_vendor: dict[str, list[dict]] = {}
    for raw_event in events:
        detected = _detect_vendor_and_adapt(raw_event)
        if detected is None:
            continue
        vendor, event_attr = detected
        rules = ed_rules_service.get_rules(event_attr)
        if rules:
            by_vendor.setdefault(vendor, []).append(raw_event)
    for vendor, processable_raw in by_vendor.items():
        result = event_ingest_service.ingest(
            vendor=vendor, events=processable_raw
        )
        _LOG.debug(
            "Ingested %s %s events from message %s (received=%s, saved=%s)",
            len(processable_raw),
            vendor,
            message.message_id,
            result.received,
            result.saved,
        )


def _detect_vendor_and_adapt(
    raw_event: dict,
) -> tuple[str, EventRecordAttribute] | None:
    """
    Try AWS and Maestro adapters; return (vendor, event_attr) or None.
    """
    from services.event_driven.adapters import EventRecordsAdapter

    for vendor in (AWS_VENDOR, MAESTRO_VENDOR):
        adapter = EventRecordsAdapter(vendor=vendor, events=[raw_event])
        event_attr = adapter.adapt_single(raw_event)
        if event_attr is not None:
            return (vendor, event_attr)
    return None


def _normalize_to_events(body: dict | list) -> list[dict]:
    """Normalize message body to a list of event dicts."""
    if isinstance(body, list):
        return [e for e in body if isinstance(e, dict)]
    if isinstance(body, dict):
        if "events" in body:
            events = body.get("events", [])
            return [e for e in events if isinstance(e, dict)]
        if "detail" in body:
            return [body]
        return [body]
    return []
