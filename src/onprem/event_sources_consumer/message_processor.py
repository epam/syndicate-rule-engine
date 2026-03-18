"""
Processes messages from event sources and ingests via EventIngestService.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

from .connectors import Message

if TYPE_CHECKING:
    from services.event_driven import EventIngestService

_LOG = get_logger(__name__)


def process_message(
    message: Message,
    event_ingest_service: EventIngestService,
) -> None:
    """Parse body, normalize to events, ingest (vendor auto-detected, rules filtered)."""
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
    result = event_ingest_service.ingest(raw_events=events, vendor=None)
    _LOG.debug(
        "Ingested from message %s: received=%s, saved=%s",
        message.message_id,
        result.received,
        result.saved,
    )


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
