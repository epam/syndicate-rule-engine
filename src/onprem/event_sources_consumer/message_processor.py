"""
Processes messages from event sources and ingests via EventIngestService.

Supports the wrapped format documented in docs/event_driven.md:
  {"version": "1.0.0", "vendor": "<VENDOR>", "events": [...]}
as well as raw EventBridge events ({"detail-type": ..., "detail": ...}).

K8S watcher events arrive with an explicit vendor override.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR, SRE_K8S_AGENT_VENDOR
from helpers.log_helper import get_logger
from services.event_driven.domain.constants import (
    EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE,
    EB_DETAIL_TYPE,
    MA_EVENT_METADATA,
    MA_TENANT_NAME,
)

from .connectors import Message


if TYPE_CHECKING:
    from services.event_driven import EventIngestService

_LOG = get_logger(__name__)


class EventMessageProcessor:
    """Normalises queue message bodies and forwards them to ``EventIngestService``."""

    _WRAPPED_VENDOR_KEY = 'vendor'
    _WRAPPED_EVENTS_KEY = 'events'

    def __init__(self, event_ingest_service: EventIngestService) -> None:
        self._event_ingest_service = event_ingest_service

    def process(self, message: Message, vendor: str | None = None) -> None:
        """Parse body, resolve vendor + events, ingest.

        If *vendor* is given (K8S watcher path), body is treated as raw event(s)
        for that vendor. Otherwise the message must match ``docs/event_driven.md``.
        """
        body = self._parse_message_body(message.body)
        resolved_vendor, events = self._resolve_vendor_and_events(
            body, vendor_override=vendor
        )

        if not resolved_vendor:
            _LOG.warning(
                'Could not determine vendor for message %s, skipping',
                message.message_id,
            )
            return
        if not events:
            _LOG.debug(
                'No events to process in message %s', message.message_id
            )
            return

        result = self._event_ingest_service.ingest(
            raw_events=events,
            vendor=resolved_vendor,
        )
        _LOG.debug(
            'Ingested from message %s: received=%d, saved=%d',
            message.message_id,
            result.received,
            result.saved,
        )

    def _parse_message_body(
        self, body: str | bytes | dict | list
    ) -> dict | list:
        """Decode JSON string/bytes into dict or list; pass through already-parsed values."""
        if isinstance(body, dict | list):
            return body
        raw = body if isinstance(body, str) else body.decode()
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            _LOG.warning('Could not parse message body as JSON: %s', e)
            raise

    def _extract_wrapped_vendor_events(
        self, body: dict
    ) -> tuple[str | None, list[dict]]:
        """``{vendor, events: [...]}`` wrapper from event_driven.md."""
        vendor = body.get(self._WRAPPED_VENDOR_KEY)
        events = body.get(self._WRAPPED_EVENTS_KEY)
        if vendor and isinstance(events, list):
            return str(vendor), [e for e in events if isinstance(e, dict)]
        return None, []

    def _detect_raw_vendor(self, body: dict) -> str | None:
        """Best-effort vendor for an unwrapped single-object message."""
        if body.get(EB_DETAIL_TYPE) == EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE:
            return AWS_VENDOR
        if MA_TENANT_NAME in body and MA_EVENT_METADATA in body:
            return MAESTRO_VENDOR
        if 'type' in body and 'reason' in body and 'platformId' in body:
            return SRE_K8S_AGENT_VENDOR
        return None

    def _resolve_vendor_and_events(
        self,
        body: dict | list,
        vendor_override: str | None,
    ) -> tuple[str | None, list[dict]]:
        """
        Resolve (vendor, events list) for ingest.

        If *vendor_override* is set (K8S watch path), *body* is one or more raw event dicts.
        Otherwise apply wrapped format first, then raw vendor heuristics.
        """
        if vendor_override is not None:
            if isinstance(body, dict):
                return vendor_override, [body]
            if isinstance(body, list):
                return vendor_override, [
                    e for e in body if isinstance(e, dict)
                ]
            return vendor_override, []

        if isinstance(body, list):
            return None, []

        if not isinstance(body, dict):
            return None, []

        wrapped_vendor, wrapped_events = self._extract_wrapped_vendor_events(
            body
        )
        if wrapped_vendor and wrapped_events:
            return wrapped_vendor, wrapped_events

        raw_vendor = self._detect_raw_vendor(body)
        if raw_vendor:
            return raw_vendor, [body]

        _LOG.debug('Unrecognised message shape, keys: %s', list(body.keys()))
        return None, []
