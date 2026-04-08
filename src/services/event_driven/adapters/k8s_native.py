"""
Adapter for SRE K8S Agent and SRE K8S Watcher events.

Both vendors use the same payload shape (``type``, ``reason``, ``platformId``);
only the vendor string differs for routing and persistence metadata.
"""

from __future__ import annotations

from typing import Any

from helpers.constants import (
    SRE_K8S_AGENT_VENDOR,
    SRE_K8S_WATCHER_VENDOR,
    Cloud,
)
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import EventRecord


class K8sNativeEventAdapter(BaseEventAdapter):
    """Maps native K8s-shaped events to ``EventRecord`` for a given vendor."""

    def __init__(self, vendor: str) -> None:
        if vendor not in [SRE_K8S_AGENT_VENDOR, SRE_K8S_WATCHER_VENDOR]:
            raise ValueError(f'Invalid vendor: {vendor}')
        super().__init__(vendor=vendor)

    def to_event_record(self, event: dict[str, Any]) -> EventRecord:
        type_ = event.get('type')
        reason = event.get('reason')
        platform_id = event.get('platformId')
        if not type_ or not reason or not platform_id:
            raise ValueError("'type', 'reason', and 'platformId' are required")
        return EventRecord(
            cloud=Cloud.KUBERNETES,
            region_name='global',
            source_name=str(type_),
            event_name=str(reason),
            platform_id=str(platform_id),
        )
