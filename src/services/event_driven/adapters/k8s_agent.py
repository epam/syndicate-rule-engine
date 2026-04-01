from __future__ import annotations

from typing import Any

from helpers.constants import Cloud, SRE_K8S_AGENT_VENDOR
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import EventRecord


class K8sAgentEventAdapter(BaseEventAdapter):
    def __init__(self) -> None:
        super().__init__(vendor=SRE_K8S_AGENT_VENDOR)

    def to_event_record(self, event: dict[str, Any]) -> EventRecord:
        type_ = event.get("type")
        reason = event.get("reason")
        platform_id = event.get("platformId")
        if not type_ or not reason or not platform_id:
            raise ValueError("'type', 'reason', and 'platformId' are required")
        return EventRecord(
            cloud=Cloud.KUBERNETES,
            region_name="global",  # K8s has no cloud region; use GLOBAL_REGION
            source_name=str(type_),
            event_name=str(reason),
            platform_id=str(platform_id),
        )
