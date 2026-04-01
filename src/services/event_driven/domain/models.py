from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel as PydanticBaseModel
from pydantic import model_validator

from helpers.constants import Cloud
from models.event import EventRecordAttribute


class EventRecord(PydanticBaseModel):
    """
    EventRecord — internal model for validating and converting event records.
    """

    cloud: Cloud
    region_name: str
    source_name: str
    event_name: str
    account_id: str | None = None
    tenant_name: str | None = None
    platform_id: str | None = None

    @model_validator(mode="before")
    def at_least_one_tenant_identifier(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            raise ValueError(f"Expected dict, got {type(values)}")
        account_id = values.get("account_id")
        tenant_name = values.get("tenant_name")
        platform_id = values.get("platform_id")
        if not account_id and not tenant_name and not platform_id:
            raise ValueError(
                "At least one of 'account_id', 'tenant_name', or 'platform_id' "
                "must be provided"
            )
        return values

    def to_event_record_attribute(self) -> EventRecordAttribute:
        return EventRecordAttribute(
            cloud=self.cloud,
            region_name=self.region_name,
            source_name=self.source_name,
            event_name=self.event_name,
            platform_id=self.platform_id,
            account_id=self.account_id,
            tenant_name=self.tenant_name,
        )


@dataclass(frozen=True)
class FailedEvent:
    vendor: str
    event: dict[str, Any]
    error: str
