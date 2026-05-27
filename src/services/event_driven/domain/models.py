from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic import model_validator

from helpers.constants import Cloud
from models.event import EventRecordAttribute


class _EventRecordCore(PydanticBaseModel):
    """Fields common to every ingested event (routing / rules identity)."""

    region_name: str
    source_name: str
    event_name: str


class EventRecord(_EventRecordCore):
    """
    Public-cloud event (AWS, Azure, Google). Tenant scoping uses account and/or
    tenant identifiers — not ``platform_id`` (that is Kubernetes-only).
    """

    cloud: Literal[Cloud.AWS, Cloud.AZURE, Cloud.GOOGLE, Cloud.GCP]
    account_id: str | None = None
    tenant_name: str | None = None

    @model_validator(mode="before")
    def at_least_one_tenant_identifier(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            raise ValueError(f"Expected dict, got {type(values)}")
        account_id = values.get("account_id")
        tenant_name = values.get("tenant_name")
        if not account_id and not tenant_name:
            raise ValueError(
                "At least one of 'account_id' or 'tenant_name' must be provided"
            )
        return values

    def to_event_record_attribute(self) -> EventRecordAttribute:
        return EventRecordAttribute(
            cloud=self.cloud,
            region_name=self.region_name,
            source_name=self.source_name,
            event_name=self.event_name,
            platform_id=None,
            account_id=self.account_id,
            tenant_name=self.tenant_name,
            metadata=None,
        )


class KubernetesMetadata(PydanticBaseModel):
    """Involved object identity from K8s watcher/agent ingest (stored on ``EventRecord.metadata``)."""

    model_config = ConfigDict(frozen=True)

    resource_uid: str
    kind: str
    name: str
    namespace: str | None = None


class KubernetesEventRecord(_EventRecordCore):
    """
    Kubernetes cluster–scoped event. ``platform_id`` identifies the platform
    (cluster); optional K8s-specific fields live on ``metadata`` (more can be
    added there later without polluting public-cloud records).
    """

    cloud: Literal[Cloud.KUBERNETES]
    platform_id: str
    metadata: KubernetesMetadata

    def to_event_record_attribute(self) -> EventRecordAttribute:
        return EventRecordAttribute(
            cloud=self.cloud,
            region_name=self.region_name,
            source_name=self.source_name,
            event_name=self.event_name,
            platform_id=self.platform_id,
            account_id=None,
            tenant_name=None,
            metadata=self.metadata.model_dump(),
        )


EventRecordUnion: TypeAlias = EventRecord | KubernetesEventRecord


@dataclass(frozen=True)
class FailedEvent:
    vendor: str
    event: dict[str, Any]
    error: str
