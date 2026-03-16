"""Shared domain types for event-driven flow."""

from typing import Literal

from models.event import EventRecordAttribute

from .models import EventRecord

VendorKind = Literal["AWS", "MAESTRO"]

# Type aliases
RegionNameType = str
TenantNameType = str
CloudType = str
RuleNameType = str
EventSourceType = str
EventNameType = str

ESourceENameRulesMap = dict[EventSourceType, dict[EventNameType, list[RuleNameType]]]
EventGenericRecord = EventRecordAttribute | EventRecord
