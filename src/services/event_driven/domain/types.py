"""Shared domain types for event-driven flow."""

from enum import Enum

from models.event import EventRecordAttribute

from .models import EventRecordUnion


class VendorKind(str, Enum):
    AWS = 'AWS'
    MAESTRO = 'MAESTRO'
    SRE_K8S_AGENT = 'SRE_K8S_AGENT'
    SRE_K8S_WATCHER = 'SRE_K8S_WATCHER'


# Type aliases
RegionNameType = str
TenantNameType = str
CloudType = str
RuleNameType = str
EventSourceType = str
EventNameType = str

ESourceENameRulesMap = dict[
    EventSourceType, dict[EventNameType, list[RuleNameType]]
]
EventGenericRecord = EventRecordAttribute | EventRecordUnion
K8sServiceRulesMap = dict[str, list[RuleNameType]]
