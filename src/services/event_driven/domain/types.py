"""Shared domain types for event-driven flow."""

from enum import Enum

from models.event import EventRecordAttribute

from .models import EventRecordUnion


class VendorKind(str, Enum):
    AWS = 'AWS'
    MAESTRO = 'MAESTRO'
    SRE_K8S_AGENT = 'SRE_K8S_AGENT'
    SRE_K8S_WATCHER = 'SRE_K8S_WATCHER'


# Aliases only for nested event-mapping shapes (S3 JSON), not for general annotations
EventSourceType = str
EventNameType = str
RuleNameType = str
CloudType = str
TenantNameType = str

ESourceENameRulesMap = dict[
    EventSourceType, dict[EventNameType, list[RuleNameType]]
]
EventGenericRecord = EventRecordAttribute | EventRecordUnion
K8sServiceRulesMap = dict[EventSourceType, list[RuleNameType]]
