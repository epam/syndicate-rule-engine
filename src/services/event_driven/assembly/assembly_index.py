"""Structured aggregation of rules and resource refs before job materialization."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field

from helpers.constants import Cloud
from models.event import EventRecordAttribute
from services.event_driven.assembly.resource_refs import ResourceRef
from services.event_driven.domain import (
    CloudType,
    TenantNameType,
    VendorKind,
)


@dataclass(frozen=True, slots=True)
class AssemblyBucketKey:
    """Stable bucket for merging events (platform + assembly region)."""

    platform_id: str | None
    region_name: str

    @classmethod
    def from_event_record(
        cls,
        event_record: EventRecordAttribute,
    ) -> AssemblyBucketKey:
        return cls(
            platform_id=cls._assembly_platform_id(
                cloud=event_record.cloud,
                event_record=event_record,
            ),
            region_name=cls._assembly_region(
                cloud=event_record.cloud,
                event_record=event_record,
            ),
        )

    @staticmethod
    def _assembly_region(
        cloud: str,
        event_record: EventRecordAttribute,
    ) -> str:
        if cloud in {
            Cloud.GOOGLE.value,
            Cloud.AZURE.value,
            Cloud.KUBERNETES.value,
        }:
            return 'global'
        return event_record.region_name

    @staticmethod
    def _assembly_platform_id(
        cloud: str,
        event_record: EventRecordAttribute,
    ) -> str | None:
        """K8s cluster id for platform-scoped jobs; other clouds use tenant-wide buckets."""
        if cloud == Cloud.KUBERNETES.value:
            return event_record.platform_id
        return None


@dataclass
class BucketRulesAndRefs:
    """Rules and optional resource refs accumulated for one assembly bucket."""

    rules: set[str] = field(default_factory=set)
    refs_by_rule: dict[str, set[ResourceRef]] = field(default_factory=dict)

    def merge_event_rules(
        self,
        rule_names: set[str],
        resource_ref: ResourceRef | None,
    ) -> None:
        self.rules.update(rule_names)
        if resource_ref is None:
            return
        for r in rule_names:
            self.refs_by_rule.setdefault(r, set()).add(resource_ref)


# Per vendor: cloud → tenant → assembly bucket → merged rules/refs (nested dicts)
CloudAssemblyMap = dict[
    CloudType,
    dict[TenantNameType, dict[AssemblyBucketKey, BucketRulesAndRefs]],
]

_VendorNested = dict[VendorKind, CloudAssemblyMap]


class VendorRuleIndex:
    """
    In-memory index of rules and resource refs while scanning events.

    Adding a new cloud is mostly: plug a :class:`ResourceRefExtractionStrategy`
    and matching policy-bundle strategy; this structure stays the same.
    """

    __slots__ = ('_root',)

    def __init__(self) -> None:
        self._root: _VendorNested = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

    def merge(
        self,
        *,
        vendor: VendorKind,
        cloud: str,
        tenant_name: str,
        bucket_key: AssemblyBucketKey,
        rule_names: set[str],
        resource_ref: ResourceRef | None,
    ) -> None:
        tenant_map = self._root[vendor][cloud][tenant_name]
        bucket = tenant_map.get(bucket_key)
        if bucket is None:
            bucket = BucketRulesAndRefs()
            tenant_map[bucket_key] = bucket
        bucket.merge_event_rules(rule_names, resource_ref)

    def iter_vendors(self) -> Iterator[VendorKind]:
        """Vendors present in the index (insertion order)."""
        yield from self._root

    def cloud_assembly_map(self, vendor: VendorKind) -> CloudAssemblyMap:
        """
        Nested map for one vendor (all clouds, then tenant, then bucket).

        Returns a shallow copy of the top-level vendor slice; inner tenant and
        bucket maps are the same objects as in the index—do not mutate them.
        """
        return dict(self._root.get(vendor, {}))

    def is_empty(self) -> bool:
        return not self._root
