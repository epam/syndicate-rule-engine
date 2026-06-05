"""Per-job mapping of rules to resource refs after assembly (narrow scan input)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from services.event_driven.assembly.resource_refs import ResourceRef
from services.event_driven.domain import RuleNameType


@dataclass(frozen=True, slots=True)
class JobRuleRefs:
    """Immutable rule → refs for one reactive job (possibly empty frozensets)."""

    by_rule: dict[RuleNameType, frozenset[ResourceRef]]

    @classmethod
    def from_mutable_sets(
        cls,
        refs_by_rule: dict[RuleNameType, set[ResourceRef]],
    ) -> JobRuleRefs:
        return cls(
            by_rule={r: frozenset(xs) for r, xs in refs_by_rule.items()},
        )

    def filtered_to_scan(
        self,
        rules_to_scan: Iterable[RuleNameType],
    ) -> JobRuleRefs | None:
        """Keep only keys present in ``rules_to_scan`` (typically post-license filter)."""
        names: Sequence[RuleNameType] = (
            rules_to_scan
            if isinstance(rules_to_scan, list)
            else list(rules_to_scan)
        )
        if not names:
            return None
        return JobRuleRefs(
            by_rule={r: self.by_rule.get(r, frozenset()) for r in names},
        )

    def as_mapping(self) -> Mapping[RuleNameType, frozenset[ResourceRef]]:
        return self.by_rule

    def is_effectively_empty(self) -> bool:
        return not self.by_rule or not any(self.by_rule.values())
