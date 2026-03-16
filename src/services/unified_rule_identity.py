"""
UnifiedRuleIdentity service.

Provides a way to track rules that represent the same logical check
across different categories, rulesets, or naming conventions.

It builds an index keyed by fingerprint and allows querying for
canonical IDs, aliases, and merged metadata.
"""
from typing import Iterable

from helpers.log_helper import get_logger
from models.rule import Rule
from services.metadata import Metadata, RuleMetadata, EMPTY_RULE_METADATA

_LOG = get_logger(__name__)


class UnifiedRuleIdentity:
    """
    Maintains an index of rules grouped by fingerprint and provides
    helpers for deduplication, alias resolution, and metadata merging.
    """

    __slots__ = (
        '_fp_to_names',
        '_name_to_fp',
        '_canonical_map',
    )

    def __init__(self) -> None:
        # fingerprint -> set of rule names sharing that fingerprint
        self._fp_to_names: dict[str, set[str]] = {}
        # rule name -> fingerprint
        self._name_to_fp: dict[str, str] = {}
        # fingerprint -> canonical (primary) rule name
        self._canonical_map: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Building the index
    # ------------------------------------------------------------------

    def build_index(self, rules: Iterable[Rule]) -> None:
        """
        Populate the index from an iterable of Rule model instances.
        The first rule encountered for each fingerprint is considered
        the canonical representative.
        """
        for rule in rules:
            fp = rule.fingerprint
            if not fp:
                continue
            name = rule.name
            self._name_to_fp[name] = fp
            names = self._fp_to_names.setdefault(fp, set())
            names.add(name)
            if fp not in self._canonical_map:
                self._canonical_map[fp] = name

    def add_rule(self, name: str, fingerprint: str) -> None:
        """
        Incrementally add a single rule to the index.
        """
        self._name_to_fp[name] = fingerprint
        names = self._fp_to_names.setdefault(fingerprint, set())
        names.add(name)
        if fingerprint not in self._canonical_map:
            self._canonical_map[fingerprint] = name

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_fingerprint(self, rule_name: str) -> str | None:
        """
        Return the fingerprint for the given rule name, or None.
        """
        return self._name_to_fp.get(rule_name)

    def get_canonical_name(self, rule_name: str) -> str:
        """
        Return the canonical (primary) name for the given rule.
        If the rule is not in the index, returns itself.
        """
        fp = self._name_to_fp.get(rule_name)
        if not fp:
            return rule_name
        return self._canonical_map.get(fp, rule_name)

    def get_all_aliases(self, rule_name: str) -> set[str]:
        """
        Return all names (including itself) that share the same fingerprint.
        """
        fp = self._name_to_fp.get(rule_name)
        if not fp:
            return {rule_name}
        return set(self._fp_to_names.get(fp, {rule_name}))

    def is_same_rule(self, name1: str, name2: str) -> bool:
        """
        Check whether two rule names represent the same logical rule.
        """
        fp1 = self._name_to_fp.get(name1)
        fp2 = self._name_to_fp.get(name2)
        if fp1 is None or fp2 is None:
            return name1 == name2
        return fp1 == fp2

    def iter_duplicate_groups(self) -> Iterable[tuple[str, set[str]]]:
        """
        Yield (fingerprint, names) for every group that has more than one
        rule name. Useful for logging / auditing.
        """
        for fp, names in self._fp_to_names.items():
            if len(names) > 1:
                yield fp, names

    def merge_metadata(
        self,
        rule_name: str,
        metadata: Metadata,
    ) -> RuleMetadata:
        """
        Given a rule name, retrieve and merge metadata from all aliases.
        The primary metadata comes from the canonical name; additional
        categories are collected from aliases.
        """
        aliases = self.get_all_aliases(rule_name)
        primary_meta = metadata.rule(rule_name)
        if primary_meta is EMPTY_RULE_METADATA:
            # try other aliases
            for alias in aliases:
                candidate = metadata.rule(alias)
                if candidate is not EMPTY_RULE_METADATA:
                    primary_meta = candidate
                    break

        if primary_meta is EMPTY_RULE_METADATA:
            return primary_meta

        # Collect categories from all aliases
        all_categories: list[str] = []
        seen: set[str] = set()
        for alias in aliases:
            m = metadata.rule(alias)
            if m is EMPTY_RULE_METADATA:
                continue
            cat = m.category
            if cat and cat not in seen:
                all_categories.append(cat)
                seen.add(cat)

        other_aliases = tuple(sorted(aliases - {rule_name}))

        # msgspec frozen structs don't allow attribute assignment so we
        # create a new instance with updated fields.  Because the struct
        # is ``array_like`` we cannot use keyword unpacking of a dict
        # built from the old struct, so we construct positionally.
        # NOTE: the field order must match the struct definition exactly.
        return RuleMetadata(
            source=primary_meta.source,
            category=primary_meta.category,
            service_section=primary_meta.service_section,
            service=primary_meta.service,
            severity=primary_meta.severity,
            report_fields=primary_meta.report_fields,
            periodic=primary_meta.periodic,
            article=primary_meta.article,
            impact=primary_meta.impact,
            remediation=primary_meta.remediation,
            standard=primary_meta.standard,
            mitre=primary_meta.mitre,
            waf=primary_meta.waf,
            remediation_complexity=primary_meta.remediation_complexity,
            deprecation=primary_meta.deprecation,
            categories=tuple(all_categories),
            aliases=other_aliases,
            events=primary_meta.events,
            cloud=primary_meta.cloud,
        )

    # ------------------------------------------------------------------
    # Summary / stats
    # ------------------------------------------------------------------

    @property
    def total_rules(self) -> int:
        return len(self._name_to_fp)

    @property
    def total_unique(self) -> int:
        return len(self._fp_to_names)

    @property
    def total_duplicates(self) -> int:
        return sum(
            1 for names in self._fp_to_names.values() if len(names) > 1
        )

