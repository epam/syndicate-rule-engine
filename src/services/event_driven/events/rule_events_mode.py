"""Classify rules and events for reactive execution mode."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal


from services.metadata import Metadata, RuleMetadata

if TYPE_CHECKING:
    from models.job import Job

RuleEventDrivenScope = Literal['with_resource_ids', 'names_by_source', 'absent']


def rule_event_driven_scope(meta: RuleMetadata) -> RuleEventDrivenScope:
    if not meta.events:
        return 'absent'
    if isinstance(meta.events, list):
        return 'with_resource_ids'
    return 'names_by_source'


def resource_scoped_rule_names(job: Job, metadata: Metadata) -> set[str]:
    """Get rule names that should get per-rule payloads under ``{job_id}/events/``."""
    names: set[str] = set()
    for rule in job.rules_to_scan:
        if rule_event_driven_scope(metadata.rule(rule)) == 'with_resource_ids':
            names.add(rule)
    return names
