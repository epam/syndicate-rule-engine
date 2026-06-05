"""
This module contains custom execution modes for cloudcustodian.

It is used to implement SRE-side event-driven execution mode for cloudcustodian.
"""

from typing import Any

from .aws_event_driven import SreAwsEventDrivenMode
from .constants import SRE_AWS_EVENT_DRIVEN_MODE
from services.metadata import Metadata
from c7n.policy import Policy
from services.event_driven.events import rule_event_driven_scope

__all__ = ('SreAwsEventDrivenMode',)


def inject_resource_scoped_modes(
    policies: list[dict[str, Any]],
    metadata: Metadata,
) -> None:
    """Add ``sre-aws-event-driven`` mode from LM metadata when YAML has no mode."""
    for policy in policies:
        if policy.get('mode'):
            continue
        name = policy.get('name')
        if not name:
            continue
        meta = metadata.rule(name)
        if rule_event_driven_scope(meta) != 'with_resource_ids':
            continue
        events = meta.events
        if not isinstance(events, list):
            continue
        policy['mode'] = {
            'type': SRE_AWS_EVENT_DRIVEN_MODE,
            'events': [dict(e) for e in events],
        }


def filter_events_for_policy_mode(
    events: list[dict[str, Any]],
    mode: dict[str, Any],
) -> list[dict[str, Any]]:
    """Keep job events matching policy mode ``events`` subscriptions."""
    subscriptions = mode.get('events') or []
    if not subscriptions:
        return events
    matched: list[dict[str, Any]] = []
    for rec in _event_records(events):
        event_name = rec.get('event_name') or ''
        source_name = rec.get('source_name') or ''
        for sub in subscriptions:
            if event_name == sub.get('event') and source_name == sub.get(
                'source'
            ):
                matched.append(rec)
                break
    return matched


def is_resource_scoped_policy(policy: Policy) -> bool:
    mode = policy.data.get('mode') or {}
    return mode.get('type') == SRE_AWS_EVENT_DRIVEN_MODE


def _event_records(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return events
