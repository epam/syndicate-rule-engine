"""Tests for reactive rule events mode helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from services.event_driven.events.rule_events_mode import (
    resource_scoped_rule_names,
    rule_event_driven_scope,
)
from executor.job.policies.modes.constants import SRE_AWS_EVENT_DRIVEN_MODE
from executor.job.policies.modes import (
    filter_events_for_policy_mode,
    inject_resource_scoped_modes,
    is_resource_scoped_policy,
)
from services.metadata import Metadata, RuleMetadata


def _meta_with_list_events() -> RuleMetadata:
    return RuleMetadata(
        cloud='aws',
        source='ecc',
        category='c',
        service_section='s',
        service='iam',
        article='',
        impact='',
        remediation='',
        events=[
            {
                'event': 'EnableMFADevice',
                'source': 'iam.amazonaws.com',
                'ids': 'x',
            },
        ],
    )


def _meta_with_dict_events() -> RuleMetadata:
    return RuleMetadata(
        cloud='aws',
        source='ecc',
        category='c',
        service_section='s',
        service='iam',
        article='',
        impact='',
        remediation='',
        events={'iam.amazonaws.com': ['EnableMFADevice']},
    )


class TestRuleEventDrivenScope:
    def test_list_is_with_resource_ids(self) -> None:
        assert rule_event_driven_scope(_meta_with_list_events()) == 'with_resource_ids'

    def test_dict_is_names_by_source(self) -> None:
        assert rule_event_driven_scope(_meta_with_dict_events()) == 'names_by_source'

    def test_absent_events(self) -> None:
        meta = _meta_with_dict_events()
        empty = RuleMetadata(
            cloud=meta.cloud,
            source=meta.source,
            category=meta.category,
            service_section=meta.service_section,
            service=meta.service,
            article=meta.article,
            impact=meta.impact,
            remediation=meta.remediation,
            events=None,
        )
        assert rule_event_driven_scope(empty) == 'absent'


class TestInjectResourceScopedModes:
    def test_injects_mode_when_missing(self) -> None:
        policies = [{'name': 'rule-a', 'resource': 'aws.iam-user'}]
        meta = Metadata(rules={'rule-a': _meta_with_list_events()})
        inject_resource_scoped_modes(policies, meta)
        assert policies[0]['mode']['type'] == SRE_AWS_EVENT_DRIVEN_MODE
        assert len(policies[0]['mode']['events']) == 1

    def test_skips_when_mode_present(self) -> None:
        policies = [
            {
                'name': 'rule-a',
                'mode': {'type': 'pull'},
            }
        ]
        meta = Metadata(rules={'rule-a': _meta_with_list_events()})
        inject_resource_scoped_modes(policies, meta)
        assert policies[0]['mode']['type'] == 'pull'


class TestFilterEventsForPolicyMode:
    def test_filters_by_source_and_event(self) -> None:
        events = [
            {
                'event_name': 'EnableMFADevice',
                'source_name': 'iam.amazonaws.com',
                'metadata': {},
            },
            {
                'event_name': 'Other',
                'source_name': 'iam.amazonaws.com',
                'metadata': {},
            },
        ]
        mode = {
            'type': SRE_AWS_EVENT_DRIVEN_MODE,
            'events': [
                {
                    'event': 'EnableMFADevice',
                    'source': 'iam.amazonaws.com',
                    'ids': 'x',
                },
            ],
        }
        matched = filter_events_for_policy_mode(events, mode)
        assert len(matched) == 1
        assert matched[0]['event_name'] == 'EnableMFADevice'


class TestResourceScopedRuleNames:
    def test_lm_list_meta(self) -> None:
        job = SimpleNamespace(rules_to_scan=['rule-a', 'rule-b'])
        meta = Metadata(
            rules={
                'rule-a': _meta_with_list_events(),
                'rule-b': _meta_with_dict_events(),
            }
        )
        names = resource_scoped_rule_names(job, meta)
        assert names == {'rule-a'}

class TestIsResourceScopedPolicy:
    def test_detects_mode_type(self) -> None:
        policy = Mock()
        policy.data = {'mode': {'type': SRE_AWS_EVENT_DRIVEN_MODE}}
        assert is_resource_scoped_policy(policy) is True
