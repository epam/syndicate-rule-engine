"""Tests for list-shaped RuleMetadata.events in EventMappingCollector."""

from __future__ import annotations

from unittest.mock import MagicMock

from helpers.constants import Cloud
from services.event_driven.mappings.collector import EventMappingCollector
from services.metadata import RuleMetadata


def test_list_event_metadata_populates_aws_map() -> None:
    collector = EventMappingCollector(
        s3_client=MagicMock(),
        environment_service=MagicMock(),
    )
    meta = RuleMetadata(
        cloud=Cloud.AWS.value,
        source='ecc',
        category='c',
        service_section='s',
        service='iam',
        article='',
        impact='',
        remediation='',
        events=[
            {'event': 'EnableMFADevice', 'source': 'iam.amazonaws.com', 'ids': 'x'},
            {'event': 'DeleteUser', 'source': 'iam.amazonaws.com', 'ids': 'y'},
        ],
    )
    collector._add_meta('rule-mfa', meta)
    assert collector._aws_events['iam.amazonaws.com']['EnableMFADevice'] == [
        'rule-mfa'
    ]
    assert collector._aws_events['iam.amazonaws.com']['DeleteUser'] == ['rule-mfa']
