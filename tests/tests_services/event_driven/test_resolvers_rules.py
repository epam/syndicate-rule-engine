from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from helpers import Version
from helpers.constants import Cloud
from services.event_driven.domain.models import EventRecord
from services.event_driven.resolvers.rules_resolver import RulesResolver


@pytest.fixture
def mapping_provider() -> MagicMock:
    return MagicMock()


@pytest.fixture
def rules_resolver(mapping_provider: MagicMock) -> RulesResolver:
    return RulesResolver(event_mapping_provider=mapping_provider)


def test_cloud_mapping_empty_when_s3_returns_none(
    rules_resolver: RulesResolver,
    mapping_provider: MagicMock,
):
    mapping_provider.get_from_s3.return_value = None

    out = rules_resolver.cloud_mapping(
        Cloud.AWS,
        license_key="lk",
        version=Version("1.0.0"),
    )

    assert out == {}


def test_get_rules_returns_rule_set(
    rules_resolver: RulesResolver,
    mapping_provider: MagicMock,
):
    mapping_provider.get_from_s3.return_value = {
        "ec2.amazonaws.com": {"RunInstances": ["rule-a", "rule-b"]},
    }
    event = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="ec2.amazonaws.com",
        event_name="RunInstances",
        account_id="1",
        tenant_name=None,
    )

    rules = rules_resolver.get_rules(
        event=event,
        license_key="lk",
        version=Version("1.0.0"),
    )

    assert rules == {"rule-a", "rule-b"}


def test_get_rules_empty_when_source_missing(
    rules_resolver: RulesResolver,
    mapping_provider: MagicMock,
):
    mapping_provider.get_from_s3.return_value = {"other.source": {"X": ["r"]}}
    event = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="missing",
        event_name="X",
        account_id="1",
        tenant_name=None,
    )

    assert (
        rules_resolver.get_rules(
            event=event,
            license_key="lk",
            version=Version("1.0.0"),
        )
        == set()
    )
