from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from helpers.constants import Cloud
from services.event_driven.domain.models import EventRecord
from services.event_driven.services.rules_service import EventDrivenRulesService


@pytest.fixture
def tenant_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def license_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mapping_provider() -> MagicMock:
    return MagicMock()


@pytest.fixture
def rules_service(
    license_service: MagicMock,
    mapping_provider: MagicMock,
    tenant_service: MagicMock,
) -> EventDrivenRulesService:
    return EventDrivenRulesService(
        license_service=license_service,
        event_mapping_provider=mapping_provider,
        tenant_service=tenant_service,
    )


@pytest.fixture
def sample_event() -> EventRecord:
    return EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="ec2.amazonaws.com",
        event_name="RunInstances",
        account_id="1",
        tenant_name=None,
    )


def test_get_rules_no_tenant(
    rules_service: EventDrivenRulesService,
    tenant_service: MagicMock,
    sample_event: EventRecord,
):
    tenant_service.i_get_by_acc.return_value = iter([])

    assert rules_service.get_rules(sample_event) is None


def test_get_rules_no_license(
    rules_service: EventDrivenRulesService,
    tenant_service: MagicMock,
    license_service: MagicMock,
    sample_event: EventRecord,
):
    tenant = MagicMock()
    tenant.name = "t1"
    tenant.customer_name = "c1"
    tenant_service.i_get_by_acc.return_value = iter([tenant])
    license_service.get_tenant_license.return_value = None

    assert rules_service.get_rules(sample_event) is None


def test_get_rules_no_matching_rules(
    rules_service: EventDrivenRulesService,
    tenant_service: MagicMock,
    license_service: MagicMock,
    mapping_provider: MagicMock,
    sample_event: EventRecord,
):
    tenant = MagicMock()
    tenant.name = "t1"
    tenant.customer_name = "c1"
    tenant_service.i_get_by_acc.return_value = iter([tenant])

    lic = MagicMock()
    lic.license_key = "lk"
    lic.is_expired.return_value = False
    lic.event_driven = {"active": True}
    license_service.get_tenant_license.return_value = lic
    license_service.is_subject_applicable.return_value = True

    mapping_provider.get_from_s3.return_value = {}

    assert rules_service.get_rules(sample_event) is None


def test_get_rules_happy_path(
    rules_service: EventDrivenRulesService,
    tenant_service: MagicMock,
    license_service: MagicMock,
    mapping_provider: MagicMock,
):
    tenant = MagicMock()
    tenant.name = "t1"
    tenant.customer_name = "c1"
    tenant_service.get.return_value = tenant

    lic = MagicMock()
    lic.license_key = "lk"
    lic.is_expired.return_value = False
    lic.event_driven = {"active": True}
    license_service.get_tenant_license.return_value = lic
    license_service.is_subject_applicable.return_value = True

    mapping_provider.get_from_s3.return_value = {
        "ec2.amazonaws.com": {"RunInstances": ["rule-1"]},
    }

    event_with_name = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="ec2.amazonaws.com",
        event_name="RunInstances",
        account_id=None,
        tenant_name="t1",
    )

    assert rules_service.get_rules(event_with_name) == {"rule-1"}
