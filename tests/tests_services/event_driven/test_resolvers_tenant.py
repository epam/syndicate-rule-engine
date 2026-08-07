from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from services.event_driven.resolvers.tenant_resolver import TenantResolver


@pytest.fixture
def tenant_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def resolver(tenant_service: MagicMock) -> TenantResolver:
    return TenantResolver(tenant_service=tenant_service)


def test_resolve_returns_none_when_both_missing(resolver: TenantResolver):
    assert resolver.resolve(tenant_name=None, account_id=None) is None


def test_resolve_prefers_tenant_name(
    resolver: TenantResolver,
    tenant_service: MagicMock,
):
    tenant = object()
    tenant_service.get.return_value = tenant

    out = resolver.resolve(tenant_name="t1", account_id="123")

    tenant_service.get.assert_called_once_with("t1")
    tenant_service.i_get_by_acc.assert_not_called()
    assert out is tenant


def test_resolve_by_account_id(
    resolver: TenantResolver,
    tenant_service: MagicMock,
):
    tenant = object()
    tenant_service.i_get_by_acc.return_value = iter([tenant])

    out = resolver.resolve(tenant_name=None, account_id="999")

    tenant_service.i_get_by_acc.assert_called_once_with(
        acc="999",
        active=True,
        limit=1,
    )
    assert out is tenant
