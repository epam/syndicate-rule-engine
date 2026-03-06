from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant
    from modular_sdk.services.tenant_service import TenantService

_LOG = get_logger(__name__)


class TenantResolver:
    def __init__(self, tenant_service: TenantService) -> None:
        self._tenant_service = tenant_service

    def get_by_account_id(self, account_id: str) -> Tenant | None:
        return next(
            self._tenant_service.i_get_by_acc(
                acc=str(account_id),
                active=True,
                limit=1,
            ),
            None,
        )

    def get_by_name(self, tenant_name: str) -> Tenant | None:
        return self._tenant_service.get(tenant_name)

    def resolve(
        self,
        tenant_name: str | None,
        account_id: str | None,
    ) -> Tenant | None:
        if not tenant_name and not account_id:
            _LOG.warning("Tenant name or account id is required")
            return None

        if tenant_name:
            return self.get_by_name(tenant_name)
        if account_id:
            return self.get_by_account_id(account_id)
        return None
