from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from helpers.log_helper import get_logger
from services.platform_service import PlatformService

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant
    from modular_sdk.services.tenant_service import TenantService

_LOG = get_logger(__name__)

_DEFAULT_MAX_CACHE_ENTRIES = 512


class TenantResolver:
    """
    Resolves tenants by name, cloud account, or K8s platform id.

    In-process LRU cache (hits only) to cut repeated SDK lookups during
    event batches; bounded to avoid unbounded growth in long-lived workers.
    """

    def __init__(
        self,
        tenant_service: TenantService,
        platform_service: PlatformService,
        *,
        max_cache_entries: int = _DEFAULT_MAX_CACHE_ENTRIES,
    ) -> None:
        self._tenant_service = tenant_service
        self._platform_service = platform_service
        self._max_cache_entries = max(1, max_cache_entries)
        self._cache: OrderedDict[str, Tenant] = OrderedDict()

    def _cache_touch(self, key: str) -> Tenant:
        tenant = self._cache.pop(key)
        self._cache[key] = tenant
        return tenant

    def _cache_put(self, key: str, tenant: Tenant) -> None:
        if key in self._cache:
            self._cache.pop(key)
        self._cache[key] = tenant
        while len(self._cache) > self._max_cache_entries:
            self._cache.popitem(last=False)

    def get_by_account_id(self, account_id: str) -> Tenant | None:
        key = f'a:{account_id}'
        if key in self._cache:
            return self._cache_touch(key)
        tenant = next(
            self._tenant_service.i_get_by_acc(
                acc=str(account_id),
                active=True,
                limit=1,
            ),
            None,
        )
        if tenant is not None:
            self._cache_put(key, tenant)
        return tenant

    def get_by_name(self, tenant_name: str) -> Tenant | None:
        key = f'n:{tenant_name}'
        if key in self._cache:
            return self._cache_touch(key)
        tenant = self._tenant_service.get(tenant_name)
        if tenant is not None:
            self._cache_put(key, tenant)
        return tenant

    def resolve(
        self,
        tenant_name: str | None,
        account_id: str | None,
        platform_id: str | None = None,
    ) -> Tenant | None:
        if not tenant_name and not account_id and not platform_id:
            _LOG.warning('Tenant name, account id, or platform_id is required')
            return None

        if tenant_name:
            return self.get_by_name(tenant_name)
        if platform_id:
            pkey = f'p:{platform_id}'
            if pkey in self._cache:
                return self._cache_touch(pkey)
            platform = self._platform_service.get_nullable(
                hash_key=platform_id,
            )
            if not platform:
                return None
            tenant = self.get_by_name(platform.tenant_name)
            if tenant is not None:
                self._cache_put(pkey, tenant)
            return tenant
        if account_id:
            return self.get_by_account_id(account_id)
        return None
