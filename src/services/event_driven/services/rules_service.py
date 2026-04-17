from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin
from services.event_driven.domain import (
    EventGenericRecord,
)
from services.event_driven.resolvers import RulesResolver, TenantResolver
from services.metadata import DEFAULT_VERSION
from services.platform_service import PlatformService

if TYPE_CHECKING:
    from modular_sdk.services.tenant_service import TenantService

    from services.event_driven.mappings import S3EventMappingProvider
    from services.license_service import LicenseService


_LOG = get_logger(__name__)


class EventDrivenRulesService(EventDrivenLicenseMixin):
    """
    Rules service.
    """

    def __init__(
        self,
        license_service: LicenseService,
        event_mapping_provider: S3EventMappingProvider,
        tenant_service: TenantService,
        platform_service: PlatformService,
    ) -> None:
        self._license_service = license_service
        self._tenant_resolver = TenantResolver(
            tenant_service=tenant_service,
            platform_service=platform_service,
        )
        self._rules_resolver = RulesResolver(
            event_mapping_provider=event_mapping_provider
        )

    def get_rules(
        self,
        event: EventGenericRecord,
    ) -> set[str] | None:
        tenant_name = event.tenant_name
        account_id = event.account_id
        platform_id = getattr(event, "platform_id", None)

        tenant = self._tenant_resolver.resolve(
            tenant_name=tenant_name,
            account_id=account_id,
            platform_id=platform_id,
        )
        if not tenant:
            _LOG.warning(
                "No tenant found for name: %s, account_id: %s, platform_id: %s",
                tenant_name,
                account_id,
                platform_id,
            )
            return None

        event_driven_license = self.get_allowed_event_driven_license(tenant)
        if not event_driven_license:
            _LOG.warning("No event driven license found for tenant: %s", tenant.name)
            return None

        rules = self._rules_resolver.get_rules(
            event=event,
            license_key=event_driven_license.license_key,
            version=DEFAULT_VERSION,
        )
        if not rules:
            _LOG.warning(f"No rules found for event: {event}")
            return None

        return rules
