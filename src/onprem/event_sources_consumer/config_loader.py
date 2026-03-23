"""
Loads SQS event source configurations from Application (Mongo).
Per-customer: loads event sources for all customers (consumer processes all queues).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from helpers.constants import CUSTODIAN_EVENT_SOURCE_TYPE
from helpers.log_helper import get_logger

from onprem.event_sources_consumer import settings

if TYPE_CHECKING:
    from modular_sdk.services.application_service import ApplicationService

_LOG = get_logger(__name__)

META_QUEUE_URL = "queue_url"
META_REGION = "region"
META_ENABLED = "enabled"
META_ROLE_ARN = "role_arn"


@dataclass(frozen=True)
class EventSourceConfig:
    """Configuration for a single SQS event source (per-customer)."""

    application_id: str
    customer_id: str
    queue_url: str
    region: str
    enabled: bool
    secret: str | None
    role_arn: str | None


def load_event_sources(
    application_service: ApplicationService,
) -> list[EventSourceConfig]:
    """
    Load all enabled SQS event sources from Application (all customers).
    """
    configs: list[EventSourceConfig] = []
    for app in application_service.list(
        customer=None,
        _type=CUSTODIAN_EVENT_SOURCE_TYPE,
        deleted=False,
        limit=settings.CONFIG_LIMIT,
    ):
        meta = app.meta.as_dict() if app.meta else {}
        enabled = meta.get(META_ENABLED, True)
        if not enabled:
            _LOG.debug("Skipping disabled event source %s", app.application_id)
            continue
        queue_url = cast(str, meta.get(META_QUEUE_URL))
        region = cast(str, meta.get(META_REGION))
        if not all((queue_url, region)):
            _LOG.warning(
                "Event source %s missing required meta (queue_url, region). Skipping.",
                app.application_id,
            )
            continue
        role_arn = meta.get(META_ROLE_ARN)
        role_arn = (
            role_arn.strip() if isinstance(role_arn, str) and role_arn.strip()
            else None
        )
        configs.append(
            EventSourceConfig(
                application_id=app.application_id,
                customer_id=app.customer_id,
                queue_url=queue_url,
                region=region,
                enabled=enabled,
                secret=app.secret,
                role_arn=role_arn,
            )
        )
    if configs:
        _LOG.info(
            "Loaded %d event source(s): %s",
            len(configs),
            [c.application_id for c in configs],
        )
    return configs
