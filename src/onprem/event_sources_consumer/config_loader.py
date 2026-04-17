"""
Loads event source configurations from Application (Mongo).
Supports SQS and K8S source types.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from helpers.constants import CUSTODIAN_EVENT_SOURCE_TYPE
from helpers.log_helper import get_logger
from onprem.event_sources_consumer.constants import CONFIG_LIMIT


if TYPE_CHECKING:
    from modular_sdk.services.application_service import ApplicationService

    from services.platform_service import PlatformService

_LOG = get_logger(__name__)

META_QUEUE_URL = 'queue_url'
META_REGION = 'region'
META_ENABLED = 'enabled'
META_ROLE_ARN = 'role_arn'
META_SOURCE_TYPE = 'source_type'
SOURCE_TYPE_SQS = 'SQS'
SOURCE_TYPE_K8S = 'K8S'


@dataclass(frozen=True)
class EventSourceConfig:
    """Configuration for a single event source."""

    application_id: str
    customer_id: str
    source_type: str
    enabled: bool
    secret: str | None
    # SQS-specific
    queue_url: str | None = None
    region: str | None = None
    role_arn: str | None = None
    # K8S-specific
    platform_id: str | None = None


class EventSourceStrategy:
    def load(self) -> list[EventSourceConfig]:
        raise NotImplementedError


class SqsEventSourceStrategy(EventSourceStrategy):
    def __init__(self, application_service: ApplicationService):
        self._application_service = application_service

    def load(self) -> list[EventSourceConfig]:
        configs: list[EventSourceConfig] = []
        for app in self._application_service.list(
            customer=None,
            _type=CUSTODIAN_EVENT_SOURCE_TYPE,
            deleted=False,
            limit=CONFIG_LIMIT,
        ):
            meta = app.meta.as_dict() if app.meta else {}
            enabled = meta.get(META_ENABLED, True)
            if not enabled:
                _LOG.debug(
                    'Skipping disabled event source %s', app.application_id
                )
                continue

            source_type = meta.get(META_SOURCE_TYPE, SOURCE_TYPE_SQS)
            if source_type != SOURCE_TYPE_SQS:
                _LOG.debug(
                    'Skipping non-SQS event source %s (source_type=%s)',
                    app.application_id,
                    source_type,
                )
                continue

            queue_url = cast(str, meta.get(META_QUEUE_URL))
            region = cast(str, meta.get(META_REGION))
            if not all((queue_url, region)):
                _LOG.warning(
                    'Event source %s missing required meta (queue_url, region). Skipping.',
                    app.application_id,
                )
                continue
            role_arn = meta.get(META_ROLE_ARN)
            role_arn = (
                role_arn.strip()
                if isinstance(role_arn, str) and role_arn.strip()
                else None
            )
            configs.append(
                EventSourceConfig(
                    application_id=app.application_id,
                    customer_id=app.customer_id,
                    source_type=SOURCE_TYPE_SQS,
                    enabled=enabled,
                    secret=app.secret,
                    queue_url=queue_url,
                    region=region,
                    role_arn=role_arn,
                )
            )
        return configs


class K8sPlatformStrategy(EventSourceStrategy):
    def __init__(self, platform_service: PlatformService):
        self._platform_service = platform_service

    def load(self) -> list[EventSourceConfig]:
        configs: list[EventSourceConfig] = []
        # DB-side filtered by application meta.event_driven_enabled=True.
        for platform in self._platform_service.iter_by_event_driven_enabled(
            enabled=True,
            customer=None,
            tenant_name=None,
            limit=CONFIG_LIMIT,
        ):
            self._platform_service.fetch_application(platform)
            app = platform.application
            if not app:
                _LOG.warning(
                    'Platform %s has no linked application. Skipping.',
                    platform.id,
                )
                continue
            configs.append(
                EventSourceConfig(
                    application_id=app.application_id,
                    customer_id=app.customer_id,
                    source_type=SOURCE_TYPE_K8S,
                    enabled=True,
                    secret=app.secret,
                    platform_id=platform.id,
                )
            )
        return configs


def load_event_sources(
    application_service: ApplicationService,
    platform_service: PlatformService | None = None,
) -> list[EventSourceConfig]:
    """
    Load all enabled event sources from configured strategies.
    """
    strategies: list[EventSourceStrategy] = [
        SqsEventSourceStrategy(application_service=application_service)
    ]
    if platform_service:
        strategies.append(
            K8sPlatformStrategy(platform_service=platform_service)
        )

    configs: list[EventSourceConfig] = []
    for strategy in strategies:
        configs.extend(strategy.load())

    if configs and _LOG.isEnabledFor(logging.DEBUG):
        _LOG.debug(
            'Loaded %d event source(s): %s',
            len(configs),
            [c.application_id for c in configs],
        )

    return configs
