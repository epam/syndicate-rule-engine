"""
Event assembler handler: Lambda entry and event fetch; assembly logic lives in
``services.event_driven.assembly``.
"""

from __future__ import annotations

import heapq
import operator
from http import HTTPStatus
from typing import TYPE_CHECKING, MutableMapping

from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self

import services.cache as cache
from helpers.lambda_response import LambdaOutput, build_response
from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin, SubmitJobToBatchMixin
from models.event import Event
from models.setting import Setting
from services import SERVICE_PROVIDER
from services.clients.batch import (
    BatchClient,
    CeleryJobClient,
)
from services.environment_service import EnvironmentService
from services.event_driven import EventStoreService
from services.event_driven.assembly import EventDrivenAssemblyService
from services.job_policy_filters import JobPolicyBundleService
from services.job_service import JobService
from services.license_service import LicenseService
from services.ruleset_service import Ruleset, RulesetService
from services.setting_service import (
    EVENT_CURSOR_TIMESTAMP_ATTR,
    SettingsService,
)

if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

    from services.event_driven.services.rules_service import (
        EventDrivenRulesService,
    )
    from services.platform_service import PlatformService

DEFAULT_NOT_FOUND_RESPONSE = 'No events to assemble and process.'

_LOG = get_logger(__name__)


class EventAssemblerHandler(SubmitJobToBatchMixin, EventDrivenLicenseMixin):
    """Assembles events and processes them."""

    def __init__(
        self,
        event_service: EventStoreService,
        settings_service: SettingsService,
        tenant_service: TenantService,
        platform_service: PlatformService,
        ruleset_service: RulesetService,
        license_service: LicenseService,
        environment_service: EnvironmentService,
        batch_client: BatchClient | CeleryJobClient,
        job_service: JobService,
        tenant_settings_service: TenantSettingsService,
        ed_rules_service: EventDrivenRulesService,
        job_policy_bundle_service: JobPolicyBundleService,
    ) -> None:
        self._event_service = event_service
        self._settings_service = settings_service
        self._ruleset_service = ruleset_service
        self._environment_service = environment_service
        self._license_service = license_service
        self._batch_client = batch_client
        self._tss = tenant_settings_service
        self._rulesets_cache = cache.factory(ttu=lambda k, v, now: now + 900)
        self._assembly_service = EventDrivenAssemblyService(
            tenant_service=tenant_service,
            platform_service=platform_service,
            job_service=job_service,
            environment_service=environment_service,
            ed_rules_service=ed_rules_service,
            get_license=self.get_allowed_event_driven_license,
            get_ruleset=self.get_ruleset,
            submit_event_driven_jobs=lambda jobs: self._submit_jobs_to_batch(
                jobs,
                as_event_driven=True,
            ),
        )

    def _log_cache(self) -> None:
        """Just for debug."""
        attrs = filter(
            lambda name: name.endswith('_cache') and name != '_log_cache',
            dir(self),
        )
        for attr in attrs:
            _LOG.debug('%s: %s', attr, getattr(self, attr))

    @cache.cachedmethod(operator.attrgetter('_rulesets_cache'))
    def get_ruleset(self, _id: str) -> Ruleset | None:
        """Supposed to be used with licensed rule-sets."""
        return self._ruleset_service.get_licensed(_id)

    @classmethod
    def instantiate(cls) -> Self:
        return cls(
            event_service=SERVICE_PROVIDER.event_service,
            settings_service=SERVICE_PROVIDER.settings_service,
            tenant_service=SERVICE_PROVIDER.modular_client.tenant_service(),
            platform_service=SERVICE_PROVIDER.platform_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            license_service=SERVICE_PROVIDER.license_service,
            environment_service=SERVICE_PROVIDER.environment_service,
            batch_client=SERVICE_PROVIDER.batch,
            job_service=SERVICE_PROVIDER.job_service,
            tenant_settings_service=SERVICE_PROVIDER.modular_client.tenant_settings_service(),
            ed_rules_service=SERVICE_PROVIDER.ed_rules_service,
            job_policy_bundle_service=SERVICE_PROVIDER.job_policy_bundle_service,
        )

    def handler(
        self,
        event: MutableMapping | None = None,
    ) -> LambdaOutput:
        self._log_cache()

        _LOG.info('Going to obtain cursor value of the event assembler.')
        event_cursor = None
        config = self._settings_service.get_event_assembler_configuration()
        if isinstance(config, dict) and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
        elif (
            isinstance(config, Setting)
            and EVENT_CURSOR_TIMESTAMP_ATTR in config.value
        ):
            event_cursor = float(config.value[EVENT_CURSOR_TIMESTAMP_ATTR])
        _LOG.info('Cursor was obtained: %s', event_cursor)
        events = self._obtain_events(since=event_cursor)
        _LOG.debug(
            'Events obtained (count: %s): %s... (showing first 5)',
            len(events),
            events[:5],
        )

        if not events:
            _LOG.info('No events have been collected.')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=DEFAULT_NOT_FOUND_RESPONSE,
            )

        end_event = events[-1]
        config = self._settings_service.create_event_assembler_configuration(
            cursor=end_event.timestamp
        )
        self._settings_service.save(setting=config)
        _LOG.info(
            'Cursor value of the event assembler has bee updated to - %s',
            end_event.timestamp,
        )

        result = self._assembly_service.run(events)
        return build_response(code=result.status, content=result.body)

    def _obtain_events(self, since: float | None = None) -> list[Event]:
        """
        Makes N queries. N is a number of partitions (from envs). After that
        merges these N already sorted lists.
        """
        iters = []
        for partition in range(
            self._environment_service.number_of_partitions_for_events()
        ):
            _LOG.debug(
                'Making query for %s partition since: %s',
                partition,
                since,
            )
            iters.append(
                self._event_service.get_events(partition, since=since)
            )
        return list(heapq.merge(*iters, key=lambda e: e.timestamp))


class EventRemoverHandler:
    def __init__(
        self,
        settings_service: SettingsService,
        event_service: EventStoreService,
        environment_service: EnvironmentService,
    ):
        self._settings_service = settings_service
        self._event_service = event_service
        self._environment_service = environment_service

    @classmethod
    def instantiate(cls) -> Self:
        return cls(
            settings_service=SERVICE_PROVIDER.settings_service,
            event_service=SERVICE_PROVIDER.event_service,
            environment_service=SERVICE_PROVIDER.environment_service,
        )

    def _obtain_events(self, till: float) -> list[Event]:
        iters = []
        for partition in range(
            self._environment_service.number_of_partitions_for_events()
        ):
            iters.append(self._event_service.get_events(partition, till=till))
        from itertools import chain

        return list(chain.from_iterable(iters))

    def handler(
        self,
        event: MutableMapping | None = None,
    ) -> LambdaOutput:
        _LOG.info('Going to procure cursor value of the event assembler.')
        event_cursor = None
        config = self._settings_service.get_event_assembler_configuration()
        if config and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
        if not event_cursor:
            return build_response(
                content='Event cursor has not been initialized yet. '
                'No events to clear.'
            )

        events = self._obtain_events(till=event_cursor)
        _len = len(events)

        if _len == 0:
            _LOG.info('No events have been collected.')
            return build_response(
                content=f'No events till {event_cursor} exist in DB'
            )
        _LOG.info('Going to remove %s old events from SREEvents', _len)
        self._event_service.batch_delete(iter(events))
        message = f'{_len} old events were removed successfully'
        _LOG.info(message)
        return build_response(content=message)
