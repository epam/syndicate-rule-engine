"""
Event assembler handler.
"""

from __future__ import annotations

import heapq
import operator
from collections import defaultdict
from datetime import timedelta
from http import HTTPStatus
from itertools import chain
from typing import TYPE_CHECKING, Generator, MutableMapping, cast

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self

import services.cache as cache
from helpers import adjust_cloud
from helpers.constants import (
    Cloud,
    JobState,
    JobType,
)
from helpers.lambda_response import LambdaOutput, build_response
from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin, SubmitJobToBatchMixin
from models.event import Event
from models.job import Job
from models.setting import Setting
from services import SERVICE_PROVIDER, modular_helpers
from services.clients.batch import (
    BatchClient,
    CeleryJobClient,
)
from services.environment_service import EnvironmentService
from services.event_driven import (
    CloudType,
    EventStoreService,
    RegionNameType,
    RuleNameType,
    TenantNameType,
    VendorKind,
)
from services.job_service import JobService
from services.license_service import LicenseService
from services.ruleset_service import Ruleset, RulesetName, RulesetService
from services.setting_service import (
    EVENT_CURSOR_TIMESTAMP_ATTR,
    SettingsService,
)


if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

    from models.event import EventRecordAttribute
    from services.event_driven.services.rules_service import (
        EventDrivenRulesService,
    )
    from services.platform_service import PlatformService

DEFAULT_NOT_FOUND_RESPONSE = 'No events to assemble and process.'
DEFAULT_UNRESOLVED_RESPONSE = 'Request has run into an unresolvable issue.'

_LOG = get_logger(__name__)


PlatformRegionKey = tuple[str | None, RegionNameType]
RegionRulesMap = dict[PlatformRegionKey, set[RuleNameType]]
TenantRegionRulesMap = dict[TenantNameType, RegionRulesMap]
CloudTenantRegionRulesMap = dict[CloudType, TenantRegionRulesMap]
ByVendorMap = dict[
    VendorKind,
    CloudTenantRegionRulesMap,
]


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
    ) -> None:
        self._event_service = event_service
        self._settings_service = settings_service
        self._tenant_service = tenant_service
        self._platform_service = platform_service
        self._ruleset_service = ruleset_service
        self._environment_service = environment_service
        self._license_service = license_service
        self._job_service = job_service
        self._batch_client = batch_client
        self._tss = tenant_settings_service
        self._ed_rules_service = ed_rules_service
        # this cache does not affect user directly, so we can put custom
        # ttl that does not depend on env
        self._rulesets_cache = cache.factory(ttu=lambda k, v, now: now + 900)

    def _log_cache(self) -> None:
        """
        Just for debug
        """
        attrs = filter(
            lambda name: name.endswith('_cache') and name != '_log_cache',
            dir(self),
        )
        for attr in attrs:
            _LOG.debug(f'{attr}: {getattr(self, attr)}')

    @cache.cachedmethod(operator.attrgetter('_rulesets_cache'))
    def get_ruleset(self, _id: str) -> Ruleset | None:
        """
        Supposed to be used with licensed rule-sets.
        """
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
        )

    def handler(
        self,
        event: MutableMapping | None = None,
    ) -> LambdaOutput:
        self._log_cache()

        # Collect all events since the last execution.
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
        _LOG.info(f'Cursor was obtained: {event_cursor}')
        # Establishes Events: List[$oldest, ... $nearest]
        events = self._obtain_events(since=event_cursor)
        _LOG.debug(
            f'Events obtained (count: {len(events)}): {events[:5]}... (showing first 5)'
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
            'Cursor value of the event assembler has bee updated '
            f'to - {end_event.timestamp}'
        )

        vendor_maps = self.vendor_rule_map(events)
        tenant_jobs: list[tuple[Tenant, Job]] = []
        for vendor, mapping in vendor_maps.items():
            if not mapping:
                _LOG.warning(f'{vendor}`s mapping is empty. Skipping')
                continue
            tenant_jobs.extend(self.handle_vendor(mapping))

        if not tenant_jobs:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Could derive no Jobs. Skipping',
            )

        # here we leave only jobs of tenants for which ed is
        # enabled by license. And also leave only rules that available by
        # that license.
        allowed_jobs: list[Job] = []
        for tenant, job in tenant_jobs:
            _license = self.get_allowed_event_driven_license(tenant)
            if not _license:
                _LOG.debug(
                    f'Tenant {tenant.name} does not have event-driven license. Skipping...'
                )
                continue
            # by here we have license item which allows event-driven for
            # current tenant. Now we only have to restrict the list or rules

            # here just a chain of generators in order to make the
            # total number of iterations by all the rules equal to 1 with
            # small number of lines or code

            # Filter license rulesets by ``Ruleset.cloud`` (see below). For
            # non-platform jobs the event context follows the tenant's primary
            # cloud. For platform-scoped jobs (``platform_id`` set) the work is
            # always Kubernetes cluster events, even if ``tenant.cloud`` is
            # e.g. AWS—using the tenant cloud here would drop all K8s rulesets.
            cloud = (
                adjust_cloud(tenant.cloud)
                if job.platform_id is None
                else Cloud.KUBERNETES.value
            )

            # all rule-sets ids provided by the license
            ids_set = set(_license.ruleset_ids or [])
            ids = iter(ids_set)
            _LOG.debug('Ruleset IDs: %s', ids_set)
            # all rule-sets items
            _rule_sets = (self.get_ruleset(_id) for _id in ids)
            # Only rule-sets whose cloud matches the job scope (see ``cloud`` above)
            _rule_sets = (r for r in _rule_sets if r and r.cloud == cloud)
            # all the rules ids from rule-sets
            # Convert to set[str] to ensure proper typing
            allowed_rules = set(
                str(rule)
                for rule in chain.from_iterable(
                    iter(r.rules) for r in _rule_sets if r
                )
            )
            # all the rules Custom Core's names (without versions)
            # allowed_rules = set(
            #     self._rule_service.i_without_version(raw_rules)
            # )

            _LOG.debug(
                f'Tenant {job.tenant_name} is allowed to use '
                f'such rules: {allowed_rules}'
            )
            # Convert job rules_to_scan back to region_rules_map format
            # for restriction logic
            rules_to_scan = job.rules_to_scan
            _LOG.debug(
                f'Restricting {rules_to_scan} from event to '
                f'the scope of allowed rules'
            )
            restricted_rules_to_scan = self.restrict_rules_to_scan(
                rules_to_scan=rules_to_scan,
                allowed_rules=allowed_rules,
            )
            if not restricted_rules_to_scan:
                _LOG.info('No rules after restricting left. Skipping')
                continue
            elif len(restricted_rules_to_scan) != len(rules_to_scan):
                _LOG.info(
                    f'From {len(rules_to_scan)} rules to scan, '
                    f'{len(restricted_rules_to_scan)} rules left'
                )

            # Update job with restricted rules and license info
            job.rules_to_scan = restricted_rules_to_scan
            job.affected_license = _license.license_key
            # Set rulesets from license
            job.rulesets = [
                RulesetName(_id, None, _license.license_key).to_str()
                for _id in _license.ruleset_ids
            ]
            allowed_jobs.append(job)

        if not allowed_jobs:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='No jobs left after checking licenses',
            )

        # Save all jobs and submit them to batch
        job_ids = []
        for job in allowed_jobs:
            # job_type and ttl are set during creation or update
            # ttl is handled by JobService.create if passed
            self._job_service.save(job)
            job_ids.append(job.id)

        submitted_job_ids = []
        resp = self._submit_jobs_to_batch(
            allowed_jobs,
            as_event_driven=True,
        )
        if resp:
            for job in allowed_jobs:
                self._job_service.update(
                    job=job,
                    batch_job_id=resp.get('jobId'),
                    celery_task_id=resp.get('celeryTaskId'),
                    status=JobState(resp.get('status')),
                )
                submitted_job_ids.append(job.id)
            _LOG.debug(f'Jobs were submitted: {submitted_job_ids}')

        if not submitted_job_ids:
            return build_response(
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
                content='Failed to submit any jobs',
            )

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f'Jobs were submitted: {", ".join(submitted_job_ids)}',
        )

    def handle_vendor(
        self,
        cl_tn_rg_rl_map: CloudTenantRegionRulesMap,
    ) -> Generator[tuple[Tenant, Job], None, None]:
        """
        Generic logic for vendor audit events
        {
            'AZURE': {
                'TEST_TENANT': {
                    (None, 'global'): {'rule1', 'rule2'}
                }
            }
        }
        Uses a normalized map:
        cloud -> tenant_name -> (platform_id, region) -> rules
        For Kubernetes, platform_id is set so each cluster gets its own job.
        """
        for _, tn_rg_rl_map in cl_tn_rg_rl_map.items():
            for tenant_name, rg_rl_map in tn_rg_rl_map.items():
                tenant = self._obtain_tenant(tenant_name)  # only active
                if not tenant:
                    _LOG.warning(f'Tenant {tenant_name} not found. Skipping')
                    continue
                by_platform: dict[str | None, list[tuple[str, set[str]]]] = (
                    defaultdict(list)
                )
                for (plat_id, region_name), rules in rg_rl_map.items():
                    by_platform[plat_id].append((region_name, rules))

                ttl_days = self._environment_service.jobs_time_to_live_days()
                ttl = None
                if ttl_days:
                    ttl = timedelta(days=ttl_days)

                for plat_id, region_rules_pairs in by_platform.items():
                    regions = sorted({reg for reg, _ in region_rules_pairs})
                    all_rules = set(
                        chain.from_iterable(
                            rules for _, rules in region_rules_pairs
                        )
                    )
                    job = self._job_service.create(
                        customer_name=tenant.customer_name,
                        tenant_name=tenant.name,
                        regions=regions,
                        job_type=JobType.REACTIVE,
                        rules_to_scan=list(all_rules),
                        platform_id=plat_id,
                        ttl=ttl,
                        rulesets=[],  # Will be set later based on license
                        affected_license=None,  # Will be set later based on license
                        status=JobState.PENDING,
                    )
                    yield tenant, job

    def _obtain_events(self, since: float | None = None) -> list[Event]:
        """
        Makes N queries. N is a number of partitions (from envs). After that
        merges these N already sorted lists
        :param since:
        :return:
        """
        iters = []
        for partition in range(
            self._environment_service.number_of_partitions_for_events()
        ):
            _LOG.debug(
                f'Making query for {partition} partition since: {since}'
            )
            iters.append(
                self._event_service.get_events(partition, since=since)
            )
        # actually, there is no need to merge all the lists, we just need to
        # first and last timestamp. But this "merge" must be fast
        return list(heapq.merge(*iters, key=lambda e: e.timestamp))

    def vendor_rule_map(
        self,
        events: list[Event],
    ) -> ByVendorMap:
        """
        For each vendor derives rules mapping in its format. The formats of
        each vendor differ
        """
        # Ensure proper nested defaultdict structure to prevent KeyError
        # Structure: result[vendor][cloud][tenant_name][(platform_id, region)] = rules
        result: ByVendorMap = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

        for event in events:
            vendor = cast(VendorKind, event.vendor)
            for event_record in event.events:
                rules = self._ed_rules_service.get_rules(event=event_record)
                if not rules:
                    _LOG.warning(f'No rules found for event: {event_record}')
                    continue
                cloud = event_record.cloud
                tenant_name = self.resolve_tenant_name(event_record)
                if not tenant_name:
                    _LOG.error(
                        'Tenant name cannot be derived for event: %s',
                        event_record,
                    )
                    continue

                # NOTE: If we are handling GCP/Azure/K8s events,
                # we use 'global' as region for assembly (scan semantics).
                if cloud in {
                    Cloud.GOOGLE.value,
                    Cloud.AZURE.value,
                    Cloud.KUBERNETES.value,
                }:
                    region_name = 'global'
                else:
                    region_name = event_record.region_name

                platform_id = getattr(event_record, 'platform_id', None)
                if cloud != Cloud.KUBERNETES.value:
                    platform_id = None
                bucket_key: PlatformRegionKey = (platform_id, region_name)

                # Extend the rules set for the bucket rather than overwriting,
                # handling the case where multiple events may add to the same scope
                if bucket_key not in result[vendor][cloud][tenant_name]:
                    result[vendor][cloud][tenant_name][bucket_key] = set(rules)
                else:
                    result[vendor][cloud][tenant_name][bucket_key].update(
                        rules
                    )

        if not result:
            _LOG.warning('No rules found for any event')

        return result

    def _obtain_tenant(self, name: str) -> Tenant | None:
        _LOG.info(f"Going to retrieve Tenant by '{name}' name.")
        _head = f"Tenant:'{name}'"
        tenant = self._tenant_service.get(name)
        if not modular_helpers.is_tenant_valid(tenant=tenant):
            _LOG.warning(_head + ' is inactive or does not exist')
            return None
        return tenant

    @staticmethod
    def restrict_rules_to_scan(
        rules_to_scan: list[str],
        allowed_rules: set[str],
    ) -> list[str]:
        """
        Restrict rules_to_scan to the scope of allowed rules
        """
        return list(set(rules_to_scan) & set(allowed_rules))

    def resolve_tenant_name(
        self, event_record: EventRecordAttribute
    ) -> str | None:
        if event_record.tenant_name:
            return event_record.tenant_name
        elif (
            event_record.cloud == Cloud.KUBERNETES.value
            and event_record.platform_id
        ):
            platform = self._platform_service.get_nullable(
                hash_key=event_record.platform_id
            )
            if not platform or not platform.tenant_name:
                _LOG.error(
                    'Tenant name cannot be derived for K8s event: platform_id=%s, event=%s',
                    event_record.platform_id,
                    event_record,
                )
                return None
            return platform.tenant_name
        return None

    # NOTE: unused
    # @staticmethod
    # def _optimize_region_rule_map_size(mapping: RegionRulesMap) -> dict[str, list[str]]:
    #     """
    #     On small payload the benefit is not clearly visible. The method can
    #     be skipped as well. Docker can parse both payloads.
    #     input = {
    #         'eu-central-1': {'one', 'two', 'three'},
    #         'eu-west-1': {'one', 'two', 'four'},
    #         'eu-west-2': {'one', 'five'}
    #     }
    #     output = {
    #         'eu-central-1,eu-west-1': ['two'],
    #         'eu-central-1': ['three'],
    #         'eu-central-1,eu-west-1,eu-west-2': ['one'],
    #         'eu-west-1': ['four'],
    #         'eu-west-2': ['five']
    #     }
    #     """

    #     def _rule_to_regions(m: RegionRulesMap) -> dict[str, set[str]]:
    #         ref: dict[str, set[str]] = {}
    #         for region, rules in m.items():
    #             for rule in rules:
    #                 ref.setdefault(rule, set()).add(region)
    #         return ref

    #     def _optimized(m: dict[str, set[str]]) -> dict[str, list[str]]:
    #         ref: dict[str, list[str]] = {}
    #         for rule, regions in m.items():
    #             ref.setdefault(",".join(sorted(regions)), []).append(rule)
    #         return ref

    #     return _optimized(_rule_to_regions(mapping))

    # NOTE: this need only for AWS Batch
    # def _build_common_envs(self) -> dict:
    #     return {
    #         Env.REPORTS_BUCKET_NAME.value: self._environment_service.default_reports_bucket_name(),
    #         Env.STATISTICS_BUCKET_NAME.value: self._environment_service.get_statistics_bucket_name(),
    #         Env.RULESETS_BUCKET_NAME.value: self._environment_service.get_rulesets_bucket_name(),
    #         BatchJobEnv.AWS_REGION.value: self._environment_service.aws_region(),
    #         # BatchJobEnv.JOB_TYPE.value: BatchJobType.REACTIVE.value,
    #         "LOG_LEVEL": self._environment_service.batch_job_log_level(),
    #         # BatchJobEnv.SYSTEM_CUSTOMER_NAME.value: SystemCustomer.get_name(),
    #     }


# probably not used anymore because events are removed by their ttl
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
        """
        Makes N queries. N is a number of partitions (from envs). After that
        merges these N already sorted lists
        :param since:
        :return:
        """
        iters = []
        for partition in range(
            self._environment_service.number_of_partitions_for_events()
        ):
            iters.append(self._event_service.get_events(partition, till=till))
        # actually, there is no need to merge all the lists, we just need to
        # first and last timestamp. But this "merge" must be fast
        # return list(heapq.merge(*iters, key=lambda e: e.timestamp))
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
        _LOG.info(f'Going to remove {_len} old events from SREEvents')
        self._event_service.batch_delete(iter(events))
        message = f'{_len} old events were removed successfully'
        _LOG.info(message)
        return build_response(content=message)
