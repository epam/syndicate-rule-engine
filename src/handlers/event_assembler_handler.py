"""
Event assembler handler.
"""

from __future__ import annotations

import heapq
import operator
from datetime import timedelta
from http import HTTPStatus
from itertools import chain
from typing import TYPE_CHECKING, Generator, MutableMapping

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self

import services.cache as cache
from helpers import adjust_cloud
from helpers.constants import (
    AWS_VENDOR,
    MAESTRO_VENDOR,
    JobState,
    JobType,
)
from helpers.lambda_response import LambdaOutput, build_response
from helpers.log_helper import get_logger
from helpers.mixins import EventDrivenLicenseMixin, SubmitJobToBatchMixin
from models.job import Job
from models.setting import Setting
from services import SERVICE_PROVIDER, modular_helpers
from services.clients.batch import (
    BatchClient,
    CeleryJobClient,
)
from services.environment_service import EnvironmentService
from services.event_driven.event_processor_service import (
    AccountRegionRuleMap,
    BaseEventProcessor,
    CloudTenantRegionRulesMap,
    EventBridgeEventProcessor,
    EventProcessorService,
    MaestroEventProcessor,
    RegionRuleMap,
)
from services.event_driven.event_service import Event, EventService
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

DEFAULT_NOT_FOUND_RESPONSE = "No events to assemble and process."
DEFAULT_UNRESOLVED_RESPONSE = "Request has run into an unresolvable issue."

_LOG = get_logger(__name__)


class EventAssemblerHandler(SubmitJobToBatchMixin, EventDrivenLicenseMixin):
    """Assembles events and processes them."""

    def __init__(
        self,
        event_service: EventService,
        settings_service: SettingsService,
        tenant_service: TenantService,
        ruleset_service: RulesetService,
        event_processor_service: EventProcessorService,
        license_service: LicenseService,
        environment_service: EnvironmentService,
        batch_client: BatchClient | CeleryJobClient,
        job_service: JobService,
        tenant_settings_service: TenantSettingsService,
    ):
        self._event_service = event_service
        self._event_processor_service = event_processor_service
        self._settings_service = settings_service
        self._tenant_service = tenant_service
        self._ruleset_service = ruleset_service
        self._environment_service = environment_service
        self._license_service = license_service
        self._job_service = job_service
        self._batch_client = batch_client
        self._tss = tenant_settings_service

        # this cache does not affect user directly, so we can put custom
        # ttl that does not depend on env
        self._rulesets_cache = cache.factory(ttu=lambda k, v, now: now + 900)

    def _log_cache(self) -> None:
        """
        Just for debug
        """
        attrs = filter(
            lambda name: name.endswith("_cache") and name != "_log_cache", dir(self)
        )
        for attr in attrs:
            _LOG.debug(f"{attr}: {getattr(self, attr)}")

    @cache.cachedmethod(operator.attrgetter("_rulesets_cache"))
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
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            event_processor_service=SERVICE_PROVIDER.event_processor_service,
            license_service=SERVICE_PROVIDER.license_service,
            environment_service=SERVICE_PROVIDER.environment_service,
            batch_client=SERVICE_PROVIDER.batch,
            job_service=SERVICE_PROVIDER.job_service,
            tenant_settings_service=SERVICE_PROVIDER.modular_client.tenant_settings_service(),
        )

    def handler(
        self,
        event: MutableMapping | None = None,
    ) -> LambdaOutput:
        self._log_cache()

        # Collect all events since the last execution.
        _LOG.info("Going to obtain cursor value of the event assembler.")
        event_cursor = None
        config = self._settings_service.get_event_assembler_configuration()
        if isinstance(config, dict) and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
        elif (
            isinstance(config, Setting) and EVENT_CURSOR_TIMESTAMP_ATTR in config.value
        ):
            event_cursor = float(config.value[EVENT_CURSOR_TIMESTAMP_ATTR])
        _LOG.info(f"Cursor was obtained: {event_cursor}")
        # Establishes Events: List[$oldest, ... $nearest]
        events = self._obtain_events(since=event_cursor)
        _LOG.debug(
            f"Events obtained (count: {len(events)}): {events[:5]}... (showing first 5)"
        )

        if not events:
            _LOG.info("No events have been collected.")
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=DEFAULT_NOT_FOUND_RESPONSE,
            )

        start_event = events[0]
        end_event = events[-1]
        config = self._settings_service.create_event_assembler_configuration(
            cursor=end_event.timestamp
        )
        self._settings_service.save(setting=config)
        _LOG.info(
            "Cursor value of the event assembler has bee updated "
            f"to - {end_event.timestamp}"
        )

        vendor_maps = self.vendor_rule_map(events)
        tenant_jobs: list[tuple[Tenant, Job]] = []
        for vendor, mapping in vendor_maps.items():
            if not mapping:
                _LOG.warning(f"{vendor}`s mapping is empty. Skipping")
                continue
            # handler must yield tuples (Tenant, Job)
            if vendor == MAESTRO_VENDOR:
                tenant_jobs.extend(self.handle_maestro_vendor(mapping))
            elif vendor == AWS_VENDOR:
                tenant_jobs.extend(self.handle_aws_vendor(mapping))
        if not tenant_jobs:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content="Could derive no Jobs. Skipping",
            )

        # here we leave only jobs of tenants for which ed is
        # enabled by license. And also leave only rules that available by
        # that license.
        allowed_jobs: list[Job] = []
        for tenant, job in tenant_jobs:
            _license = self.get_allowed_event_driven_license(tenant)
            if not _license:
                continue
            # by here we have license item which allows event-driven for
            # current tenant. Now we only have to restrict the list or rules

            # here just a chain of generators in order to make the
            # total number of iterations by all the rules equal to 1 with
            # small number of lines or code

            # all rule-sets ids provided by the license
            ids = iter(set(_license.ruleset_ids or []))
            # all rule-sets items
            _rule_sets = (self.get_ruleset(_id) for _id in ids)
            # Only rule-sets for tenant's cloud
            _rule_sets = filter(
                lambda r: r and r.cloud == adjust_cloud(tenant.cloud), _rule_sets
            )
            # all the rules ids from rule-sets
            # Convert to set[str] to ensure proper typing
            allowed_rules = set(
                str(rule)
                for rule in chain.from_iterable(iter(r.rules) for r in _rule_sets if r)
            )
            # all the rules Custom Core's names (without versions)
            # allowed_rules = set(
            #     self._rule_service.i_without_version(raw_rules)
            # )

            _LOG.debug(
                f"Tenant {job.tenant_name} is allowed to use "
                f"such rules: {allowed_rules}"
            )
            # Convert job rules_to_scan back to region_rules_map format
            # for restriction logic
            rules_to_scan = job.rules_to_scan
            _LOG.debug(
                f"Restricting {rules_to_scan} from event to "
                f"the scope of allowed rules"
            )
            restricted_rules_to_scan = self.restrict_rules_to_scan(
                rules_to_scan=rules_to_scan,
                allowed_rules=allowed_rules,
            )
            if not restricted_rules_to_scan:
                _LOG.info("No rules after restricting left. Skipping")
                continue
            elif len(restricted_rules_to_scan) != len(rules_to_scan):
                _LOG.info(
                    f"From {len(rules_to_scan)} rules to scan, "
                    f"{len(restricted_rules_to_scan)} rules left"
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
                content="No jobs left after checking licenses",
            )

        # Save all jobs and submit them to batch
        job_ids = []
        for job in allowed_jobs:
            # job_type and ttl are set during creation or update
            # ttl is handled by JobService.create if passed
            self._job_service.save(job)
            job_ids.append(job.id)

        submitted_job_ids = []
        resp = self._submit_jobs_to_batch(allowed_jobs)
        if resp:
            for job in allowed_jobs:
                self._job_service.update(
                    job=job,
                    batch_job_id=resp.get("jobId"),
                    celery_task_id=resp.get("celeryTaskId"),
                    status=JobState(resp.get("status")),
                )
                submitted_job_ids.append(job.id)
            _LOG.debug(f"Jobs were submitted: {submitted_job_ids}")

        if not submitted_job_ids:
            return build_response(
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
                content="Failed to submit any jobs",
            )

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f"Jobs were submitted: {', '.join(submitted_job_ids)}",
        )

    def handle_aws_vendor(
        self, cid_rg_rl_map: AccountRegionRuleMap
    ) -> Generator[tuple[Tenant, Job], None, None]:
        """
        cid_rg_rl_map = {
            '12424123423': {
                'eu-central-1': {'rule1', 'rule2'}
            }
        }
        """
        for cid in cid_rg_rl_map:
            tenant = self._obtain_tenant_by_acc(cid)
            if not tenant:
                _LOG.info(f"Tenant {cid} not found. Skipping")
                continue

            # rules to exclude
            accessible_rg_rl_map = self._obtain_tenant_based_region_rule_map(
                tenant=tenant, region_rule_map=cid_rg_rl_map[cid]
            )
            if not accessible_rg_rl_map:
                _LOG.warning(
                    f"No rules within region(s) are accessible to "
                    f"'{tenant.name}' tenant."
                )
                continue

            # Convert region_rule_map to regions and rules_to_scan
            regions = list(accessible_rg_rl_map.keys())
            all_rules = set(chain.from_iterable(accessible_rg_rl_map.values()))

            ttl_days = self._environment_service.jobs_time_to_live_days()
            ttl = None
            if ttl_days:
                ttl = timedelta(days=ttl_days)

            job = self._job_service.create(
                customer_name=tenant.customer_name,
                tenant_name=tenant.name,
                regions=regions,
                job_type=JobType.REACTIVE,
                rules_to_scan=list(all_rules),
                ttl=ttl,
                rulesets=[],  # Will be set later based on license
                affected_license=None,  # Will be set later based on license
                status=JobState.PENDING,
            )
            yield tenant, job

    def handle_maestro_vendor(
        self, cl_tn_rg_rl_map: CloudTenantRegionRulesMap
    ) -> Generator[tuple[Tenant, Job], None, None]:
        """
        Separate logic for maestro audit events
        {
            'AZURE': {
                'TEST_TENANT': {
                    'global': {'rule1', 'rule2'}
                }
            }
        }
        Azure tenants will always contain one mock region. But such a
        structure is kept in case we want to process AWS maestro events
        """
        _LOG.debug("Processing maestro vendor")
        for cloud, tn_rg_rl_map in cl_tn_rg_rl_map.items():
            for tenant_name, rg_rl_map in tn_rg_rl_map.items():
                tenant = self._obtain_tenant(tenant_name)  # only active
                if not tenant:
                    _LOG.warning(f"Tenant {tenant_name} not found. Skipping")
                    continue
                # here we skip restrictions by regions for AZURE because
                # currently I don't know how it's supposed to work

                # Convert region_rule_map to regions and rules_to_scan
                regions = list(rg_rl_map.keys())
                all_rules = set(chain.from_iterable(rg_rl_map.values()))

                ttl_days = self._environment_service.jobs_time_to_live_days()
                ttl = None
                if ttl_days:
                    ttl = timedelta(days=ttl_days)

                job = self._job_service.create(
                    customer_name=tenant.customer_name,
                    tenant_name=tenant.name,
                    regions=regions,
                    job_type=JobType.REACTIVE,
                    rules_to_scan=list(all_rules),
                    ttl=ttl,
                    rulesets=[],  # Will be set later based on license
                    affected_license=None,  # Will be set later based on license
                    credentials_key=None,  # Will be set later based on credentials
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
            _LOG.debug(f"Making query for {partition} partition since: {since}")
            iters.append(self._event_service.get_events(partition, since=since))
        # actually, there is no need to merge all the lists, we just need to
        # first and last timestamp. But this "merge" must be fast
        return list(heapq.merge(*iters, key=lambda e: e.timestamp))

    def vendor_rule_map(self, events: list[Event]) -> dict[str, dict]:
        """
        For each vendor derives rules mapping in its format. The formats of
        each vendor differ
        """
        vendor_processor = {
            MAESTRO_VENDOR: self._event_processor_service.get_processor(MAESTRO_VENDOR),
            AWS_VENDOR: self._event_processor_service.get_processor(AWS_VENDOR),
        }
        for event in events:
            if event.vendor not in vendor_processor:
                _LOG.warning(f"Not known vendor: {event.vendor}. Skipping")
                continue
            _LOG.debug(f"Processing event: {event!r} by vendor: {event.vendor!r}")
            processor: BaseEventProcessor = vendor_processor[event.vendor]
            processor.events.extend(event.events)  # type: ignore[arg-type]
        _LOG.info("All the vendor-processors are initialized with events")
        result = {}

        maestro_proc = vendor_processor[MAESTRO_VENDOR]
        if isinstance(maestro_proc, MaestroEventProcessor):
            it = maestro_proc.without_duplicates(maestro_proc.prepared_events())
            _res = maestro_proc.cloud_tenant_region_rules_map(it)
            if _res:
                result[MAESTRO_VENDOR] = _res

        aws_proc = vendor_processor[AWS_VENDOR]
        if isinstance(aws_proc, EventBridgeEventProcessor):
            it = aws_proc.without_duplicates(aws_proc.prepared_events())
            _res = aws_proc.account_region_rule_map(it)
            if _res:
                result[AWS_VENDOR] = _res
    
        return result

    def _obtain_tenant(self, name: str) -> Tenant | None:
        _LOG.info(f"Going to retrieve Tenant by '{name}' name.")
        _head = f"Tenant:'{name}'"
        tenant = self._tenant_service.get(name)
        if not modular_helpers.is_tenant_valid(tenant=tenant):
            _LOG.warning(_head + " is inactive or does not exist")
            return
        return tenant

    def _obtain_tenant_by_acc(self, acc: str) -> Tenant | None:
        _LOG.info(f"Going to retrieve Tenant by '{acc}' cloud id")
        return next(
            self._tenant_service.i_get_by_acc(acc=str(acc), active=True, limit=1),
            None,
        )

    def _obtain_tenant_based_region_rule_map(
        self, tenant: Tenant, region_rule_map: RegionRuleMap
    ):
        tn = tenant.name
        _LOG.info(
            f"Going to restrict {list(region_rule_map)} regions, "
            f"based on ones accessible to '{tn}' Tenant."
        )
        ref = {}
        # Maestro's regions in tenants have attribute "is_active" ("act").
        # But currently (30.01.2023) they ignore it. They deem all the
        # regions listed in an active tenant to be active as well. So do we
        active_rg = modular_helpers.get_tenant_regions(tenant, self._tss)
        for region in region_rule_map:
            if region not in active_rg:
                _LOG.warning(
                    f"Going to exclude `{region}` region(s) based "
                    f"on `{tn}` tenant inactive region state."
                )
                continue
            ref[region] = region_rule_map[region]
        return ref

    @staticmethod
    def restrict_region_rule_map(
        mapping: RegionRuleMap, allowed_rules: set[str]
    ) -> RegionRuleMap:
        """
        Restrict rules in RegionRuleMap to the scope of allowed rules
        """
        result = {}
        for region, rules in mapping.items():
            intersection = rules & allowed_rules
            if intersection:
                result[region] = intersection
        return result

    @staticmethod
    def restrict_rules_to_scan(
        rules_to_scan: list[str],
        allowed_rules: set[str],
    ) -> list[str]:
        """
        Restrict rules_to_scan to the scope of allowed rules
        """
        return list(set(rules_to_scan) & set(allowed_rules))

    @staticmethod
    def _optimize_region_rule_map_size(mapping: RegionRuleMap) -> dict[str, list[str]]:
        """
        On small payload the benefit is not clearly visible. The method can
        be skipped as well. Docker can parse both payloads.
        input = {
            'eu-central-1': {'one', 'two', 'three'},
            'eu-west-1': {'one', 'two', 'four'},
            'eu-west-2': {'one', 'five'}
        }
        output = {
            'eu-central-1,eu-west-1': ['two'],
            'eu-central-1': ['three'],
            'eu-central-1,eu-west-1,eu-west-2': ['one'],
            'eu-west-1': ['four'],
            'eu-west-2': ['five']
        }
        """

        def _rule_to_regions(m: RegionRuleMap) -> dict[str, set[str]]:
            ref: dict[str, set[str]] = {}
            for region, rules in m.items():
                for rule in rules:
                    ref.setdefault(rule, set()).add(region)
            return ref

        def _optimized(m: dict[str, set[str]]) -> dict[str, list[str]]:
            ref: dict[str, list[str]] = {}
            for rule, regions in m.items():
                ref.setdefault(",".join(sorted(regions)), []).append(rule)
            return ref

        return _optimized(_rule_to_regions(mapping))

    # TODO: this need only for AWS Batch
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
        event_service: EventService,
        environment_service: EnvironmentService,
    ):
        self._settings_service = settings_service
        self._event_service = event_service
        self._environment_service = environment_service

    @classmethod
    def instantiate(cls) -> "EventRemoverHandler":
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
        _LOG.info("Going to procure cursor value of the event assembler.")
        event_cursor = None
        config = self._settings_service.get_event_assembler_configuration()
        if config and EVENT_CURSOR_TIMESTAMP_ATTR in config:
            event_cursor = float(config[EVENT_CURSOR_TIMESTAMP_ATTR])
        if not event_cursor:
            return build_response(
                content="Event cursor has not been initialized yet. "
                "No events to clear."
            )

        events = self._obtain_events(till=event_cursor)
        _len = len(events)

        if _len == 0:
            _LOG.info("No events have been collected.")
            return build_response(content=f"No events till {event_cursor} exist in DB")
        _LOG.info(f"Going to remove {_len} old events from SREEvents")
        self._event_service.batch_delete(iter(events))
        message = f"{_len} old events were removed successfully"
        _LOG.info(message)
        return build_response(content=message)
