"""Orchestrate event-driven assembly: vendor maps, jobs, licenses, bundles, batch."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import timedelta
from http import HTTPStatus
from itertools import chain
from typing import Any, cast

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from helpers import adjust_cloud
from helpers.constants import Cloud, JobState, JobType
from helpers.log_helper import get_logger
from models.event import Event, EventRecordAttribute
from models.job import Job
from services import modular_helpers
from services.environment_service import EnvironmentService
from services.event_driven.assembly.assembly_index import (
    AssemblyBucketKey,
    BucketRulesAndRefs,
    CloudAssemblyMap,
    VendorRuleIndex,
)
from services.event_driven.assembly.job_rule_refs import JobRuleRefs
from services.event_driven.assembly.resource_refs import ResourceRef
from services.event_driven.assembly.strategies import (
    DefaultPolicyBundleStrategyRouter,
    ResourceRefStrategyCatalog,
)
from services.event_driven.domain import (
    RuleNameType,
    TenantNameType,
    VendorKind,
)
from services.event_driven.resolvers.tenant_resolver import TenantResolver
from services.event_driven.services.rules_service import (
    EventDrivenRulesService,
)
from services.job_service import JobService
from services.license_service import License
from services.platform_service import PlatformService
from services.ruleset_service import Ruleset, RulesetName

_LOG = get_logger(__name__)


@dataclass(frozen=True)
class EventAssemblyResult:
    """Outcome of assembling and submitting event-driven jobs (HTTP-oriented)."""

    status: HTTPStatus
    body: str
    submitted_job_ids: tuple[str, ...] = ()


class EventDrivenAssemblyService:
    """
    Build vendor/rule maps from events, materialize jobs, apply licenses,
    persist policy-filter bundles, submit to batch.

    Cloud-specific pieces use strategies (resource refs, policy bundles) and
    small routing helpers so new providers mostly add classes, not branches.
    """

    def __init__(
        self,
        *,
        tenant_service: TenantService,
        platform_service: PlatformService,
        job_service: JobService,
        environment_service: EnvironmentService,
        ed_rules_service: EventDrivenRulesService,
        get_license: Callable[[Tenant], License | None],
        get_ruleset: Callable[[str], Ruleset | None],
        submit_event_driven_jobs: Callable[[list[Job]], Any],
        ref_catalog: ResourceRefStrategyCatalog | None = None,
        bundle_router: DefaultPolicyBundleStrategyRouter | None = None,
    ) -> None:
        self._tenant_service = tenant_service
        self._platform_service = platform_service
        self._job_service = job_service
        self._environment_service = environment_service
        self._ed_rules_service = ed_rules_service
        self._get_license = get_license
        self._get_ruleset = get_ruleset
        self._submit_event_driven_jobs = submit_event_driven_jobs
        self._ref_catalog = ref_catalog or ResourceRefStrategyCatalog.default()
        self._bundle_router = (
            bundle_router or DefaultPolicyBundleStrategyRouter.default()
        )
        self._tenant_resolver = TenantResolver(
            tenant_service=tenant_service,
            platform_service=platform_service,
        )

    def run(self, events: list[Event]) -> EventAssemblyResult:
        """Assume ``events`` is non-empty. Cursor must already be persisted by caller."""
        index = self.build_vendor_rule_index(events)
        if index.is_empty():
            _LOG.warning('No rules found for any event')

        tenant_jobs: list[tuple[Tenant, Job, JobRuleRefs | None]] = []
        for vendor in index.iter_vendors():
            by_cloud = index.cloud_assembly_map(vendor)
            if not by_cloud:
                _LOG.warning("%s's mapping is empty. Skipping", vendor)
                continue
            tenant_jobs.extend(self.materialize_tenant_jobs(by_cloud))

        if not tenant_jobs:
            return EventAssemblyResult(
                status=HTTPStatus.NOT_FOUND,
                body='Could derive no Jobs. Skipping',
            )

        allowed_jobs = self._apply_licenses_and_filter_rule_refs(tenant_jobs)
        if not allowed_jobs:
            return EventAssemblyResult(
                status=HTTPStatus.NOT_FOUND,
                body='No jobs left after checking licenses',
            )

        jobs_only = [j for j, _ in allowed_jobs]
        for job, rule_refs in allowed_jobs:
            self._job_service.save(job)
            strategy = self._bundle_router.strategy_for_job(job)
            strategy.maybe_persist(
                job=job,
                rule_refs=rule_refs,
            )

        submitted_job_ids: list[str] = []
        resp = self._submit_event_driven_jobs(jobs_only)
        if resp:
            for job in jobs_only:
                self._job_service.update(
                    job=job,
                    batch_job_id=resp.get('jobId'),
                    celery_task_id=resp.get('celeryTaskId'),
                    status=JobState(resp.get('status')),
                )
                submitted_job_ids.append(job.id)
            _LOG.debug('Jobs were submitted: %s', submitted_job_ids)

        if not submitted_job_ids:
            return EventAssemblyResult(
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
                body='Failed to submit any jobs',
            )

        return EventAssemblyResult(
            status=HTTPStatus.ACCEPTED,
            body=f'Jobs were submitted: {", ".join(submitted_job_ids)}',
            submitted_job_ids=tuple(submitted_job_ids),
        )

    def build_vendor_rule_index(self, events: list[Event]) -> VendorRuleIndex:
        """Aggregate rules and resource refs per vendor / cloud / tenant / bucket."""
        index = VendorRuleIndex()
        for event in events:
            vendor = cast(VendorKind, event.vendor)
            for event_record in event.events:
                rules = self._ed_rules_service.get_rules(event=event_record)
                if not rules:
                    _LOG.warning('No rules found for event: %s', event_record)
                    continue
                cloud = event_record.cloud
                tenant_name = self.resolve_tenant_name(
                    event_record=event_record,
                )
                if not tenant_name:
                    continue

                bucket_key = AssemblyBucketKey.from_event_record(
                    event_record=event_record,
                )
                ref_strategy = self._ref_catalog.for_cloud(cloud)
                resource_ref = ref_strategy.try_extract(event_record)
                index.merge(
                    vendor=vendor,
                    cloud=cloud,
                    tenant_name=tenant_name,
                    bucket_key=bucket_key,
                    rule_names=set(rules),
                    resource_ref=resource_ref,
                )
        return index

    def materialize_tenant_jobs(
        self,
        by_cloud: CloudAssemblyMap,
    ) -> Generator[tuple[Tenant, Job, JobRuleRefs | None], None, None]:
        """One job per tenant/platform aggregate with merged rules and refs."""
        for _, tn_rg_rl_map in by_cloud.items():
            for tenant_name, rg_rl_map in tn_rg_rl_map.items():
                tenant = self._obtain_tenant(tenant_name)
                if not tenant:
                    _LOG.warning('Tenant %s not found. Skipping', tenant_name)
                    continue
                by_platform: dict[
                    str | None,
                    list[
                        tuple[
                            str,
                            BucketRulesAndRefs,
                        ]
                    ],
                ] = defaultdict(list)
                for bucket_key, bucket in rg_rl_map.items():
                    by_platform[bucket_key.platform_id].append(
                        (bucket_key.region_name, bucket)
                    )

                ttl_days = self._environment_service.jobs_time_to_live_days()
                ttl = None
                if ttl_days:
                    ttl = timedelta(days=ttl_days)

                for plat_id, region_rules_pairs in by_platform.items():
                    regions = sorted({reg for reg, _ in region_rules_pairs})
                    all_rules: set[RuleNameType] = set()
                    merged_rule_to_refs: dict[
                        RuleNameType, set[ResourceRef]
                    ] = defaultdict(set)
                    for _, bucket in region_rules_pairs:
                        all_rules.update(bucket.rules)
                        for rule, refs in bucket.refs_by_rule.items():
                            merged_rule_to_refs[rule].update(refs)
                    job_rule_refs: JobRuleRefs | None = (
                        JobRuleRefs.from_mutable_sets(merged_rule_to_refs)
                        if merged_rule_to_refs
                        else None
                    )
                    job = self._job_service.create(
                        customer_name=tenant.customer_name,
                        tenant_name=tenant.name,
                        regions=regions,
                        job_type=JobType.REACTIVE,
                        rules_to_scan=list(all_rules),
                        platform_id=plat_id,
                        ttl=ttl,
                        rulesets=[],
                        affected_license=None,
                        status=JobState.PENDING,
                    )
                    yield tenant, job, job_rule_refs

    def resolve_tenant_name(
        self,
        event_record: EventRecordAttribute,
    ) -> TenantNameType | None:
        """Derive tenant name from the event, K8s platform, or AWS (etc.) account id."""
        er = event_record
        if er.tenant_name:
            return er.tenant_name

        platform_id = (
            getattr(er, 'platform_id', None)
            if er.cloud == Cloud.KUBERNETES.value
            else None
        )
        account_id = (
            getattr(er, 'account_id', None)
            if er.cloud == Cloud.AWS.value
            else None
        )

        tenant = self._tenant_resolver.resolve(
            tenant_name=None,
            account_id=account_id,
            platform_id=platform_id,
        )
        if not tenant:
            _LOG.warning('Tenant not found. Skipping')
            return None
        return tenant.name

    def _obtain_tenant(self, name: str) -> Tenant | None:
        _LOG.debug("Going to retrieve Tenant by '%s' name.", name)
        _head = f"Tenant:'{name}'"
        tenant = self._tenant_service.get(name)
        if not modular_helpers.is_tenant_valid(tenant=tenant):
            _LOG.warning(_head + ' is inactive or does not exist')
            return None
        return tenant

    def _apply_licenses_and_filter_rule_refs(
        self,
        tenant_jobs: list[tuple[Tenant, Job, JobRuleRefs | None]],
    ) -> list[tuple[Job, JobRuleRefs | None]]:
        """
        Filter by event-driven license and ruleset cloud.

        For platform-scoped jobs (``platform_id`` set) ruleset cloud is always
        Kubernetes, even when ``tenant.cloud`` is a public cloud—otherwise K8s
        rulesets would be dropped.
        """
        allowed_jobs: list[tuple[Job, JobRuleRefs | None]] = []
        for tenant, job, rule_refs in tenant_jobs:
            _license = self._get_license(tenant)
            if not _license:
                _LOG.debug(
                    'Tenant %s does not have event-driven license. Skipping...',
                    tenant.name,
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

            ids_set = set(_license.ruleset_ids or [])
            _LOG.debug('Ruleset IDs: %s', ids_set)
            _rule_sets = (self._get_ruleset(_id) for _id in ids_set)
            _rule_sets = (r for r in _rule_sets if r and r.cloud == cloud)
            allowed_rules = set(
                str(rule)
                for rule in chain.from_iterable(
                    iter(r.rules) for r in _rule_sets if r
                )
            )

            _LOG.debug(
                'Tenant %s is allowed to use such rules: %s',
                job.tenant_name,
                allowed_rules,
            )
            rules_to_scan = job.rules_to_scan
            _LOG.debug(
                'Restricting %s from event to the scope of allowed rules',
                rules_to_scan,
            )
            restricted_rules_to_scan = restrict_rules_to_scan(
                rules_to_scan=rules_to_scan,
                allowed_rules=allowed_rules,
            )
            if not restricted_rules_to_scan:
                _LOG.info('No rules after restricting left. Skipping')
                continue
            if len(restricted_rules_to_scan) != len(rules_to_scan):
                _LOG.info(
                    'From %s rules to scan, %s rules left',
                    len(rules_to_scan),
                    len(restricted_rules_to_scan),
                )

            job.rules_to_scan = restricted_rules_to_scan
            job.affected_license = _license.license_key
            job.rulesets = [
                RulesetName(_id, None, _license.license_key).to_str()
                for _id in _license.ruleset_ids
            ]
            filtered_refs: JobRuleRefs | None = (
                None
                if rule_refs is None
                else rule_refs.filtered_to_scan(restricted_rules_to_scan)
            )
            allowed_jobs.append((job, filtered_refs))

        return allowed_jobs


def restrict_rules_to_scan(
    rules_to_scan: list[str],
    allowed_rules: set[str],
) -> list[str]:
    return list(set(rules_to_scan) & set(allowed_rules))
