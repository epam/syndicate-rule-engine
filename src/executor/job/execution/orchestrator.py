"""
Main job orchestration. Standard and reactive jobs use separate entry points.
"""

from __future__ import annotations

from itertools import chain

from executor.job.credentials.resolver import (
    get_job_credentials,
    get_platform_credentials,
    get_rules_to_exclude,
    get_tenant_credentials,
)
from executor.job.execution.context import JobExecutionContext
from executor.job.execution.job_execution_core import (
    JobScanOptions,
    execute_job_region_scan,
    finalize_job_scan,
)
from executor.job.integration.license_manager import post_lm_job
from executor.job.job_failure import JobErrorCode, JobFailure
from executor.job.policies.filter import (
    filter_policies,
    skip_duplicated_policies,
)
from executor.job.rulesets.resolver import resolve_job_rulesets
from executor.job.scan.progress import all_scan_regions
from executor.job.types import JobExecutionError
from helpers.constants import (
    K8S_RBAC_RESOURCE_TYPES,
    Cloud,
    Env,
)
from helpers.log_helper import get_logger
from services import SP
from services.job_policy_filters.types import BundleFilters
from services.k8s_rbac import FINDING_TYPES as K8S_RBAC_FINDING_TYPES
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.ruleset_service import RulesetName

_LOG = get_logger(__name__)


def _prepare_job_scan(
    ctx: JobExecutionContext,
) -> tuple[
    Cloud,
    dict[str, str],
    TenantReportsBucketKeysBuilder | PlatformReportsBucketKeysBuilder,
    list,
    set[str],
]:
    cloud = ctx.cloud()
    job = ctx.job

    names = []
    lists = []
    for name, lst in resolve_job_rulesets(
        job.customer_name, map(RulesetName, job.rulesets)
    ):
        names.append(name)
        lists.append(lst)

    SP.job_service.update(job, rulesets=[r.to_str() for r in names])

    if job.affected_license:
        _LOG.info('The job is licensed. Making post job request to lm')
        posted = post_lm_job(job)
        ctx.set_lm_job_posted(posted)

    if pl := ctx.platform:
        credentials = get_platform_credentials(job, pl)
        keys_builder = PlatformReportsBucketKeysBuilder(pl)
    else:
        credentials = get_job_credentials(
            job, cloud
        ) or get_tenant_credentials(ctx.tenant)
        keys_builder = TenantReportsBucketKeysBuilder(ctx.tenant)

    if credentials is None:
        raise JobExecutionError(
            JobFailure.standard(JobErrorCode.NO_CREDENTIALS)
        )

    credentials = {str(k): str(v) for k, v in credentials.items() if v}

    policies = list(
        skip_duplicated_policies(
            ctx=ctx,
            it=filter_policies(
                it=chain.from_iterable(lists),
                keep=set(job.rules_to_scan),
                exclude=get_rules_to_exclude(ctx.tenant),
            ),
        )
    )
    _LOG.info('Policies are collected: %d', len(policies))
    regions = all_scan_regions(job)

    return cloud, credentials, keys_builder, policies, regions


def has_rbac_policies(policies: list[dict]) -> bool:
    """
    Tells whether the job scans Roles, ClusterRoles or their bindings, i.e.
    whether its results can be used by the RBAC report
    """
    for policy in policies:
        resource = str(policy.get('resource') or '')
        if not resource.startswith('k8s.'):
            resource = f'k8s.{resource}'
        if resource in K8S_RBAC_FINDING_TYPES:
            return True
    return False


def collect_k8s_rbac_snapshot(
    ctx: JobExecutionContext, warnings: list[str]
) -> bool:
    """
    Collects the RBAC inventory snapshot of this scan. Returns whether the
    snapshot is complete. Collection problems are warnings and do not fail
    the job.
    """
    platform = ctx.platform
    if platform is None:
        return False
    if not Env.ENABLE_CUSTOM_CC_PLUGINS.is_set():
        warnings.append(
            'RBAC inventory refresh skipped: custom Custodian plugins are '
            f'disabled ({Env.ENABLE_CUSTOM_CC_PLUGINS.value} is not set)'
        )
        return False
    from executor.resource_collector.collector import (
        CustodianResourceCollector,
    )
    from services.k8s_rbac_snapshot import RbacSnapshotWriter

    writer = RbacSnapshotWriter(
        s3_client=SP.s3,
        bucket=SP.environment_service.default_reports_bucket_name(),
        platform=platform,
        job=ctx.job,
    )
    _LOG.info('Collecting RBAC snapshot for platform %s', platform.id)
    try:
        saved = CustodianResourceCollector.build().collect_platform(
            tenant=ctx.tenant,
            platform=platform,
            resource_types=K8S_RBAC_RESOURCE_TYPES,
            on_resources=writer.write,
        )
        manifest = writer.finish()
        if manifest['complete'] is not True:
            warnings.append(
                'RBAC inventory snapshot is incomplete: roles or role '
                'bindings could not be collected'
            )
            return False
        if missing := writer.missing_optional_types:
            warnings.append(
                'RBAC inventory snapshot has no '
                f'{", ".join(missing)}: pods and workloads of service '
                'accounts may be missing in the RBAC report'
            )
        _LOG.info('RBAC snapshot collected: %d resources', saved)
        return True
    except Exception:
        _LOG.exception('Failed to collect RBAC snapshot of %s', platform.id)
        warnings.append('Could not collect RBAC inventory snapshot')
        return False


def run_standard_job(ctx: JobExecutionContext) -> None:
    """Standard and scheduled jobs: regional pull scan only."""
    cloud, credentials, keys_builder, policies, regions = _prepare_job_scan(
        ctx
    )

    successful, warnings, failed, completed_regions = execute_job_region_scan(
        ctx,
        policies=policies,
        credentials=credentials,
        keys_builder=keys_builder,
        cloud=cloud,
        regions=regions,
    )
    rbac_snapshot_ready = (
        ctx.platform is not None
        and has_rbac_policies(policies)
        and collect_k8s_rbac_snapshot(ctx, warnings)
    )

    finalize_job_scan(
        ctx,
        keys_builder=keys_builder,
        cloud=cloud,
        credentials=credentials,
        regions=regions,
        successful=successful,
        warnings=warnings,
        failed=failed,
        completed_regions=completed_regions,
        rbac_snapshot_ready=rbac_snapshot_ready,
    )
    _LOG.info('Job %r has ended (type=%r)', ctx.job.id, ctx.job.job_type)


def run_reactive_job(ctx: JobExecutionContext) -> None:
    """Reactive jobs: optional K8s bundle, job events push mode, report delivery."""
    cloud, credentials, keys_builder, policies, regions = _prepare_job_scan(
        ctx
    )
    job = ctx.job

    if job.affected_license:
        license = SP.license_service.get_nullable(job.affected_license)
        if license and license.event_driven.get('active'):
            from ..policies.modes import (
                inject_resource_scoped_modes,
            )

            metadata = SP.metadata_provider.get(license)
            inject_resource_scoped_modes(policies, metadata)
        else:
            msg = (
                f'Event-driven is not valid for license {job.affected_license}'
            )
            _LOG.info(msg)
            raise JobExecutionError(
                JobFailure.standard(
                    JobErrorCode.NO_VALID_LICENSE,
                    detail=msg,
                )
            )

    policy_bundle: BundleFilters | None = None
    if cloud is Cloud.KUBERNETES and ctx.platform is not None:
        policy_bundle = SP.job_policy_bundle_service.load_bundle(
            platform=ctx.platform,
            job=job,
        )
        if policy_bundle:
            _LOG.info(
                'Loaded policy filters bundle with %d policy key(s) for job %s',
                len(policy_bundle),
                job.id,
            )

    if ctx.platform is not None:
        rule_events = SP.job_events_service.load_all_rule_events(
            platform=ctx.platform,
            job=job,
        )
    else:
        rule_events = SP.job_events_service.load_all_rule_events(
            tenant=ctx.tenant,
            job=job,
        )

    if rule_events:
        _LOG.info(
            'Loaded job events for %d policy key(s) for job %s',
            len(rule_events),
            job.id,
        )

    scan_options = JobScanOptions(
        policy_bundle=policy_bundle,
        rule_events=rule_events or None,
    )

    successful, warnings, failed, completed_regions = execute_job_region_scan(
        ctx,
        policies=policies,
        credentials=credentials,
        keys_builder=keys_builder,
        cloud=cloud,
        scan_options=scan_options,
        regions=regions,
    )
    finalize_job_scan(
        ctx,
        keys_builder=keys_builder,
        cloud=cloud,
        credentials=credentials,
        regions=regions,
        successful=successful,
        warnings=warnings,
        failed=failed,
        completed_regions=completed_regions,
    )

    try:
        SP.report_delivery_service.notify_job_completed(
            job=job,
            tenant=ctx.tenant,
        )
    except Exception:
        _LOG.exception(
            'Report delivery notification failed for job %s', job.id
        )

    _LOG.info('Job %r has ended (type=%r)', job.id, job.job_type)
