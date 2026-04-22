"""
Main job orchestration. Runs standard job: resolve rulesets, credentials,
execute policies per region, write reports, upload to SIEM.
"""

from typing import cast

import billiard as multiprocessing
from itertools import chain
from pathlib import Path

from modular_sdk.commons.constants import (
    ENV_AZURE_CLIENT_CERTIFICATE_PATH,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
    ENV_KUBECONFIG,
)

from executor.job.execution.publish import finalize_standard_job_reports
from executor.job.job_failure import JobFailure, JobErrorCode
from executor.job.credentials.resolver import (
    get_job_credentials,
    get_platform_credentials,
    get_rules_to_exclude,
    get_tenant_credentials,
)
from executor.job.execution.context import JobExecutionContext
from executor.job.execution.region_executor import (
    job_initializer,
    process_job_concurrent,
    RegionScanResult,
)
from executor.job.integration.license_manager import post_lm_job
from executor.job.policies.filter import (
    filter_policies,
    skip_duplicated_policies,
)
from executor.job.rulesets.resolver import resolve_job_rulesets
from executor.job.types import JobExecutionError
from executor.services.report_service import JobResult
from helpers.constants import GLOBAL_REGION, Cloud
from services.job_policy_filters.types import BundleFilters
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from executor.job.scan import (
    FailedPoliciesMap,
    ScanCheckpoint,
    ScanPartialStore,
    pending_scan_regions,
    scan_checkpoint_from_job,
)
from services import SP
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.ruleset_service import RulesetName

_LOG = get_logger(__name__)


def run_standard_job(ctx: JobExecutionContext):
    """
    Accepts a job item full of data and runs it. Does not use envs to
    get info about the job
    """
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
        _LOG.info("The job is licensed. Making post job request to lm")
        posted = post_lm_job(job)
        ctx.set_lm_job_posted(posted)

    if pl := ctx.platform:
        credentials = get_platform_credentials(job, pl)
        keys_builder = PlatformReportsBucketKeysBuilder(pl)
    else:
        credentials = get_job_credentials(job, cloud) or get_tenant_credentials(
            ctx.tenant
        )
        keys_builder = TenantReportsBucketKeysBuilder(ctx.tenant)

    if credentials is None:
        raise JobExecutionError(JobFailure.standard(JobErrorCode.NO_CREDENTIALS))

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
    _LOG.info(f"Policies are collected: {len(policies)}")
    regions = set(job.regions) | {GLOBAL_REGION}

    policy_bundle: BundleFilters | None = None
    if cloud is Cloud.KUBERNETES and ctx.platform is not None:
        policy_bundle = SP.job_policy_bundle_service.load_bundle(
            platform=ctx.platform,
            job=job,
        )
        if policy_bundle:
            _LOG.info(
                "Loaded policy filters bundle with %d policy key(s) for job %s",
                len(policy_bundle),
                job.id,
            )

    cp = scan_checkpoint_from_job(job)
    failed: FailedPoliciesMap = {}
    bucket = SP.environment_service.default_reports_bucket_name()
    partial_key = keys_builder.job_scan_partial(job)
    scan_partial = ScanPartialStore(SP.s3)
    if cp:
        failed = scan_partial.load_failed_policies_sidecar(bucket, partial_key)

    pending = pending_scan_regions(job, cp)

    successful = 0
    warnings = []

    checkpoint_version = cp['checkpoint_version'] if cp else 0
    completed_regions = set(cp['completed_regions']) if cp else set()

    _LOG.debug(f"Fingerprint aliases: {ctx.fingerprint_aliases}")

    for region in pending:
        _LOG.info(f"Going to init pool for region {region}")
        with multiprocessing.Pool(
            processes=1,
            initializer=job_initializer,
            initargs=(credentials,),
        ) as pool:
            scan = cast(
                RegionScanResult,
                pool.apply(
                    process_job_concurrent,
                    (policies, ctx.work_dir, cloud, region, policy_bundle),
                )
            )

        if scan.load_error_detail is not None:
            _LOG.warning(
                "Could not load policies for region %s: %s",
                region,
                scan.load_error_detail,
            )
            warnings.append(
                f"Could not load policies for region {region}: "
                f"{scan.load_error_detail}"
            )
            continue

        assert scan.failed is not None
        successful += scan.n_successful
        if scan.failed:
            if region == GLOBAL_REGION:
                w = (
                    f"{len(scan.failed)}/{len(scan.failed) + scan.n_successful} "
                    "global policies failed"
                )
            else:
                w = (
                    f"{len(scan.failed)}/{len(scan.failed) + scan.n_successful} "
                    f"policies failed in region {region}"
                )
            warnings.append(w)
        failed.update(scan.failed)

        result = JobResult(ctx.work_dir, cloud)
        partial = scan_partial.load_partial_collection(cloud, bucket, partial_key)
        ScanPartialStore.merge_delta_into_partial(
            partial,
            result.iter_shard_parts_for_region(region, failed),
            result.rules_meta_for_region(region),
        )
        partial.write_all()
        partial.write_meta()
        scan_partial.write_failed_policies_sidecar(bucket, partial_key, failed)
        completed_regions.add(region)
        checkpoint_version += 1
        checkpoint = ScanCheckpoint(
            checkpoint_version=checkpoint_version,
            completed_regions=sorted(completed_regions),
            updated_at=utc_iso(),
        )
        payload = dict(checkpoint)
        SP.job_service.update(job, scan_checkpoint=payload)
        job.scan_checkpoint = payload

    ctx.add_warnings(*warnings)
    del warnings

    creds_env = credentials if isinstance(credentials, dict) else {}
    if cloud is Cloud.GOOGLE and (
        path := creds_env.get(ENV_GOOGLE_APPLICATION_CREDENTIALS)
    ):
        _LOG.debug(f"Removing temporary google credentials file {path}")
        Path(path).unlink(missing_ok=True)
    if cloud is Cloud.AZURE and (
        path := creds_env.get(ENV_AZURE_CLIENT_CERTIFICATE_PATH)
    ):
        _LOG.debug(f"Removing temporary azure certificate file {path}")
        Path(path).unlink(missing_ok=True)
    if cloud is Cloud.KUBERNETES and (path := creds_env.get(ENV_KUBECONFIG)):
        _LOG.debug(f"Removing temporary kubeconfig file {path}")
        Path(path).unlink(missing_ok=True)

    del credentials

    merged_collection = None
    if completed_regions == regions:
        merged_collection = scan_partial.load_partial_collection(
            cloud, bucket, partial_key
        )

    finalize_standard_job_reports(
        ctx=ctx,
        keys_builder=keys_builder,
        cloud=cloud,
        failed=failed,
        successful=successful,
        merged_collection=merged_collection,
    )

    if completed_regions == regions:
        try:
            scan_partial.delete_partial(bucket, partial_key)
        except Exception:
            _LOG.exception(
                "Could not delete scan partial s3 prefix %s/%s",
                bucket,
                partial_key,
            )
        SP.job_service.update(job, clear_scan_checkpoint=True)
        job.scan_checkpoint = None

    if completed_regions == regions:
        assert merged_collection is not None
        if not any(merged_collection.iter_parts()):
            raise JobExecutionError(
                JobFailure.standard(JobErrorCode.NO_SUCCESSFUL_POLICIES)
            )
    elif not successful:
        raise JobExecutionError(
            JobFailure.standard(JobErrorCode.NO_SUCCESSFUL_POLICIES)
        )
    if job.is_ed_job:
        try:
            SP.report_delivery_service.notify_job_completed(job=job, tenant=ctx.tenant)
        except Exception:
            _LOG.exception(f"Report delivery notification failed for job {job.id}")
    _LOG.info(f"Job {job.id!r} has ended (type={job.job_type!r})")
