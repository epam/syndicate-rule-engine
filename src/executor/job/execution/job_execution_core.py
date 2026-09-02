"""Shared scan loop for standard and reactive job orchestrators."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import billiard as multiprocessing
from modular_sdk.commons.constants import (
    ENV_AZURE_CLIENT_CERTIFICATE_PATH,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
    ENV_KUBECONFIG,
)

from executor.job.execution.context import JobExecutionContext
from executor.job.execution.publish import finalize_standard_job_reports
from executor.job.execution.region_executor import (
    RegionScanResult,
    job_initializer,
    process_job_concurrent,
)
from executor.job.job_failure import JobFailure, JobErrorCode
from executor.job.scan import (
    FailedPoliciesMap,
    ScanCheckpoint,
    ScanPartialStore,
    pending_scan_regions,
    scan_checkpoint_from_job,
)
from executor.job.types import JobExecutionError
from executor.services.report_service import JobResult
from helpers.constants import GLOBAL_REGION, Cloud, PolicyErrorType
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from services import SP
from services.job_policy_filters.types import BundleFilters
from services.reports_bucket import ReportsBucketKeysBuilder

_LOG = get_logger(__name__)

_POLICY_ERROR_TYPE_TO_JOB_CODE = {
    PolicyErrorType.ACCESS: JobErrorCode.CLOUD_ACCESS_DENIED,
    PolicyErrorType.CREDENTIALS: JobErrorCode.INVALID_CLOUD_CREDENTIALS,
}


def build_failure_for_zero_success(failed: FailedPoliciesMap) -> JobFailure:
    """
    Build a JobFailure for the case where no policies succeeded.
    If all failures are of the same type, use the corresponding JobErrorCode.
    Otherwise, use JobErrorCode.NO_SUCCESSFUL_POLICIES with
    a summary of the failure types.
    """
    if not failed:
        return JobFailure.standard(JobErrorCode.NO_SUCCESSFUL_POLICIES)

    failed_types = set(i[0] for i in failed.values())
    if len(failed_types) == 1:
        error_type = failed_types.pop()
        job_code = _POLICY_ERROR_TYPE_TO_JOB_CODE.get(
            error_type, JobErrorCode.NO_SUCCESSFUL_POLICIES
        )
        return JobFailure.standard(job_code)

    failed_stat = build_failure_statistics(failed)
    details = ', '.join(f'{k} - {v}' for k, v in failed_stat.items())
    return JobFailure.standard(
        JobErrorCode.NO_SUCCESSFUL_POLICIES, detail=f'Error types: {details}'
    )


def build_failure_statistics(failed: FailedPoliciesMap) -> dict[str, int]:
    """
    Build a statistics dictionary for the failed policies.

    Returns a dictionary with the count of each error type.
    """
    return dict(Counter(i[0].value for i in failed.values()))


@dataclass(frozen=True, slots=True)
class JobScanOptions:
    policy_bundle: BundleFilters | None = None
    rule_events: dict[str, list[dict[str, Any]]] | None = None


def cleanup_temporary_credentials(
    cloud: Cloud,
    credentials: dict[str, str],
) -> None:
    if cloud is Cloud.GOOGLE and (
        path := credentials.get(ENV_GOOGLE_APPLICATION_CREDENTIALS)
    ):
        _LOG.debug('Removing temporary google credentials file %s', path)
        Path(path).unlink(missing_ok=True)
    if cloud is Cloud.AZURE and (
        path := credentials.get(ENV_AZURE_CLIENT_CERTIFICATE_PATH)
    ):
        _LOG.debug('Removing temporary azure certificate file %s', path)
        Path(path).unlink(missing_ok=True)
    if cloud is Cloud.KUBERNETES and (path := credentials.get(ENV_KUBECONFIG)):
        _LOG.debug('Removing temporary kubeconfig file %s', path)
        Path(path).unlink(missing_ok=True)


def execute_job_region_scan(
    ctx: JobExecutionContext,
    *,
    policies: list,
    credentials: dict[str, str],
    keys_builder: ReportsBucketKeysBuilder,
    cloud: Cloud,
    regions: set[str],
    scan_options: JobScanOptions | None = None,
) -> tuple[int, list[str], FailedPoliciesMap, set[str]]:
    """
    Run pending regions, update checkpoints and partials.

    Returns successful count, warnings, failed map, completed_regions.
    """
    job = ctx.job
    scan_options = scan_options or JobScanOptions()
    cp = scan_checkpoint_from_job(job)
    failed: FailedPoliciesMap = {}
    bucket = SP.environment_service.default_reports_bucket_name()
    partial_key = keys_builder.job_scan_partial(job)
    scan_partial = ScanPartialStore(SP.s3)
    if cp:
        failed = scan_partial.load_failed_policies_sidecar(bucket, partial_key)

    pending = pending_scan_regions(job, cp, all_regions=regions)
    successful = 0
    warnings: list[str] = []
    checkpoint_version = cp['checkpoint_version'] if cp else 0
    completed_regions = set(cp['completed_regions']) if cp else set()

    _LOG.debug('Fingerprint aliases: %s', ctx.fingerprint_aliases)

    for region in pending:
        _LOG.info('Going to init pool for region %s', region)
        with multiprocessing.Pool(
            processes=1,
            initializer=job_initializer,
            initargs=(credentials,),
        ) as pool:
            scan = cast(
                RegionScanResult,
                pool.apply(
                    process_job_concurrent,
                    (
                        policies,
                        ctx.work_dir,
                        cloud,
                        region,
                        scan_options.policy_bundle,
                        scan_options.rule_events,
                    ),
                ),
            )

        if scan.load_error_detail is not None:
            _LOG.warning(
                'Could not load policies for region %s: %s',
                region,
                scan.load_error_detail,
            )
            warnings.append(
                f'Could not load policies for region {region}: '
                f'{scan.load_error_detail}'
            )
            continue

        assert scan.failed is not None
        successful += scan.n_successful
        if scan.failed is not None:
            failed_len = len(scan.failed)
            total = failed_len + scan.n_successful

            failed_stat = build_failure_statistics(scan.failed)
            failed_stat_msg = ', '.join(
                f'{k} - {v}' for k, v in failed_stat.items()
            )
            if region == GLOBAL_REGION:
                w = (
                    f'{failed_len}/{total} global policies failed. '
                    f'Error types: {failed_stat_msg}'
                )
            else:
                w = (
                    f'{failed_len}/{total} policies failed in region {region}. '
                    f'Error types: {failed_stat_msg}'
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

    return successful, warnings, failed, completed_regions


def finalize_job_scan(
    ctx: JobExecutionContext,
    *,
    keys_builder: ReportsBucketKeysBuilder,
    cloud: Cloud,
    credentials: dict[str, str],
    regions: set[str],
    successful: int,
    warnings: list[str],
    failed: FailedPoliciesMap,
    completed_regions: set[str],
) -> None:
    job = ctx.job
    ctx.add_warnings(*warnings)

    cleanup_temporary_credentials(cloud, credentials)

    bucket = SP.environment_service.default_reports_bucket_name()
    partial_key = keys_builder.job_scan_partial(job)
    scan_partial = ScanPartialStore(SP.s3)

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
                'Could not delete scan partial s3 prefix %s/%s',
                bucket,
                partial_key,
            )
        SP.job_service.update(job, clear_scan_checkpoint=True)
        job.scan_checkpoint = None

    if completed_regions == regions:
        assert merged_collection is not None
        if not any(merged_collection.iter_parts()):
            raise JobExecutionError(
                build_failure_for_zero_success(failed)
            )
    elif not successful:
        raise JobExecutionError(
            build_failure_for_zero_success(failed)
        )
