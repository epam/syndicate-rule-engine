"""
Main job orchestration. Runs standard job: resolve rulesets, credentials,
execute policies per region, write reports, upload to SIEM.
"""

import billiard as multiprocessing
from itertools import chain
from pathlib import Path

from modular_sdk.commons.constants import (
    ENV_AZURE_CLIENT_CERTIFICATE_PATH,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
    ENV_KUBECONFIG,
)

from executor.helpers.constants import ExecutorError
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
)
from executor.job.integration.license_manager import post_lm_job
from executor.job.integration.siem import upload_to_siem
from executor.job.policies.filter import (
    expand_results_to_aliases,
    filter_policies,
    skip_duplicated_policies,
)
from executor.job.rulesets.resolver import resolve_job_rulesets
from executor.job.types import ExecutorException
from executor.services.report_service import JobResult
from helpers.constants import GLOBAL_REGION, Cloud
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services.platform_service import Platform
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    StatisticsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.ruleset_service import RulesetName
from services.sharding import ShardsCollectionFactory, ShardsS3IO

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
        raise ExecutorException(ExecutorError.NO_CREDENTIALS)

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

    successful = 0
    failed = {}
    warnings = []

    _LOG.debug(f"Fingerprint aliases: {ctx.fingerprint_aliases}")

    for region in sorted(regions):
        _LOG.info(f"Going to init pool for region {region}")
        with multiprocessing.Pool(
            processes=1,
            initializer=job_initializer,
            initargs=(credentials,),
        ) as pool:
            pair = pool.apply(
                process_job_concurrent, (policies, ctx.work_dir, cloud, region)
            )

        if pair[1] is None:
            _LOG.warning(f"Job for region {region} has failed with no policies loaded")
            warnings.append(f"Could not load policies for region {region}")
            continue

        successful += pair[0]
        if pair[1]:
            if region == GLOBAL_REGION:
                w = f"{len(pair[1])}/{len(pair[1]) + pair[0]} global policies failed"
            else:
                w = f"{len(pair[1])}/{len(pair[1]) + pair[0]} policies failed in region {region}"
            warnings.append(w)
        failed.update(pair[1])

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

    if ctx.fingerprint_aliases:
        _LOG.info("Expanding scan results to fingerprint aliases")
        expand_results_to_aliases(ctx, ctx.work_dir)

    result = JobResult(ctx.work_dir, cloud)

    collection = ShardsCollectionFactory.from_cloud(cloud)
    collection.put_parts(result.iter_shard_parts(failed))
    meta = result.rules_meta()
    collection.meta = meta

    if successful:
        _LOG.info("Going to upload to SIEM")
        upload_to_siem(ctx=ctx, collection=collection)

    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.job_result(job),
        client=SP.s3,
    )

    _LOG.debug("Writing job report")
    collection.write_all()

    latest = ShardsCollectionFactory.from_cloud(cloud)
    latest.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.latest_key(),
        client=SP.s3,
    )

    _LOG.debug("Pulling latest state")
    latest.fetch_by_indexes(collection.shards.keys())
    latest.fetch_meta()

    _LOG.debug("Writing latest state")
    latest.update(collection)
    latest.update_meta(meta)
    latest.write_all()
    latest.write_meta()

    _LOG.info("Writing statistics")
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(job),
        obj=result.statistics(ctx.tenant, failed),
    )
    if not successful:
        raise ExecutorException(ExecutorError.NO_SUCCESSFUL_POLICIES)
    if job.is_ed_job:
        try:
            SP.report_delivery_service.notify_job_completed(job=job, tenant=ctx.tenant)
        except Exception:
            _LOG.exception(f"Report delivery notification failed for job {job.id}")
    _LOG.info(f"Job {job.id!r} has ended (type={job.job_type!r})")
