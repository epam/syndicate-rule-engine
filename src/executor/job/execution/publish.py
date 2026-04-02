"""Finalize standard job: job report S3, latest merge, statistics."""

from __future__ import annotations

from executor.job.execution.context import JobExecutionContext
from executor.services.report_service import JobResult, statistics_from_shards_collection
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from executor.job.scan.types import FailedPoliciesMap
from services import SP
from services.reports_bucket import ReportsBucketKeysBuilder, StatisticsBucketKeysBuilder
from services.sharding import (
    ShardPart,
    ShardsCollection,
    ShardsCollectionFactory,
    ShardsS3IO,
)

_LOG = get_logger(__name__)


def _expand_collection_fingerprint_aliases(
    ctx: JobExecutionContext,
    collection: ShardsCollection,
) -> None:
    """Mirror filesystem alias expansion for S3-merged collections (resume path)."""
    if not ctx.fingerprint_aliases:
        return
    parts = list(collection.iter_all_parts())
    extra_parts: list[ShardPart] = []
    for _fp, names in ctx.fingerprint_aliases.items():
        if len(names) <= 1:
            continue
        primary = names[0]
        for alias in names[1:]:
            for part in parts:
                if part.policy != primary:
                    continue
                extra_parts.append(
                    ShardPart(
                        policy=alias,
                        location=part.location,
                        timestamp=part.timestamp,
                        resources=list(part.resources),
                        error=part.error,
                        previous_timestamp=part.previous_timestamp,
                    )
                )
            if primary in collection.meta:
                collection.meta.setdefault(alias, {}).update(
                    dict(collection.meta[primary])
                )
    if extra_parts:
        collection.put_parts(extra_parts)


def finalize_standard_job_reports(
    ctx: JobExecutionContext,
    keys_builder: ReportsBucketKeysBuilder,
    cloud: Cloud,
    failed: FailedPoliciesMap,
    successful: int,
    *,
    merged_collection: ShardsCollection | None = None,
) -> None:
    """
    Writes full job result to S3, merges into latest, uploads statistics.

    When ``merged_collection`` is set (all regions finished; data was merged
    incrementally to S3 partial), the final report is built from that
    collection so resumed jobs include every region, not only the current
    worker ``work_dir``.
    """
    if merged_collection is not None:
        collection = merged_collection
        if ctx.fingerprint_aliases:
            _LOG.info('Expanding merged scan shards to fingerprint aliases')
        _expand_collection_fingerprint_aliases(ctx, collection)
        has_successful = any(collection.iter_parts())
        stats = statistics_from_shards_collection(ctx.tenant, failed, collection)
        meta = collection.meta
    else:
        if ctx.fingerprint_aliases:
            _LOG.info('Expanding scan results to fingerprint aliases')
            from executor.job.policies.filter import expand_results_to_aliases

            expand_results_to_aliases(ctx, ctx.work_dir)

        result = JobResult(ctx.work_dir, cloud)

        collection = ShardsCollectionFactory.from_cloud(cloud)
        collection.put_parts(result.iter_shard_parts(failed))
        meta = result.rules_meta()
        collection.meta = meta

        has_successful = bool(successful)
        stats = result.statistics(ctx.tenant, failed)

    if has_successful:
        _LOG.info('Going to upload to SIEM')
        from executor.job.integration.siem import upload_to_siem

        upload_to_siem(ctx=ctx, collection=collection)

    collection.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.job_result(ctx.job),
        client=SP.s3,
    )

    _LOG.debug('Writing job report')
    collection.write_all()

    latest = ShardsCollectionFactory.from_cloud(cloud)
    latest.io = ShardsS3IO(
        bucket=SP.environment_service.default_reports_bucket_name(),
        key=keys_builder.latest_key(),
        client=SP.s3,
    )

    _LOG.debug('Pulling latest state')
    latest.fetch_by_indexes(collection.shards.keys())
    latest.fetch_meta()

    _LOG.debug('Writing latest state')
    latest.update(collection)
    latest.update_meta(meta)
    latest.write_all()
    latest.write_meta()

    _LOG.info('Writing statistics')
    SP.s3.gz_put_json(
        bucket=SP.environment_service.get_statistics_bucket_name(),
        key=StatisticsBucketKeysBuilder.job_statistics(ctx.job),
        obj=stats,
    )
