import logging
import os
import sys
from urllib.parse import quote, unquote

from modular_sdk.models.parent import Parent
from modular_sdk.services.customer_service import CustomerService
from modular_sdk.commons.constants import ParentType, ParentScope

from helpers.constants import Env, GLOBAL_REGION
from services import SP
from services.clients.s3 import S3Client
from services.platform_service import PlatformService
from services.reports_bucket import ReportsBucketKeysBuilder

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)

# Legacy reactive job segments (before rename to "reactive/"). Phase 2 runs after
# K8s platform path migration so objects already under .../KUBERNETES/{pid}/... are
# picked up by the raw/ scan.
_JOBS_EVENT_DRIVEN = 'jobs/event-driven/'
_JOBS_REACTIVE = 'jobs/reactive/'
_STATS_EVENT_DRIVEN_PREFIX = 'job-statistics/event-driven/'
_STATS_REACTIVE_PREFIX = 'job-statistics/reactive/'


def _init_minio_s3():
    endpoint = os.environ.get('SRE_MINIO_ENDPOINT')
    access_key = os.environ.get('SRE_MINIO_ACCESS_KEY_ID')
    secret_key = os.environ.get('SRE_MINIO_SECRET_ACCESS_KEY')
    assert endpoint, 'minio endpoint is required'
    assert access_key, 'minio access key is required'
    assert secret_key, 'minio secret key is required'

    client = SP.s3
    _LOG.info('MinIO connection was successfully initialized')
    return client


def _reactive_migration_new_key(key: str) -> str | None:
    if key.startswith(_STATS_EVENT_DRIVEN_PREFIX):
        return _STATS_REACTIVE_PREFIX + key[len(_STATS_EVENT_DRIVEN_PREFIX):]
    if _JOBS_EVENT_DRIVEN in key:
        return key.replace(_JOBS_EVENT_DRIVEN, _JOBS_REACTIVE, 1)
    return None


def migrate_reactive_bucket_paths(client: S3Client, dry_run: bool = False) -> bool:
    """
    Rename S3 path segments event-driven -> reactive under raw/.../jobs/ in the
    reports bucket, and under job-statistics/... in the statistics bucket.
    """
    reports_bucket = Env.REPORTS_BUCKET_NAME.as_str()
    statistics_bucket = Env.STATISTICS_BUCKET_NAME.as_str()
    stats = {
        'moved': 0,
        'would_move': 0,
        'skipped_collision': 0,
        'errors': 0,
    }

    if dry_run:
        _LOG.info('DRY RUN: reactive phase will not copy or delete objects')

    paginator = client.client.get_paginator('list_objects_v2')

    def scan_prefix(prefix: str, bucket: str, need_substring: bool) -> None:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []) or []:
                old_key = obj['Key']
                if need_substring and _JOBS_EVENT_DRIVEN not in old_key:
                    continue
                new_key = _reactive_migration_new_key(old_key)
                if not new_key or new_key == old_key:
                    continue
                try:
                    if client.object_exists(bucket, new_key):
                        _LOG.warning(
                            'Skipping reactive migration (destination exists): '
                            f'{old_key!r} -> {new_key!r}'
                        )
                        stats['skipped_collision'] += 1
                        continue
                    if dry_run:
                        stats['would_move'] += 1
                        continue
                    client.copy(
                        bucket=bucket,
                        key=old_key,
                        destination_bucket=bucket,
                        destination_key=new_key,
                    )
                    client.delete_object(bucket, old_key)
                    stats['moved'] += 1
                except Exception as e:
                    _LOG.error(
                        f'Failed reactive migration for {old_key!r} -> {new_key!r}: {e}'
                    )
                    stats['errors'] += 1

    try:
        scan_prefix(_STATS_EVENT_DRIVEN_PREFIX, statistics_bucket, need_substring=False)
        scan_prefix(ReportsBucketKeysBuilder.prefix, reports_bucket, need_substring=True)
    except Exception as e:
        _LOG.error(f'Reactive path migration failed: {e}')
        stats['errors'] += 1

    _LOG.info('Reactive path migration finished')
    if dry_run:
        _LOG.info(f"  Would move: {stats['would_move']} object(s)")
    else:
        _LOG.info(f"  Moved: {stats['moved']}")
    _LOG.info(f"  Skipped (destination exists): {stats['skipped_collision']}")
    _LOG.info(f"  Errors: {stats['errors']}")

    return stats['errors'] == 0


def check_key_exists(client, bucket: str, source_key: str) -> bool:
    """
    Check if any objects exist under the given prefix.
    Returns True if at least one object exists, False otherwise.
    """
    try:
        _LOG.debug(f'Checking if objects exist at: bucket={bucket}, prefix={source_key}')

        response = client.client.list_objects_v2(
            Bucket=bucket,
            Prefix=source_key,
            MaxKeys=5
        )

        contents = response.get('Contents', [])
        if contents:
            _LOG.debug(f'Found object: {contents[0].get("Key")}')
            return True
        return False

    except Exception as e:
        _LOG.error(f'Error checking if key exists {source_key}: {e}')
        return False


def find_duplicate_platform_ids(platforms: list[Parent]) -> set[str]:
    """
    Find platforms with duplicate platform_id (name-region combinations).
    Returns a set of document PIDs that duplicated.
    """
    checked_platform: dict[str, str] = dict()
    duplicates = set()

    for platform in platforms:
        pid = platform.parent_id
        meta = platform.meta.as_dict()
        name = meta.get('name')
        region = meta.get('region') or GLOBAL_REGION

        if not name:
            continue

        platform_id = f'{name}-{region}'
        if platform_id in checked_platform.values():
            checked_platform[pid] = platform_id
            duble = set(k for k, v in checked_platform.items() if v == platform_id)
            duplicates.update(duble)
            continue
        checked_platform[pid] = platform_id
    return duplicates


def migrate_platform_reports(client: S3Client,
                             dry_run: bool = False,
                             force: bool = False) -> bool | None:
    """Main migration function to move reports from old path structure to new."""
    target_bucket = Env.REPORTS_BUCKET_NAME.as_str()
    base_prefix = ReportsBucketKeysBuilder.prefix

    _LOG.info(f'Starting reports path migration in bucket: {target_bucket}')
    _LOG.info(f'Base prefix: {base_prefix}')
    if dry_run:
        _LOG.info('Running in DRY RUN mode - no changes will be made')

    if force:
        _LOG.warning(
            'Running with --force flag - duplicate platform names will be processed'
        )

    # Track statistics
    stats = {'migrated': 0, 'no_source': 0, 'skipped': 0, 'duplicate_skipped': 0, 'errors': 0}

    # Get all parent objects with type "PLATFORM_K8S" from the MongoDB
    platform_service: PlatformService = SP.platform_service
    platforms: list[Parent] = list()

    cs = CustomerService()

    for customer in cs.i_get_customer(is_active=True):
        for platform in platform_service._ps.query_by_scope_index(
                customer_id=customer.name,
                type_=ParentType.PLATFORM_K8S,
                scope=ParentScope.SPECIFIC,
                is_deleted=False,
            ):
            platforms.append(platform)

    if not platforms:
        _LOG.info('No platforms found for migration')
        return True

    _LOG.info('Loaded %s platform document(s) from MongoDB', len(platforms))

    # Check for duplicate platform_ids
    duplicates = find_duplicate_platform_ids(platforms)

    if duplicates:
        _LOG.warning('Duplicate name–region combinations detected:')
        for pid in sorted(duplicates):
            _LOG.warning('  parent_id=%s', pid)

        if not force:
            _LOG.warning(
                'Those platforms will be skipped unless you pass --force.'
            )
        else:
            _LOG.warning('--force set: duplicates will still be processed.')

    for platform in platforms:

        pid = platform.parent_id
        meta = platform.meta.as_dict()
        name = meta.get('name')
        region = meta.get('region') or GLOBAL_REGION
        customer = platform.customer_id

        if not name:
            _LOG.warning(f'Platform {pid} has no name in meta, skipping')
            stats['skipped'] += 1
            continue

        platform_id = f'{name}-{region}'

        # Check if this platform should be skipped due to duplicates
        if pid in duplicates and not force:
            _LOG.warning(f'Skipping platform {pid} ({platform_id}) due to duplicate name')
            stats['duplicate_skipped'] += 1
            continue

        source_key = f'{base_prefix}{customer}/KUBERNETES/{platform_id}/'
        # First, fully decode any percent-encoded characters
        decoded = unquote(source_key)
        # Then re-encode only special characters (keep / and spaces as-is)
        source_key = quote(decoded, safe='/ ')
        destination_key = f'{base_prefix}{customer}/KUBERNETES/{pid}/'
        try:
            # Check if source key has any objects
            exists = check_key_exists(client, target_bucket, source_key)

            if not exists:
                _LOG.info(f'No objects found at source: {source_key}, skipping')
                stats['no_source'] += 1
                continue
            paginator = client.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=target_bucket, Prefix=source_key)

            _LOG.info(f'Moving: {source_key} -> {destination_key}')

            for page in pages:
                for obj in page.get('Contents', []):
                    old_key = obj['Key']
                    new_key = old_key.replace(source_key, destination_key, 1)

                    if dry_run:
                        continue

                    client.copy(
                        bucket=target_bucket,
                        key=old_key,
                        destination_bucket=target_bucket,
                        destination_key=new_key
                    )
                    client.delete_object(target_bucket, old_key)

            if not dry_run:
                stats['migrated'] += 1
            else:
                stats['skipped'] += 1


        except Exception as e:
            _LOG.error('Failed to move prefix %s: %s', source_key, e)
            stats['errors'] += 1

    _LOG.info('Migration complete!')
    _LOG.info(f"  Migrated: {stats['migrated']}")
    _LOG.info(f"  Skipped: {stats['skipped']}")
    _LOG.info(f"  No source objects: {stats['no_source']}")
    _LOG.info(f"  Duplicate skipped: {stats['duplicate_skipped']}")
    _LOG.info(f"  Errors: {stats['errors']}")

    return stats['errors'] == 0


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            'Migrate reports: (1) K8s platform paths name-region -> id, '
            '(2) reactive job paths event-driven -> reactive'
        )
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Run without making changes')
    parser.add_argument('--force', action='store_true',
                        help='Process platforms with duplicate names (may cause issues)')
    args = parser.parse_args()

    logging.getLogger('rule_engine').setLevel(logging.WARNING)

    try:
        _LOG.info('=' * 50)
        _LOG.info(
            'Patch 5.18.0 | mode=%s',
            'DRY-RUN' if args.dry_run else 'APPLY',
        )
        if args.force:
            _LOG.info('--force enabled')
        _LOG.info('=' * 50)

        client = _init_minio_s3()

        _LOG.info('')
        _LOG.info('=' * 50)
        _LOG.info('PHASE 1 OF 2 — Kubernetes platform reports (name-region -> id)')
        _LOG.info('=' * 50)
        platform_ok = migrate_platform_reports(
            client=client,
            dry_run=args.dry_run,
            force=args.force
        )
        _LOG.info(f"Phase 1 result: {'OK' if platform_ok else 'FAIL'}")

        _LOG.info('')
        _LOG.info('=' * 50)
        _LOG.info('PHASE 2 OF 2 — Reactive paths (event-driven -> reactive)')
        _LOG.info('=' * 50)
        reactive_ok = migrate_reactive_bucket_paths(
            client=client,
            dry_run=args.dry_run,
        )
        _LOG.info(f"Phase 2 result: {'OK' if reactive_ok else 'FAIL'}")

        _LOG.info('')
        _LOG.info('=' * 50)
        _LOG.info(
            'ALL PHASES: %s',
            'success' if (platform_ok and reactive_ok) else 'finished with errors',
        )
        _LOG.info('=' * 50)
        return 0 if (platform_ok and reactive_ok) else 1
    except Exception as e:
        _LOG.exception(f'Unexpected exception during migration. {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
