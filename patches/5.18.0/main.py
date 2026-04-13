import logging
import os
import sys
from urllib.parse import quote, unquote

import pymongo
from pymongo import MongoClient
from pymongo.database import Database

from modular_sdk.commons.constants import ParentType

from helpers.constants import Env, GLOBAL_REGION

from services import SP
from services.clients.s3 import S3Client
from services.reports_bucket import ReportsBucketKeysBuilder


logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)


def _init_minio_s3():
    endpoint = os.environ.get('SRE_MINIO_ENDPOINT')
    access_key = os.environ.get('SRE_MINIO_ACCESS_KEY_ID')
    secret_key = os.environ.get('SRE_MINIO_SECRET_ACCESS_KEY')
    assert endpoint, 'minio endpoint is required'
    assert access_key, 'minio access key is required'
    assert secret_key, 'minio secret key is required'

    client = SP.s3.client
    _LOG.info('MinIO connection was successfully initialized')
    return client


def _init_mongo() -> Database:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client: MongoClient = pymongo.MongoClient(host=host)
    return client.get_database(db)


def check_key_exists(client, bucket: str, source_key: str) -> bool:
    """
    Check if any objects exist under the given prefix.
    Returns True if at least one object exists, False otherwise.
    """
    try:
        _LOG.debug(f'Checking if objects exist at: bucket={bucket}, prefix={source_key}')

        response = client.list_objects_v2(
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


def find_duplicate_platform_ids(platform_documents: list) -> set:
    """
    Find platforms with duplicate platform_id (name-region combinations).
    Returns a set of document PIDs that duplicated.
    """
    checked_platform: dict[str, str] = dict()
    duplicates = set()

    for doc in platform_documents:
        pid = doc.get("pid")
        meta = doc.get("meta", {})
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
                             database: Database,
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
        _LOG.warning('Running with --force flag - duplicate platform names will be processed')

    # Track statistics
    stats = {'migrated': 0, 'no_source': 0, 'skipped': 0, 'duplicate_skipped': 0, 'errors': 0}

    # Get all parent objects with type "PLATFORM_K8S" from the MongoDB
    platform_parent_collection = list(database.get_collection('Parents').find({"t": ParentType.PLATFORM_K8S}))

    if not platform_parent_collection:
        _LOG.info('No platforms found for migration')
        return False

    _LOG.info(f'Found {len(platform_parent_collection)} platform(s) that may need migration')

    # Check for duplicate platform_ids
    duplicates = find_duplicate_platform_ids(platform_parent_collection)

    if duplicates:
        _LOG.warning('=' * 50)
        _LOG.warning('WARNING: Found platforms with duplicate names (name-region combinations):')
        for pid in duplicates:
            _LOG.warning(f'  Platform ID: {pid}')
        _LOG.warning('=' * 50)

        if not force:
            _LOG.warning('These platforms will be SKIPPED. Use --force to process them anyway.')
        else:
            _LOG.warning('--force flag is set. These platforms will be processed (may cause issues).')

    for platform_document in platform_parent_collection:

        pid = platform_document.get("pid")
        meta = platform_document.get("meta", {})
        name = meta.get('name')
        region = meta.get('region') or GLOBAL_REGION
        customer = platform_document.get("cid")

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

            paginator = client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=target_bucket, Prefix=source_key)

            _LOG.info(f'Moving: {source_key} -> {destination_key}')

            for page in pages:
                for obj in page.get('Contents', []):
                    old_key = obj['Key']
                    new_key = old_key.replace(source_key, destination_key, 1)

                    if dry_run:
                        continue
                    copy_source = {
                        'Bucket': target_bucket,
                        'Key': old_key
                    }
                    client.copy(copy_source, target_bucket, new_key)
                    client.delete_object(Bucket=target_bucket, Key=old_key)

            if not dry_run:
                stats['migrated'] += 1
            else:
                stats['skipped'] += 1


        except Exception as e:
            _LOG.error(f'Failed to move 0{source_key}: {e}')
            stats['errors'] += 1

    _LOG.info('=' * 50)
    _LOG.info('Migration complete!')
    _LOG.info(f"  Migrated: {stats['migrated']}")
    _LOG.info(f"  Skipped: {stats['skipped']}")
    _LOG.info(f"  No source objects: {stats['no_source']}")
    _LOG.info(f"  Duplicate skipped: {stats['duplicate_skipped']}")
    _LOG.info(f"  Errors: {stats['errors']}")
    _LOG.info('=' * 50)

    return stats['errors'] == 0


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description='Migrate reports from platform_id to id paths')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run without making changes')
    parser.add_argument('--force', action='store_true',
                        help='Process platforms with duplicate names (may cause issues)')
    args = parser.parse_args()

    try:
        client = _init_minio_s3()
        database = _init_mongo()

        success = migrate_platform_reports(
            client=client,
            database=database,
            dry_run=args.dry_run,
            force=args.force
        )
        return 0 if success else 1
    except Exception as e:
        _LOG.exception(f'Unexpected exception during migration. {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())