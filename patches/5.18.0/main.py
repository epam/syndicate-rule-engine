import logging
import os
import sys
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
from typing import Iterator

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


    client = SP.s3.client()
    _LOG.info('MinIO connection was successfully initialized')
    return client


def _init_mongo() -> Database:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client: MongoClient = pymongo.MongoClient(host=host)
    return client.get_database(db)


def iter_bucket_prefixes(prefix: str, delimiter: str = '/') -> Iterator[str]:
    """Iterate over common prefixes in the reports bucket."""
    bucket = Env.REPORTS_BUCKET_NAME.as_str()
    yield from SP.s3.common_prefixes(
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter
    )


def iter_bucket_objects(prefix: str) -> Iterator[str]:
    """Iterate over all object keys with given prefix."""
    bucket = Env.REPORTS_BUCKET_NAME.as_str()
    paginator = SP.s3.client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key']


def migrate_platform_reports(client: S3Client, database: Database, dry_run: bool = False) -> bool | None:
    """Main migration function to move reports from old path structure to new."""
    target_bucket = Env.REPORTS_BUCKET_NAME.as_str()
    base_prefix = ReportsBucketKeysBuilder.prefix

    _LOG.info(f'Starting reports path migration in bucket: {target_bucket}')
    _LOG.info(f'Base prefix: {base_prefix}')


    if dry_run:
        _LOG.info('Running in DRY RUN mode - no changes will be made')

    # Track statistics
    stats = {'migrated': 0, 'errors': 0}

    # Get all parent object with type "PLATFORM_K8S" from the MongoDB
    platform_parent_collection = database.get_collection('Parents').find({"t": ParentType.PLATFORM_K8S})
    platform_parent_collection = list(platform_parent_collection)
    if not platform_parent_collection:
        _LOG.info('No platforms require path migration (platform_id == id for all)')
        return False
    _LOG.info(f'Found {len(platform_parent_collection)} platform(s) that may need migration')

    for platform_document in platform_parent_collection:

        pid = platform_document.get("id")
        name = platform_document.get("meta")['name']
        region = platform_document.get("meta")['region'] or GLOBAL_REGION
        platform_id = f'{name}-{region}'
        customer = platform_document.get("cid")

        source_key = f'{base_prefix}/{customer}/KUBERNETES/{platform_id}/'
        dest_key = f'{base_prefix}/{customer}/KUBERNETES/{pid}/'
        try:
            if dry_run:
                _LOG.info(f'[DRY RUN] Would move: {source_key} -> {dest_key}')
                return True

            _LOG.info(f'Moving: {source_key} -> {dest_key}')

            client.copy(
                bucket=target_bucket,
                key=source_key,
                destination_bucket=target_bucket,
                destination_key=dest_key
            )
            client.delete_object(
                bucket=target_bucket,
                key=source_key
            )
            stats['migrated'] += 1

        except Exception as e:
            _LOG.error(f'Failed to move {source_key}: {e}')
            stats['errors'] += 1

    _LOG.info('=' * 50)
    _LOG.info('Migration complete!')
    _LOG.info(f"  Migrated: {stats['migrated']}")
    _LOG.info(f"  Errors: {stats['errors']}")


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description='Migrate reports from platform_id to id paths')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run without making changes')
    args = parser.parse_args()

    try:
        client = _init_minio_s3()
        database = _init_mongo()

        migrate_platform_reports(client=client,
                                 database=database,
                                 dry_run=args.dry_run)
        return 0
    except Exception as e:
        _LOG.exception(f'Unexpected exception during migration. {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())