import logging
import os
import re
import sys
from urllib.parse import urlparse

import pymongo
from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.config import Config
from pymongo.collection import Collection
from pymongo.database import Database
import threading
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s.%(funcName)s:%(lineno)d %(message)s',
    level=logging.INFO,
)
_LOG = logging.getLogger(__name__)


KEY = 'Type'
VALUE = 'DataSnapshot'


def _init_minio() -> ServiceResource:
    endpoint = os.environ.get('SRE_MINIO_ENDPOINT')
    access_key = os.environ.get('SRE_MINIO_ACCESS_KEY_ID')
    secret_key = os.environ.get('SRE_MINIO_SECRET_ACCESS_KEY')
    assert endpoint, 'minio endpoint is required'
    assert access_key, 'minio access key is required'
    assert secret_key, 'minio secret key is required'
    resource = Session().resource(
        service_name='s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        config=Config(
            s3={'signature_version': 's3v4', 'addressing_style': 'path'}
        ),
    )
    _LOG.info('Minio connection was successfully initialized')
    return resource.meta.client


def _init_mongo() -> Database:
    host = os.environ.get('SRE_MONGO_URI')
    db = os.environ.get('SRE_MONGO_DB_NAME')
    assert host, 'Host is required'
    assert db, 'db name is required'

    client = pymongo.MongoClient(host=host)
    return client.get_database(db)


def _needs_tag(tag_set: list[dict], key: str, value: str) -> bool:
    for item in tag_set:
        if item.get('Key') == key and item.get('Value') == value:
            return False
    return True


class Counter:
    __slots__ = 'count', 'lock'
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.count += 1

    def get(self) -> int:
        with self.lock:
            return self.count


def _tag_object(bucket: str, key: str, minio_client, counter: Counter) -> None:
    """
    Tags an object in the specified bucket with the provided tag set.
    """
    tag_set = minio_client.get_object_tagging(Bucket=bucket, Key=key).get('TagSet') or []

    if not _needs_tag(tag_set, KEY, VALUE):
        _LOG.info(f'Skipping {key} because already has tag')
        return
    tag_set.append({'Key': KEY, 'Value': VALUE})

    minio_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={'TagSet': tag_set},
    )
    _LOG.info(f'Tagged {key} with {tag_set}')
    counter.increment()


def patch_snapshots(minio_client) -> None:
    """
    This patch tags all the files with `snapshots/*` or `*/snapshots/*` prefixes as `Type: DataSnapshot`
    """
    _LOG.info('Starting patch')

    params = {
        'Bucket': os.environ.get('SRE_REPORTS_BUCKET_NAME') or 'reports',
        'MaxKeys': 1000,
    }
    assert params.get('Bucket')

    _LOG.info(f'Bucket: {params["Bucket"]}')


    _LOG.info('Going to create a thread pool executor')
    executor = ThreadPoolExecutor(max_workers=10)

    counter = Counter()
    while True:
        response = minio_client.list_objects_v2(**params)

        for item in response.get('Contents') or ():
            if not re.match(r'(^|.*/)snapshots/.*', item['Key']):
                _LOG.debug(f'Skipping {item["Key"]} because it does not match snapshots/* or */snapshots/*')
                continue

            executor.submit(_tag_object, params['Bucket'], item['Key'], minio_client, counter)

        if response.get('IsTruncated'):
            params['ContinuationToken'] = response['NextContinuationToken']
        else:
            break

    _LOG.info('Waiting for all tasks to complete')
    executor.shutdown(wait=True)
    _LOG.info(f'Patch has finished. Tagged: {counter.get()} files')


def _need_metrics_patch(collection: Collection):
    """
    Checks if metrics are already patched
    """
    # TODO: implement some system to track executed patches in order not to use such hacks
    return bool(collection.find_one({'d': {'$type': 'object'}}))


def patch_report_metrics(minio_client, database):
    _LOG.info('Going to patch report metrics')
    collection = database.get_collection('CaaSReportMetrics')
    if not _need_metrics_patch(collection):
        _LOG.info('Metrics were already patched')
        return

    query = {'d': {'$type': 'object'}}

    ids = []
    for item in collection.find(query):
        link = item.get('l')
        if link:
            parsed = urlparse(link)
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')
            _LOG.info(f'Going to remove from minio: {link}')
            minio_client.delete_object(Bucket=bucket, Key=key)
        ids.append(item['_id'])

    _LOG.info('Going to remove old metrics items')
    collection.delete_many({'_id': {'$in': ids}})


def main() -> int:
    try:
        client = _init_minio()
        database = _init_mongo()

        patch_report_metrics(client, database)
        patch_snapshots(client)
        return 0
    except Exception:
        _LOG.exception('Unexpected exception')
        return 1


if __name__ == '__main__':
    sys.exit(main())
