"""
Two cases are currently possible:
- you want to use AWS Firehose to inject DynamoDB Stream records to S3;
- you want to put the records directly to S3 without the firehose.
The first method is more effective in terms of costs (and probably in the
matter of execution time & memory which anyway comes down to costs).
The second method allows to build custom S3 paths based on the content of
the streaming data.

The first method is chosen by default. It depends on such envs:
- FIREHOSE_STREAM_NAME: the name of configured AWS firehose stream (obviously);

The second method can be enabled by setting env `FORCE_DIRECT_UPLOAD` equal
to `true` and it depends on such envs:
- DIRECT_UPLOAD_BUCKET_NAME: the name of an S3 bucket to upload files to;
- DIRECT_UPLOAD_BUCKET_PREFIX: the key prefix to upload to. Default
  value is `Direct/CaaSJobs`;
- DIRECT_UPLOAD_JOB_PER_FILE: the number of job items to write to one file.
  In case the env is equal to `1` the name of each file is the inner job's id.
  In case the env is not set, the maximum number of jobs will be pushed
  to a single file. Its name will be built based on the first and the
  last jobs execution time.
The jobs in files are sorted in ascending order by `submitted_at` attr
if you are using the direct upload.

Both methods support one similar env:
- FILTER_RECORDS: if equal to `true` the input records will be filtered to
  keep only those that came from DynamoDB TTL. Since we are using Lambda
  Event Source Mapping filter, no custom filtering are needed. So by default
  this env is equal to `false`
"""
import gzip
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from itertools import islice
from pathlib import PurePosixPath
from secrets import token_hex
from typing import List, Dict, Generator, Optional, Iterable, IO, Union, Tuple, \
    Callable

import boto3
from botocore.exceptions import ClientError

DEFAULT_BUCKET_PREFIX = 'Direct/CaaSJobs'
LINE_SEP = '\n'
GZIP_EXTENSION = '.gz'
NOT_EXISTING_ENTITY = 'Empty'

FIREHOSE_STREAM_NAME = os.getenv('FIREHOSE_STREAM_NAME')
FILTER_RECORDS = str(os.getenv('FILTER_RECORDS')).lower() == 'false'

FORCE_DIRECT_UPLOAD = str(os.getenv('FORCE_DIRECT_UPLOAD')).lower() == 'true'
BUCKET_NAME = os.getenv('DIRECT_UPLOAD_BUCKET_NAME')
BUCKET_PREFIX = (os.getenv(
    'DIRECT_UPLOAD_BUCKET_PREFIX') or DEFAULT_BUCKET_PREFIX).strip('/')
JOBS_PER_FILE = int(os.getenv('DIRECT_UPLOAD_JOB_PER_FILE')) if os.getenv(
    'DIRECT_UPLOAD_JOB_PER_FILE') else None

_LOG = logging.getLogger()
_LOG.setLevel(logging.INFO)


def batches(iterable: Iterable, n: int) -> Generator[List, None, None]:
    if n < 1:
        raise ValueError('n must be >= 1')
    it = iter(iterable)
    batch = list(islice(it, n))
    while batch:
        yield batch
        batch = list(islice(it, n))


def backoff(max_retries: int = 5):
    """
    The decorated function must return bool: true if succeeded and false if not
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            succeeded, retry = func(*args, **kwargs), 0
            while not succeeded:
                if retry == max_retries:
                    _LOG.error(f'The number of max retries has been reached '
                               f'and function \'{func.__name__}\' has not '
                               f'succeeded. Quiting.')
                    break
                _delay = 2 ** retry
                _LOG.warning(f'Function \'{func.__name__}\' has not '
                             f'succeeded. Trying again after {_delay} secs')
                time.sleep(_delay)
                succeeded = func(*args, **kwargs)
                retry += 1

        return wrapper

    return decorator


class RecordsUploader(ABC):
    def __init__(self, records: Optional[List[Dict]] = None):
        self._records = records or []

    @property
    def i_records(self) -> Generator[Dict, None, None]:
        i_source = filter(self.is_valid_record, self._records) \
            if FILTER_RECORDS else iter(self._records)
        yield from i_source

    @property
    def i_old_images(self) -> Generator[Dict, None, None]:
        for record in self.i_records:
            yield record['dynamodb']['OldImage']

    @property
    def records(self) -> List[Dict]:
        return list(self.i_records)

    @property
    def old_images(self) -> List[Dict]:
        return list(self.i_old_images)

    @records.setter
    def records(self, value: List[Dict]):
        self._records = value

    @staticmethod
    def _record(image: dict) -> str:
        return json.dumps(image) + LINE_SEP

    @staticmethod
    def is_valid_record(record: dict) -> bool:
        _remove = record.get('eventName') == 'REMOVE'

        identity = record.get('userIdentity', {})
        _ttl = (identity.get('principalId') == 'dynamodb.amazonaws.com' and
                identity.get('type') == 'Service')
        return _remove and _ttl

    @abstractmethod
    def upload(self):
        ...


class FirehoseUploader(RecordsUploader):
    def __init__(self, records: Optional[List[Dict]] = None):
        super().__init__(records)
        self._client = None

    @property
    def client(self):
        if not self._client:
            _LOG.info('Initializing boto3 firehose client')
            self._client = boto3.client('firehose')
        return self._client

    def upload(self):
        _LOG.info('Uploading via firehose')
        data = tuple(
            {'Data': self._record(image)} for image in self.i_old_images
        )
        response = self.client.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM_NAME, Records=data
        )
        failed: int = response['FailedPutCount']
        if failed:
            _LOG.warning(f'{failed} items were not injected to the delivery '
                         f'stream. Trying to send them individually')
            for i, record in enumerate(response['RequestResponses']):
                if 'ErrorCode' not in record:
                    continue
                _LOG.info(f'Failed record: {record}')
                self.client.put_record(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Record={'Data': data[i]}
                )
        else:
            _LOG.info('All the items were successfully sent in one request')
        _LOG.info('Uploading via firehose have finished')


class DirectS3Uploader(RecordsUploader):
    def __init__(self, records: Optional[List[Dict]] = None):
        super().__init__(records)
        self._client = None

    @staticmethod
    def _gz_key(key: str) -> str:
        if not key.endswith(GZIP_EXTENSION):
            key = key.strip('.') + GZIP_EXTENSION
        return key

    @property
    def client(self):
        if not self._client:
            _LOG.info('Initializing boto3 s3 client')
            self._client = boto3.client('s3')
        return self._client

    @backoff(5)
    def put_object(self, bucket_name: str, key: str, body: Union[bytes, IO]):
        _LOG.debug(f'Uploading object \'{key}\'')
        try:
            self.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body
            )
            return True
        except (ClientError, Exception) as e:
            _LOG.warning(f'An error occurred trying to '
                         f'upload file \'{key}\': {e}.')
            return False

    def put_objects_batch(self, bucket_name: str,
                          key_body: Iterable[Tuple[str, Union[IO, bytes]]]):
        with ThreadPoolExecutor() as executor:
            for pair in key_body:
                executor.submit(self.put_object, bucket_name, *pair)

    @abstractmethod
    def upload(self):
        ...


class CaaSJobsS3Uploader(DirectS3Uploader):
    def sort_jobs(self, records: List[Dict]) -> List[Dict]:
        return sorted(records, key=lambda x: self._job_time(x))

    @staticmethod
    def _job_time(record: Dict) -> str:
        return record['submitted_at']['S']

    def get_bounds(self, records: List[Dict]) -> Tuple[str, str]:
        """
        Records must be already sorted.
        """
        _len = len(records)
        if _len < 1:
            raise ValueError('n must be >= 1')
        if _len == 1:
            _submitted_at = self._job_time(records[0])
            return _submitted_at, _submitted_at
        if _len > 1:
            return self._job_time(records[0]), self._job_time(records[-1])

    def upload(self):
        _LOG.info('Uploading directly to S3')
        entities = {}
        for image in self.i_old_images:
            entities.setdefault((
                image.get('customer_display_name', {}).get('S'),
                image.get('tenant_display_name', {}).get('S'),
                image.get('account_display_name', {}).get('S')), []
            ).append(image)
        _ = self.client  # init client before threads otherwise there are problems
        # TODO stream data in case MemoryError ?
        if JOBS_PER_FILE == 1:
            _LOG.info('Special case, JOBS_PER_FILE is equal to 1. '
                      'Key is job`s id')
            key_body = []
            for c_t_a, images in entities.items():
                _key = PurePosixPath(
                    BUCKET_PREFIX, *(e or NOT_EXISTING_ENTITY for e in c_t_a))
                key_body.extend((
                    (self._gz_key(str(_key / image['job_id']['S'])),
                     gzip.compress(self._record(image).encode())) for image in
                    images
                ))
        elif JOBS_PER_FILE:
            _LOG.info(f'Uploading batches with {JOBS_PER_FILE} jobs in a file')
            key_body = []
            for c_t_a, images in entities.items():
                _key = PurePosixPath(
                    BUCKET_PREFIX, *(e or NOT_EXISTING_ENTITY for e in c_t_a))
                for batch in batches(images, JOBS_PER_FILE):
                    items_s = self.sort_jobs(batch)
                    start, end = self.get_bounds(items_s)
                    key_body.append((
                        self._gz_key(
                            str(_key / f'{start}-{end}-{token_hex(8)}')),
                        gzip.compress(
                            ''.join(map(self._record, items_s)).encode())
                    ))
        else:
            _LOG.info('JOB_PER_FILE is not set. Uploading all at once')
            key_body = []
            for c_t_a, images in entities.items():
                _key = PurePosixPath(
                    BUCKET_PREFIX, *(e or NOT_EXISTING_ENTITY for e in c_t_a))
                items_s = self.sort_jobs(images)
                start, end = self.get_bounds(items_s)
                key_body.append((
                    self._gz_key(str(_key / f'{start}-{end}-{token_hex(8)}')),
                    gzip.compress(''.join(map(self._record, items_s)).encode())
                ))
        self.put_objects_batch(BUCKET_NAME, key_body)
        _LOG.info('Uploading directly to S3 has finished')


firehose_uploader = FirehoseUploader()
s3_uploader = CaaSJobsS3Uploader()


def lambda_handler(event, context):
    _LOG.info(f'Event: {event}')
    code, message = 200, 'Success'
    try:
        uploader = s3_uploader if FORCE_DIRECT_UPLOAD else firehose_uploader
        uploader.records = event['Records']
        uploader.upload()
    except ClientError as e:
        _LOG.error(f'Botocore error occurred: \'{e}\'')
        code, message = 500, str(e)
    except Exception as e:
        _LOG.error(f'Unexpected error occurred: \'{e}\'')
        code, message = 500, str(e)
    response = {
        'statusCode': code,
        'body': json.dumps({'message': message})
    }
    _LOG.info(f'Response: {response}')
    return response
