import io
import ipaddress
import json
import os.path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from gzip import GzipFile
from subprocess import PIPE, Popen
from typing import Union, Generator, Iterable, Dict, Tuple, Optional, \
    TypedDict, List
from urllib.parse import urlparse
from urllib3.util import Url, parse_url

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.utils import IMDSFetcher, _RetriesExceededError

from helpers import coroutine
from helpers.constants import ENV_SERVICE_MODE, ENV_MINIO_HOST, \
    ENV_MINIO_PORT, DOCKER_SERVICE_MODE, \
    ENV_MINIO_ACCESS_KEY, ENV_MINIO_SECRET_ACCESS_KEY
from helpers.log_helper import get_logger

UTF_8_ENCODING = 'utf-8'
MINIKUBE_IP_PARAM = 'MINIKUBE_IP'
DEFAULT_MINIO_PORT = 30103  # hard-coded from minio-config.yaml
GZIP_EXTENSION = '.gz'

_LOG = get_logger(__name__)

S3_NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9!-_.*()]')


class S3Url:
    def __init__(self, s3_url: str):
        self._parsed: Url = parse_url(s3_url)

    @property
    def bucket(self) -> str:
        return self._parsed.host

    @property
    def key(self) -> str:
        return self._parsed.path.lstrip('/') if self._parsed.path else None

    @property
    def url(self) -> str:
        return self._parsed.url


class S3Client:
    IS_DOCKER = os.getenv(ENV_SERVICE_MODE) == DOCKER_SERVICE_MODE

    class ObjectMetadata(TypedDict):
        Key: str
        LastModified: datetime
        ETag: str
        ChecksumAlgorithm: List[str]
        Size: int
        StorageClass: str
        Owner: Dict[str, str]
        RestoreStatus: Dict

    @staticmethod
    def safe_key(key: str) -> str:
        return re.sub(S3_NOT_AVAILABLE, '-', key)

    def __init__(self, region):
        self.region = region
        self._client = None

    def build_config(self) -> Config:
        config = Config(retries={
            'max_attempts': 10,
            'mode': 'standard'
        })
        if self.IS_DOCKER:
            config = config.merge(Config(s3={
                'signature_version': 's3v4',
                'addressing_style': 'path'
            }))
        return config

    def _init_clients(self):
        config = self.build_config()
        if self.IS_DOCKER:
            host, port = os.getenv(ENV_MINIO_HOST), os.getenv(ENV_MINIO_PORT)
            access_key = os.getenv(ENV_MINIO_ACCESS_KEY)
            secret_access_key = os.getenv(ENV_MINIO_SECRET_ACCESS_KEY)
            assert (host and port and access_key and secret_access_key), \
                f"\'{ENV_MINIO_HOST}\', \'{ENV_MINIO_PORT}\', " \
                f"\'{ENV_MINIO_ACCESS_KEY}\', " \
                f"\'{ENV_MINIO_SECRET_ACCESS_KEY}\' envs must be specified " \
                f"for on-prem"
            url = f'http://{host}:{port}'
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key
            )
            self._client = session.client('s3', endpoint_url=url,
                                          config=config)
            _LOG.info('Minio connection was successfully initialized')
        else:  # saas
            self._client = boto3.client('s3', self.region, config=config)
            _LOG.info('S3 connection was successfully initialized')

    @property
    def client(self):
        if not self._client:
            self._init_clients()
        return self._client

    @staticmethod
    def _gz_key(key: str, is_zipped: bool = True) -> str:
        if not is_zipped:
            return key
        if not key.endswith(GZIP_EXTENSION):
            key = key.strip('.') + GZIP_EXTENSION
        return key

    def create_bucket(self, bucket_name, region=None):
        region = region or self.region
        self.client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': region
            }
        )

    def file_exists(self, bucket_name, key):
        """Checks if object with the given key exists in bucket, if not
        compressed version exists, compresses and rewrites"""

        if self._file_exists(bucket_name, self._gz_key(key)):
            _LOG.info(f'Gzipped version of the file \'{key}\' exists')
            return True
        elif self._file_exists(bucket_name, key):
            _LOG.warning(f'Not gzipped version of file \'{key}\' exists. '
                         f'Compressing')
            self._gzip_object(bucket_name, key)
            return True
        else:
            return False

    def _file_exists(self, bucket_name: str, key: str) -> bool:
        obj = next(
            self.list_objects(bucket_name=bucket_name, prefix=key, max_keys=1),
            None
        )
        if not obj:
            return False
        return obj['Key'] == key

    def _gzip_object(self, bucket_name: str, key: str) -> io.BytesIO:
        """Replaces a file with gzipped version.
        And incidentally returns file's content"""
        try:
            response = self.client.get_object(
                Bucket=bucket_name,
                Key=key
            )
        except ClientError as e:
            if isinstance(e, ClientError) and \
                    e.response['Error']['Code'] == 'NoSuchKey':
                _LOG.warning(
                    f'There is no \'{key}\' file in bucket {bucket_name}')
                return
            raise e
        buf = io.BytesIO(response.get('Body').read())
        _LOG.info(f'Putting the compressed version of file \'{key}\'')
        self.put_object(bucket_name, key, buf.read())
        _LOG.info(f'Removing the old not compressed version of the '
                  f'file \'{key}\'')
        self.client.delete_object(Bucket=bucket_name, Key=key)
        buf.seek(0)
        return buf

    def put_object(self, bucket_name: str, object_name: str,
                   body: Union[str, bytes], is_zipped: bool = True):
        object_name = self._gz_key(object_name, is_zipped)
        try:
            if not is_zipped:
                return self.client.put_object(
                    Body=body, Bucket=bucket_name, Key=object_name,
                    ContentEncoding='utf-8')

            buf = io.BytesIO()
            with GzipFile(fileobj=buf, mode='wb') as f:
                f.write(body.encode() if not isinstance(body, bytes) else body)
            buf.seek(0)
            return self.client.put_object(Body=buf, Bucket=bucket_name,
                                          Key=object_name)
        except (Exception, BaseException) as e:
            _LOG.error(f'Putting data inside of an {object_name} object, '
                       f'within the {bucket_name} bucket, has triggered an'
                       f' exception - {e}.')

    def is_bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if specified bucket exists.
        :param bucket_name: name of the bucket to check;
        :return: True is exists, otherwise - False
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise e
            return False

    def list_buckets(self):
        response = self.client.list_buckets()
        return [bucket['Name'] for bucket in response.get("Buckets")]

    def get_json_file_content(self, bucket_name: str,
                              full_file_name: str) -> dict:
        """
        Returns content of the object.
        :param bucket_name: name of the bucket.
        :param full_file_name: name of the file including its folders.
            Example: /folder1/folder2/file_name.json
        :return: content of the file loaded to json
        """
        content = self.get_file_content(
            bucket_name, full_file_name, decode=True)
        return json.loads(content) if content else {}

    def get_file_content(self, bucket_name: str, full_file_name: str,
                         decode: bool = False) -> Union[str, bytes]:
        """
        Returns content of the object.
        :param bucket_name: name of the bucket.
        :param full_file_name: name of the file including its folders.
            Example: /folder1/folder2/file_name.json
        :param decode: flag
        :return: content of the file
        """
        response_stream = self.get_decompressed_stream(bucket_name,
                                                       full_file_name)
        if not response_stream:
            _LOG.warning(
                f'No gzip file found for \'{self._gz_key(full_file_name)}\'. '
                f'Trying to reach not the gzipped version')
            response_stream = self._gzip_object(bucket_name, full_file_name)
            if not response_stream:
                return
        _LOG.info(f'Resource stream of {full_file_name} within '
                  f'{bucket_name} has been established.')
        if decode:
            return response_stream.read().decode(UTF_8_ENCODING)
        return response_stream.read()

    def get_file_stream(self, bucket_name: str,
                        full_file_name: str) -> Union[StreamingBody, None]:
        """Returns boto3 stream with file content. If the file is not found,
        returns None. The given file name is not converted to gz.
        Use this method as a raw version based on which you build your
        own logic"""
        try:
            response = self.client.get_object(
                Bucket=bucket_name,
                Key=full_file_name
            )
            return response.get('Body')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise e

    def get_decompressed_stream(self, bucket_name: str,
                                full_file_name: str) -> Union[GzipFile, None]:
        stream = self.get_file_stream(bucket_name,
                                      self._gz_key(full_file_name))
        if not stream:
            return
        return GzipFile(fileobj=stream)

    def list_objects(self, bucket_name: str, prefix: Optional[str] = None,
                     max_keys: Optional[int] = None,
                     delimiter: Optional[str] = None
                     ) -> Generator[ObjectMetadata, None, None]:
        params = dict(Bucket=bucket_name)
        if max_keys:
            params.update(MaxKeys=max_keys)
        if prefix:
            params.update(Prefix=prefix)
        if delimiter:
            params.update(Delimiter=delimiter)
        is_truncated = True
        while is_truncated:
            response = self.client.list_objects_v2(**params)
            yield from response.get('Contents') or []
            limit = params.get('MaxKeys')
            n_returned = len(response.get('Contents') or [])
            if limit and n_returned == limit:
                return
            # either no limit or n_returned < limit
            if limit:
                params.update(MaxKeys=limit - n_returned)
            is_truncated = response['IsTruncated']
            params['ContinuationToken'] = response.get('NextContinuationToken')

    def delete_file(self, bucket_name: str, file_key: str):
        # TODO https://github.com/boto/boto3/issues/759, when the bug is
        #  fixed, use response statusCode instead of client.file_exists
        gzip_file_key = self._gz_key(file_key)
        if self._file_exists(bucket_name, gzip_file_key):
            self.client.delete_object(Bucket=bucket_name, Key=gzip_file_key)
        else:
            _LOG.warning(f'File {gzip_file_key} was not found during '
                         f'removing. Maybe it has not been compressed yet. '
                         f'Trying to remove the not compressed version')
            self.client.delete_object(Bucket=bucket_name, Key=gzip_file_key)

    def generate_presigned_url(self, bucket_name, full_file_name,
                               client_method='get_object', http_method='GET',
                               expires_in_sec=300, force_private_ip=False):
        """Do not forget to use client.file_exists before using this method"""
        url = self.client.generate_presigned_url(
            ClientMethod=client_method,
            Params={
                'Bucket': bucket_name,
                'Key': self._gz_key(full_file_name),
            },
            ExpiresIn=expires_in_sec,
            HttpMethod=http_method
        )
        if self.IS_DOCKER and not force_private_ip:
            ipv4 = self._get_public_ipv4()
            minikube_ip = self._get_minikube_ipv4()
            if ipv4:
                _LOG.info(f'Public ip: {ipv4} was received. Replacing the '
                          f'domain in the presigned url')
                parsed = urlparse(url)
                return parsed._replace(netloc=parsed.netloc.replace(
                    parsed.hostname, ipv4)).geturl()
            elif minikube_ip:
                _LOG.info(f'Minikube ip: {minikube_ip} was received. '
                          f'Replacing the domain in the presigned url')
                # return url
                parsed = urlparse(url)
                return parsed._replace(netloc=minikube_ip).geturl()
        return url

    def list_dir(self, bucket_name: str,
                 key: Optional[str] = None,
                 max_keys: Optional[int] = None,
                 delimiter: Optional[str] = None
                 ) -> Generator[str, None, None]:
        """
        Yields just keys
        :param max_keys:
        :param bucket_name:
        :param key:
        :param delimiter:
        :return:
        """
        yield from (obj['Key'] for obj in self.list_objects(
            bucket_name=bucket_name,
            prefix=key,
            max_keys=max_keys,
            delimiter=delimiter
        ))

    @staticmethod
    def _get_public_ipv4():
        """Tries to retrieve a public ipv4 from EC2 instance metadata"""
        try:
            _LOG.info('Trying to receive a public IP v4 from EC2 metadata')
            return IMDSFetcher(timeout=0.5)._get_request(
                "/latest/meta-data/public-ipv4", None).text
        except (_RetriesExceededError, Exception) as e:
            _LOG.warning(f'An IP v4 from EC2 metadata was not received: {e}')
        # try:
        #     from requests import get, RequestException
        #     return get('https://api.ipify.org').content.decode('utf8')
        # except RequestException:
        #     pass
        _LOG.info('No public IP was received. Returning None...')
        return

    @staticmethod
    def _get_minikube_ipv4(port=None):
        """Tries to retrieve the minikubes's api address and port"""
        port = port or DEFAULT_MINIO_PORT
        _LOG.info('Trying to receive the minikube`s ip')
        ip = os.getenv(MINIKUBE_IP_PARAM)
        if ip:
            return f'{ip}:{port}'
        _LOG.info('Minikube ip not found in environ. '
                  'Maybe the service is running beyond the Cluster?')
        process = Popen('minikube ip'.split(), stdout=PIPE)
        output, error = process.communicate()
        output = output.decode().strip()
        if not error and ipaddress.ip_address(output):
            _LOG.info('`minikube ip` has executed successfully. Return result')
            return f'{output}:{port}'

    @coroutine
    def get_json_batch(self, bucket_name: str, keys: Iterable[str]
                       ) -> Generator[Tuple[str, Dict], None, None]:
        """
        When you create this generator object it immediately starts
        downloading items, and you can iterate over results when you need.

        :param bucket_name:
        :param keys:
        :return:
        """

        def _process(*args, **kwargs):
            try:
                return self.get_json_file_content(*args, **kwargs)
            except ClientError as e:
                _LOG.warning(f'A client error was caught while getting the '
                             f'content of a file: {e}')
                return {}

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(_process, bucket_name, key): key
                for key in keys
            }
            yield  # remove this and coroutine decorator in case something wrong
            for future in as_completed(futures):
                yield futures[future], future.result()

    def put_objects_batch(self, bucket_name: str,
                          key_body: Iterable[Tuple[str, Union[str, bytes]]]):
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self.put_object, bucket_name, *pair): pair[0]
                for pair in key_body
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    future.result()
                except (ClientError, Exception) as e:
                    _LOG.warning(f'Cloud not upload file \'{key}\': {e}')


class ModularAssumeRoleS3Service(S3Client):
    from modular_sdk.services.aws_creds_provider import ModularAssumeRoleClient
    client = ModularAssumeRoleClient('s3')

    def __init__(self, region):
        super().__init__(region=region)
