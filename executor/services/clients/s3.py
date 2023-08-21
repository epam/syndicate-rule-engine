import gzip
from gzip import GzipFile
import io
import json
import os
from typing import Union, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from helpers.constants import  ENV_MINIO_HOST, ENV_MINIO_PORT, \
    ENV_MINIO_ACCESS_KEY, ENV_MINIO_SECRET_ACCESS_KEY
from services.environment_service import EnvironmentService
from helpers.log_helper import get_logger

GZIP_EXTENSION = '.gz'
UTF_8_ENCODING = 'utf-8'

_LOG = get_logger(__name__)


class S3Client:
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service
        self._client = None
        self._resource = None

    @property
    def is_docker(self) -> bool:
        return self._environment.is_docker()

    def build_config(self) -> Config:
        config = Config(retries={
            'max_attempts': 10,
            'mode': 'standard'
        })
        if self.is_docker:
            config = config.merge(Config(s3={
                'signature_version': 's3v4',
                'addressing_style': 'path'
            }))
        return config

    def _init_clients(self):
        config = self.build_config()
        if self.is_docker:
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
            self._resource = session.resource('s3', endpoint_url=url,
                                              config=config)
            _LOG.info('Minio connection was successfully initialized')
        else:  # saas
            self._client = boto3.client(
                's3', self._environment.aws_region(), config=config)
            self._resource = boto3.resource(
                's3', self._environment.aws_region(), config=config)
            _LOG.info('S3 connection was successfully initialized')

    @property
    def client(self):
        if not self._client:
            self._init_clients()
        return self._client

    @property
    def resource(self):
        if not self._resource:
            self._init_clients()
        return self._resource

    @staticmethod
    def _gz_key(key: str) -> str:
        if not key.endswith(GZIP_EXTENSION):
            key = key.strip('.') + GZIP_EXTENSION
        return key

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

    def _file_exists(self, bucket_name, key):
        response = self.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=key,
            MaxKeys=1
        )
        for obj in response.get('Contents', []):
            if obj['Key'] == key:
                return True
        return False

    def _gzip_object(self, bucket_name: str, key: str) -> io.BytesIO:
        """Replaces a file with gzipped version.
        And incidentally returns file's content"""
        response = self.client.get_object(
            Bucket=bucket_name,
            Key=key
        )
        buf = io.BytesIO(response.get('Body').read())
        _LOG.info(f'Putting the compressed version of file \'{key}\'')
        self.put_object(bucket_name, key, buf.read())
        _LOG.info(f'Removing the old not compressed version of the '
                  f'file \'{key}\'')
        self.resource.Object(bucket_name, key).delete()
        buf.seek(0)
        return buf

    def put_object(self, bucket_name: str, object_name: str,
                   body: Union[str, bytes]):
        object_name = self._gz_key(object_name)
        s3_object = self.resource.Object(bucket_name, object_name)
        buf = io.BytesIO()
        with GzipFile(fileobj=buf, mode='wb') as f:
            f.write(body.encode() if not isinstance(body, bytes) else body)
        buf.seek(0)
        try:
            return s3_object.put(Body=buf)
        except ClientError as e:
            _LOG.error(f'An error occurred trying to put '
                       f'object {object_name} to s3')
            raise e

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

    def is_bucket_exists(self, bucket_name):
        """
        Check if specified bucket exists.
        :param bucket_name: name of the bucket to check;
        :return: True is exists, otherwise - False
        """
        existing_buckets = self._list_buckets()
        return bucket_name in existing_buckets

    def _list_buckets(self):
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
        return json.loads(self.get_file_content(
            bucket_name, full_file_name, decode=True))

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
        if decode:
            return response_stream.read().decode(UTF_8_ENCODING)
        return response_stream.read()

    def get_file_stream(self, bucket_name: str,
                        full_file_name: str) -> Union[io.IOBase, None]:
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

    def put_object_encrypted(self, bucket_name, object_name, body):
        body = gzip.compress(body.encode())
        return self.client.put_object(
            Body=body,
            Bucket=bucket_name,
            Key=self._gz_key(object_name),
            ServerSideEncryption='AES256')

    def list_objects(self, bucket_name, prefix=None):
        result_keys = []
        params = dict(Bucket=bucket_name)
        if prefix:
            params['Prefix'] = prefix
        response = self.client.list_objects_v2(**params)
        if not response.get('Contents'):
            return None
        result_keys.extend(item for item in response['Contents'])
        while response['IsTruncated'] is True:
            token = response['NextContinuationToken']
            params['ContinuationToken'] = token
            response = self.client.list_objects_v2(**params)
            result_keys.extend(item for item in response['Contents'])
        return result_keys

    def delete_file(self, bucket_name: str, file_key: str):
        # TODO https://github.com/boto/boto3/issues/759, when the bug is
        #  fixed, use response statusCode instead of client.file_exists
        gzip_file_key = self._gz_key(file_key)
        if self._file_exists(bucket_name, gzip_file_key):
            self.resource.Object(bucket_name, gzip_file_key).delete()
        else:
            _LOG.warning(f'File {gzip_file_key} was not found during '
                         f'removing. Maybe it has not been compressed yet. '
                         f'Trying to remove the not compressed version')
            self.resource.Object(bucket_name, file_key).delete()

    def get_decompressed_stream(self, bucket_name: str,
                                full_file_name: str) -> Union[GzipFile, None]:
        stream = self.get_file_stream(bucket_name,
                                      self._gz_key(full_file_name))
        if not stream:
            return
        return GzipFile(fileobj=stream)