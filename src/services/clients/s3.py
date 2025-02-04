import gzip
import io
import mimetypes
import re
import shutil
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import BinaryIO, Generator, Iterable, Optional, TypedDict, cast

import msgspec
from botocore.config import Config
from botocore.exceptions import ClientError
from modular_sdk.services.aws_creds_provider import ModularAssumeRoleClient
from urllib3.util import Url, parse_url

from helpers.constants import CAASEnv
from helpers.log_helper import get_logger
from services import cache
from services.clients import (
    Boto3ClientFactory,
    Boto3ClientWrapper,
    Boto3ClientWrapperFactory,
)

_LOG = get_logger(__name__)

Json = dict | list | str | int | float | tuple


class S3Url:
    __slots__ = ('_parsed',)

    def __init__(self, s3_url: str):
        self._parsed: Url = parse_url(s3_url)
        assert isinstance(self._parsed.path, str) and self._parsed.path.lstrip(
            '/'
        ), 'Bucket key cannot be empty'

    @property
    def bucket(self) -> str:
        return self._parsed.host

    @property
    def key(self) -> str:
        return self._parsed.path.lstrip('/')

    @property
    def url(self) -> str:
        return f's3://{self.bucket}/{self.key}'

    def __str__(self) -> str:
        return self.url

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.url})'

    @classmethod
    def build(cls, bucket: str, key: str) -> 'S3Url':
        return cls(f's3://{bucket.strip()}/{key.lstrip("/")}')


class S3ClientWrapperFactory(Boto3ClientWrapperFactory['S3Client']):
    @classmethod
    def _base_config(cls) -> Config:
        return Config(retries={'max_attempts': 10, 'mode': 'standard'})

    @classmethod
    def _minio_config(cls) -> Config:
        return cls._base_config().merge(
            Config(
                s3={'signature_version': 's3v4', 'addressing_style': 'path'}
            )
        )

    def build_s3(self, region_name: str) -> 'S3Client':
        instance = self._wrapper.build()
        instance.resource = Boto3ClientFactory(
            instance.service_name
        ).build_resource(region_name=region_name, config=self._base_config())
        instance.client = instance.resource.meta.client
        _LOG.info('S3 connection was successfully initialized')
        return instance

    def build_minio(self) -> 'S3Client':
        endpoint = CAASEnv.MINIO_ENDPOINT.get()
        access_key = CAASEnv.MINIO_ACCESS_KEY_ID.get()
        secret_key = CAASEnv.MINIO_SECRET_ACCESS_KEY.get()
        assert endpoint and access_key and secret_key, (
            'Minio endpoint, access key and secret key must be '
            'provided for on-prem'
        )

        instance = self._wrapper.build()
        instance.resource = Boto3ClientFactory(
            instance.service_name
        ).build_resource(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint,
            config=self._minio_config(),
        )
        instance.client = instance.resource.meta.client
        _LOG.info('Minio connection was successfully initialized')
        return instance


class S3Client(Boto3ClientWrapper):
    """
    Most methods have their gz equivalent with prefix gz_. Such methods
    add .gz to bucket key, compress/decompress content (if the method
    interacts with content) and add gzip ContentEncoding to metadata
    """

    service_name = 's3'
    s3_not_available = re.compile(r'[^a-zA-Z0-9!-_.*()]')
    _enc = msgspec.json.Encoder()
    _dec = msgspec.json.Decoder()

    def __init__(self):
        self._ipv4_cache = cache.TTLCache(maxsize=2, ttl=3600)

    class Bucket(TypedDict):
        Name: str
        CreationDate: datetime

    @classmethod
    def safe_key(cls, key: str) -> str:
        return re.sub(cls.s3_not_available, '-', key)

    @staticmethod
    def _gz_key(key: str) -> str:
        if not key.endswith('.gz'):
            key = key.strip('.') + '.gz'
        return key

    @classmethod
    def factory(cls) -> S3ClientWrapperFactory:
        return S3ClientWrapperFactory(cls)

    @staticmethod
    def _resolve_content_type(
        key: str, ct: str | None = None, ce: str | None = None
    ) -> tuple[str | None, str | None]:
        """
        Returns user provided content type and encoding. If something is not
        provided -> tries to resolve from key
        :param key:
        :param ct:
        :param ce:
        :return: (content_type, content_encoding)
        """
        if ct and ce:
            return ct, ce
        resolved_ct, resolved_ce = mimetypes.guess_type(key)
        ct = ct or resolved_ct
        ce = ce or resolved_ce
        return ct, ce

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes | BinaryIO | bytearray,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ):
        """
        Uploads the provided stream of bytes or raw bytes.
        :param bucket:
        :param key:
        :param body:
        :param content_type:
        :param content_encoding:
        :return:
        """
        ct, ce = self._resolve_content_type(
            key, content_type, content_encoding
        )
        params = {}
        if ct:
            params.update(ContentType=ct)
        if ce:
            params.update(ContentEncoding=ce)
        if isinstance(body, (bytes, bytearray)):
            body = io.BytesIO(body)
        return self.resource.Bucket(bucket).upload_fileobj(
            Fileobj=body, Key=key, ExtraArgs=params
        )

    def gz_put_object(
        self,
        bucket: str,
        key: str,
        body: bytes | BinaryIO | bytearray,
        gz_buffer: BinaryIO | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ):
        """
        Uploads the file adding .gz to the file extension and compressing the
        body
        :param bucket:
        :param key:
        :param body:
        :param gz_buffer: optional buffer to use for compression.
        Otherwise - in memory. Argument can be used for large files
        :param content_type:
        :param content_encoding:
        :return:
        """
        if not gz_buffer:
            gz_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz:
            if isinstance(body, (bytes, bytearray)):
                gz.write(body)
            else:
                shutil.copyfileobj(body, gz)
        gz_buffer.seek(0)
        return self.put_object(
            bucket,
            self._gz_key(key),
            gz_buffer,
            content_type,
            content_encoding,
        )

    def get_object(
        self, bucket: str, key: str, buffer: BinaryIO | None = None
    ) -> BinaryIO | None:
        """
        Downloads object to memory by default. Optional buffer can be provided.
        In case the key does not exist, None is returned
        :param bucket:
        :param key:
        :param buffer:
        :return:
        """
        if not buffer:
            buffer = io.BytesIO()
        try:
            self.resource.Bucket(bucket).download_fileobj(
                Key=key, Fileobj=buffer
            )
        except ClientError as e:
            if e.response['Error']['Code'] in ('NoSuchKey', '404'):
                return
            _LOG.exception(
                f'Unexpected error occurred in '
                f'get_object: s3://{bucket}/{key}'
            )
            raise e
        buffer.seek(0)
        return buffer

    def gz_get_object(
        self,
        bucket: str,
        key: str,
        buffer: io.BytesIO | None = None,
        gz_buffer: io.BytesIO | None = None,
    ) -> io.BytesIO | None:
        """
        :param bucket:
        :param key:
        :param buffer:
        :param gz_buffer: can be optionally provided to use as a buffer for
        compression. You can provide some temp file in case the size of body
        is expected to be large
        :return:
        """
        if not gz_buffer:
            gz_buffer = io.BytesIO()
        stream = self.get_object(bucket, self._gz_key(key), gz_buffer)
        if not stream:
            return
        gz_buffer.seek(0)
        if not buffer:
            buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buffer, mode='rb') as gz:
            shutil.copyfileobj(gz, buffer)
        buffer.seek(0)
        return buffer

    def put_json(self, bucket: str, key: str, obj: Json):
        return self.put_object(
            bucket=bucket,
            key=key,
            body=self._enc.encode(obj),
            content_type='application/json',
        )

    def gz_put_json(self, bucket: str, key: str, obj: Json):
        # ignoring key, cause you specifically used this method.
        # So it must be json and gzip
        return self.gz_put_object(
            bucket=bucket,
            key=key,
            body=self._enc.encode(obj),
            content_type='application/json',
            content_encoding='gzip',
        )

    def get_json(self, bucket: str, key: str) -> Json:
        body = self.get_object(bucket, key)
        if not body:
            return {}
        result = self._dec.decode(cast(io.BytesIO, body).getvalue())
        body.close()
        return result

    def gz_get_json(self, bucket: str, key: str) -> Json:
        body = self.gz_get_object(bucket, key)
        if not body:
            return {}
        result = self._dec.decode(cast(io.BytesIO, body).getvalue())
        body.close()
        return result

    def delete_object(self, bucket: str, key: str):
        self.client.delete_object(Bucket=bucket, Key=key)

    def gz_delete_object(self, bucket: str, key: str):
        self.delete_object(bucket, self._gz_key(key))

    def object_meta(self, bucket: str, key: str):
        obj = self.resource.Object(bucket, key)
        try:
            obj.load()
            return obj
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return
            raise e

    def object_exists(self, bucket: str, key: str) -> bool:
        # or better use list_objects with limit 1
        return bool(self.object_meta(bucket, key))

    def gz_object_exists(self, bucket: str, key: str) -> bool:
        return self.object_exists(bucket, self._gz_key(key))

    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        start_after: Optional[str] = None,
    ) -> Iterable:
        params = dict()
        if prefix:
            params.update(Prefix=prefix)
        if start_after:
            params.update(Marker=start_after)
        it = self.resource.Bucket(bucket).objects.filter(**params)
        if page_size is not None:
            it = it.page_size(page_size)
        if limit is not None:
            it = it.limit(limit)
        return it

    def list_dir(
        self,
        bucket_name: str,
        key: Optional[str] = None,
        page_size: Optional[int] = None,
        limit: Optional[int] = None,
        start_after: Optional[str] = None,
    ) -> Generator[str, None, None]:
        """
        Yields just keys
        :param bucket_name:
        :param key:
        :param page_size:
        :param limit:
        :param start_after:
        :return:
        """
        yield from (
            obj.key
            for obj in self.list_objects(
                bucket=bucket_name,
                prefix=key,
                page_size=page_size,
                limit=limit,
                start_after=start_after,
            )
        )

    def common_prefixes(
        self,
        bucket: str,
        delimiter: str,
        prefix: Optional[str] = None,
        start_after: Optional[str] = None,
    ) -> Generator[str, None, None]:
        paginator = self.client.get_paginator('list_objects_v2')
        params = dict(Bucket=bucket, Delimiter=delimiter)
        if prefix:
            params.update(Prefix=prefix)
        if start_after:
            params.update(StartAfter=start_after)
        for item in paginator.paginate(**params):
            for prefix in item.get('CommonPrefixes') or []:
                yield prefix.get('Prefix')

    def create_bucket(self, bucket: str, region: str):
        self.client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={'LocationConstraint': region},
        )

    def bucket_exists(self, bucket: str) -> bool:
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise e
            return False

    def list_buckets(self) -> Generator[Bucket, None, None]:
        yield from (self.client.list_buckets().get('Buckets') or [])

    def copy(
        self,
        bucket: str,
        key: str,
        destination_bucket: str,
        destination_key: str,
    ):
        self.client.copy(
            CopySource=dict(Bucket=bucket, Key=key),
            Bucket=destination_bucket,
            Key=destination_key,
        )

    def download_url(
        self,
        bucket: str,
        key: str,
        expires_in: timedelta = timedelta(seconds=300),
        filename: Optional[str] = None,
        response_encoding: Optional[str] = None,
    ) -> str:
        """
        :param bucket:
        :param key:
        :param expires_in:
        :param filename: custom filename for the file that will be downloaded
        :param response_encoding: by default uses encoding from object meta.
        :return:
        """
        disposition = 'attachment;'
        if filename:
            disposition += f';filename="{filename}"'
        params = {
            'Bucket': bucket,
            'Key': key,
            'ResponseContentDisposition': disposition,
        }
        if response_encoding:
            params['ResponseContentEncoding'] = response_encoding
        return self.client.generate_presigned_url(
            ClientMethod='get_object',
            Params=params,
            ExpiresIn=expires_in.seconds,
        )

    def gz_download_url(
        self,
        bucket: str,
        key: str,
        expires_in: timedelta = timedelta(seconds=300),
        filename: Optional[str] = None,
        response_encoding: str = None,
    ) -> str:
        """
        Prefix gz only impact the key here
        :param bucket:
        :param key:
        :param expires_in:
        :param filename:
        :param response_encoding:
        :return:
        """
        return self.download_url(
            bucket, self._gz_key(key), expires_in, filename, response_encoding
        )

    def put_path_expiration(self, bucket: str, rules: list[tuple[str, int]]):
        """
        Creates a lifecycle rule with expiration for the given prefixes
        :return:
        """
        return self.client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                'Rules': [
                    {
                        'Expiration': {'Days': days},
                        'Filter': {'Prefix': key},
                        'Status': 'Enabled',
                    }
                    for key, days in rules
                ]
            },
        )

    @cache.cachedmethod(lambda self: self._ipv4_cache)
    def _resolve_instance_public_ipv4(self) -> str | None:
        _LOG.info('Trying to resolve instance ipv4')
        token = None
        try:
            _LOG.info('Getting imds token')
            req = urllib.request.Request(
                'http://169.254.169.254/latest/api/token',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '30'},
                method='PUT',
            )
            with urllib.request.urlopen(req, timeout=1) as resp:
                token = resp.read().decode()
        except TimeoutError:
            _LOG.warning('Could not get imds token. Timeout')
        except urllib.error.URLError:
            _LOG.warning('Could not get imds token')
        try:
            req = urllib.request.Request(
                'http://169.254.169.254/latest/meta-data/public-ipv4',
                headers={'X-aws-ec2-metadata-token': token} if token else {},
            )
            with urllib.request.urlopen(req, timeout=1) as resp:
                return resp.read().decode()
        except TimeoutError:
            _LOG.warning('Could not get imds token. Timeout')
        except urllib.error.URLError:
            _LOG.warning('Cannot resolve public-ipv4 from instance metadata')

    def prepare_presigned_url(self, url: str, host: str | None = None) -> str:
        parsed: Url = parse_url(url)
        if host:
            new_host = host
        elif _env := CAASEnv.MINIO_PRESIGNED_URL_HOST.get():
            new_host = _env
        elif ipv4 := self._resolve_instance_public_ipv4():
            new_host = ipv4
        elif parsed.host == 'minio':  # exception for docker
            new_host = '127.0.0.1'
        else:
            new_host = parsed.host
        return Url(
            scheme=parsed.scheme,
            auth=parsed.auth,
            host=new_host,
            port=parsed.port,
            path=parsed.path,
            query=parsed.query,
            fragment=parsed.fragment,
        ).url


class ModularAssumeRoleS3Service(S3Client):
    client = ModularAssumeRoleClient('s3')

    # probably actions that require resource won't work

    # the implementation in S3Client uses s3 resource to handle multipart
    # upload if necessary. Currently, we can access only client here, so
    # this implementation
    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes | BinaryIO | bytearray,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ):
        ct, ce = self._resolve_content_type(
            key, content_type, content_encoding
        )
        params = dict(Bucket=bucket, Key=key, Body=body)
        if ct:
            params.update(ContentType=ct)
        if ce:
            params.update(ContentEncoding=ce)
        return self.client.put_object(**params)
