import os

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from typing_extensions import Self

from helpers.constants import Env
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class Boto3ClientFactory:
    _session = None

    def __init__(self, service: str):
        self._service = service

    @classmethod
    def get_session(cls) -> 'Session':
        if cls._session is None:
            _LOG.info(f'Creating boto3 Session in pid {os.getpid()}')
            cls._session = Session()
        return cls._session

    def build(
        self,
        region_name: str | None = None,
        endpoint_url: str | None= None,
        aws_access_key_id: str | None= None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None= None,
        config: Config | None = None,
    ) -> BaseClient:
        _LOG.info(f'Creating boto3 client {self._service} in pid {os.getpid()}')
        return self.get_session().client(
            service_name=self._service,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            config=config,
        )

    def build_resource(
        self,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        config: Config | None = None,
    ) -> ServiceResource:
        _LOG.info(f'Creating boto3 resource {self._service} in pid {os.getpid()}')
        return self.get_session().resource(
            service_name=self._service,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            config=config,
        )

    def from_keys(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
        region_name: str | None = None,
    ) -> BaseClient:
        return self.build(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )


class Boto3ClientWrapper:
    _client = None
    _resource = None
    service_name: str = None

    @classmethod
    def build(cls) -> Self:
        return cls()

    @property
    def client(self) -> 'BaseClient':
        if self._client is None:
            self._client = Boto3ClientFactory(self.service_name).build(
                region_name=Env.AWS_REGION.get()
            )
        return self._client

    @client.setter
    def client(self, value: 'BaseClient'):
        assert (
            isinstance(value, BaseClient)
            and value.meta.service_model.service_name == self.service_name
        )
        self._client = value

    @property
    def resource(self) -> 'ServiceResource':
        if self._resource is None:
            self._resource = Boto3ClientFactory(self.service_name).build_resource(
                region_name=Env.AWS_REGION.get()
            )
        return self._resource

    @resource.setter
    def resource(self, value: ServiceResource):
        assert (
                isinstance(value, ServiceResource)
                and value.meta.service_name == self.service_name
        )
        self._resource = value
