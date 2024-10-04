from abc import ABC
from typing import Optional, Generic, TypeVar

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from typing_extensions import Self
from helpers.log_helper import get_logger
from helpers.constants import CAASEnv

_LOG = get_logger(__name__)


class Boto3ClientFactory:
    _session = Session()  # class variable

    def __init__(self, service: str, no_proxies: bool = False):
        self._service = service
        self._no_proxies = no_proxies

    def _build_default_config(self) -> Config:
        proxy = {}
        if url := CAASEnv.HTTP_PROXY.get():
            proxy['http'] = url
        if url := CAASEnv.HTTPS_PROXY.get():
            proxy['https'] = url
        if proxy and not self._no_proxies:
            return Config(proxies=proxy)
        return Config()

    def build(self, region_name: str = None, endpoint_url: str = None,
              aws_access_key_id: str = None, aws_secret_access_key: str = None,
              aws_session_token: str = None, config: Config = None,
              ) -> BaseClient:
        _LOG.info(f'Building boto3 client for {self._service}')
        conf = self._build_default_config()
        if config:
            conf = conf.merge(config)
        return self._session.client(
            service_name=self._service,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            config=conf
        )

    def build_resource(self, region_name: str = None, endpoint_url: str = None,
                       aws_access_key_id: str = None,
                       aws_secret_access_key: str = None,
                       aws_session_token: str = None, config: Config = None,
                       ) -> ServiceResource:
        _LOG.info(f'Building boto3 resource for {self._service}')
        conf = self._build_default_config()
        if config:
            conf = conf.merge(config)
        return self._session.resource(
            service_name=self._service,
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            config=conf
        )

    def from_keys(self, aws_access_key_id: str, aws_secret_access_key: str,
                  aws_session_token: Optional[str] = None,
                  region_name: Optional[str] = None) -> BaseClient:
        return self.build(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )


T = TypeVar('T')


class Boto3ClientWrapperFactory(Generic[T]):
    """
    Client wrapper means a class wrapper over raw boto3 client
    """

    def __init__(self, client_class: T):
        self._wrapper = client_class

    def build(self, region_name: Optional[str] = None) -> T:
        instance = self._wrapper.build()
        instance.client = Boto3ClientFactory(instance.service_name).build(
            region_name=region_name
        )
        return instance

    def from_keys(self, aws_access_key_id: str, aws_secret_access_key: str,
                  aws_session_token: Optional[str] = None,
                  region_name: Optional[str] = None) -> T:
        instance = self._wrapper.build()
        instance.client = Boto3ClientFactory(instance.service_name).from_keys(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )
        return instance


class Boto3ClientWrapper(ABC):
    _client = None
    _resource = None
    service_name: str = None

    @classmethod
    def build(cls) -> Self:
        return cls()

    @property
    def client(self) -> BaseClient:
        return self._client

    @client.setter
    def client(self, value: BaseClient):
        assert isinstance(value, BaseClient) and \
               value.meta.service_model.service_name == self.service_name
        self._client = value

    @property
    def resource(self) -> ServiceResource:
        return self._resource

    @resource.setter
    def resource(self, value: ServiceResource):
        assert isinstance(value, ServiceResource) and \
               value.meta.service_name == self.service_name
        self._resource = value

    @classmethod
    def factory(cls) -> Boto3ClientWrapperFactory[Self]:
        return Boto3ClientWrapperFactory(cls)
