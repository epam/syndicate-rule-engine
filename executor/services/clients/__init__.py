from abc import abstractmethod, ABC
from typing import Optional, Generic, TypeVar, TYPE_CHECKING

from boto3.session import Session
from botocore.client import BaseClient

if TYPE_CHECKING:
    from typing_extensions import Self


class Boto3ClientFactory:
    _session = Session()  # class variable

    def __init__(self, service: str):
        self._service = service

    def build(self, region_name: Optional[str] = None) -> BaseClient:
        return self._session.client(
            service_name=self._service,
            region_name=region_name
        )

    def from_keys(self, aws_access_key_id: str, aws_secret_access_key: str,
                  aws_session_token: Optional[str] = None,
                  region_name: Optional[str] = None) -> BaseClient:
        return self._session.client(
            service_name=self._service,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )


T = TypeVar('T')


class Boto3ClientWrapperFactory(Generic[T]):
    def __init__(self, client_class: T):
        self._wrapper = client_class

    def build(self, region_name: Optional[str] = None) -> T:
        instance = self._wrapper.build()
        instance.client = Boto3ClientFactory(instance.service_name).build(
            region_name)
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
    service_name: str = None

    @classmethod
    @abstractmethod
    def build(cls) -> 'Self':
        ...

    @property
    def client(self) -> BaseClient:
        return self._client

    @client.setter
    def client(self, value: BaseClient):
        assert isinstance(value, BaseClient) and \
               value.meta.service_model.service_name == self.service_name
        self._client = value

    @classmethod
    def factory(cls) -> Boto3ClientWrapperFactory['Self']:
        return Boto3ClientWrapperFactory(cls)
