import json
import os
from abc import ABC, abstractmethod
from typing import Optional, List, Dict
from typing import Union

import boto3
from botocore.client import ClientError

from helpers.constants import ENV_VAULT_HOST, ENV_VAULT_PORT, \
    ENV_VAULT_TOKEN, ENV_SERVICE_MODE, DOCKER_SERVICE_MODE
from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

SecretValue = Union[Dict, List, str]


class AbstractSSMClient(ABC):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    @abstractmethod
    def get_secret_value(self, secret_name: str) -> Optional[SecretValue]:
        ...

    @abstractmethod
    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString') -> bool:
        ...

    @abstractmethod
    def delete_parameter(self, secret_name: str) -> bool:
        ...

    @abstractmethod
    def get_secret_values(self, secret_names: List[str]
                          ) -> Optional[Dict[str, SecretValue]]:
        ...

    @abstractmethod
    def enable_secrets_engine(self, mount_point=None):
        ...


class VaultSSMClient(AbstractSSMClient):
    mount_point = 'kv'
    key = 'data'

    def __init__(self, environment_service: EnvironmentService):
        super().__init__(environment_service)
        self._client = None  # hvac.Client

    def _init_client(self):
        assert os.getenv(ENV_SERVICE_MODE) == DOCKER_SERVICE_MODE, \
            "You can init vault handler only if SERVICE_MODE=docker"
        import hvac
        vault_token = os.getenv(ENV_VAULT_TOKEN)
        vault_host = os.getenv(ENV_VAULT_HOST)
        vault_port = os.getenv(ENV_VAULT_PORT)
        _LOG.info('Initializing hvac client')
        self._client = hvac.Client(
            url=f'http://{vault_host}:{vault_port}',
            token=vault_token
        )
        _LOG.info('Hvac client was initialized')

    @property
    def client(self):
        if not self._client:
            self._init_client()
        return self._client

    def get_secret_value(self, secret_name: str) -> Optional[SecretValue]:
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=secret_name, mount_point=self.mount_point) or {}
        except Exception:  # hvac.InvalidPath
            return
        return response.get('data', {}).get('data', {}).get(self.key)

    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString') -> bool:
        return self.client.secrets.kv.v2.create_or_update_secret(
            path=secret_name,
            secret={self.key: secret_value},
            mount_point=self.mount_point
        )

    def delete_parameter(self, secret_name: str) -> bool:
        return bool(self.client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=secret_name, mount_point=self.mount_point))

    def get_secret_values(self, secret_names: List[str]
                          ) -> Optional[Dict[str, SecretValue]]:
        return {name: self.get_secret_value(name) for name in secret_names}

    def enable_secrets_engine(self, mount_point=None):
        try:
            self.client.sys.enable_secrets_engine(
                backend_type='kv', path=mount_point, options={'version': 2})
            return True
        except Exception:  # hvac.exceptions.InvalidRequest
            return False  # already exists


class SSMClient(AbstractSSMClient):
    def __init__(self, environment_service: EnvironmentService):
        super().__init__(environment_service)
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client(
                'ssm', self._environment_service.aws_region())
        return self._client

    def get_secret_value(self, secret_name):
        try:
            response = self.client.get_parameter(
                Name=secret_name,
                WithDecryption=True
            )
            value_str = response['Parameter']['Value']
            try:
                return json.loads(value_str)
            except json.decoder.JSONDecodeError:
                return value_str
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t get secret for name \'{secret_name}\', '
                       f'error code: \'{error_code}\'')

    def get_secret_values(self, secret_names: list):
        try:
            response = self.client.get_parameters(
                Names=secret_names,
                WithDecryption=True)
            parameters = {item.get('Name'): item.get('Value') for item in
                          response.get('Parameters')}
            return parameters
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t get secret for names \'{secret_names}\', '
                       f'error code: \'{error_code}\'')

    def create_secret(self, secret_name: str,
                      secret_value: Union[str, list, dict],
                      secret_type='SecureString') -> bool:
        try:
            if isinstance(secret_value, (list, dict)):
                secret_value = json.dumps(secret_value)
            self.client.put_parameter(
                Name=secret_name,
                Value=secret_value,
                Overwrite=True,
                Type=secret_type)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t put secret for name \'{secret_name}\', '
                       f'error code: \'{error_code}\'')
            return False

    def delete_parameter(self, secret_name: str) -> bool:
        try:
            self.client.delete_parameter(Name=secret_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            _LOG.error(f'Can\'t delete secret name \'{secret_name}\', '
                       f'error code: \'{error_code}\'')
            return False

    def enable_secrets_engine(self, mount_point=None):
        """No need to implement"""
