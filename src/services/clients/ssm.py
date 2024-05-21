import json
import os
from abc import ABC, abstractmethod

import boto3
from botocore.client import ClientError

from helpers.constants import CAASEnv
from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

SecretValue = list | dict | str


class AbstractSSMClient(ABC):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    @abstractmethod
    def get_secret_value(self, secret_name: str) -> SecretValue | None:
        ...

    @abstractmethod
    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString') -> bool:
        ...

    @abstractmethod
    def delete_parameter(self, secret_name: str) -> bool:
        ...

    @abstractmethod
    def enable_secrets_engine(self, mount_point=None):
        ...

    @abstractmethod
    def is_secrets_engine_enabled(self, mount_point=None) -> bool:
        ...


class VaultSSMClient(AbstractSSMClient):
    mount_point = 'kv'
    key = 'data'

    def __init__(self, environment_service: EnvironmentService):
        super().__init__(environment_service)
        self._client = None  # hvac.Client

    def _init_client(self):
        import hvac
        token = os.getenv(CAASEnv.VAULT_TOKEN)
        endpoint = os.getenv(CAASEnv.VAULT_ENDPOINT)
        assert token and endpoint, ('Vault endpoint and token must '
                                    'be specified for on-prem')
        _LOG.info('Initializing hvac client')
        self._client = hvac.Client(url=endpoint, token=token)
        _LOG.info('Hvac client was initialized')

    @property
    def client(self):
        if not self._client:
            self._init_client()
        return self._client

    def get_secret_value(self, secret_name: str) -> SecretValue | None:
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=secret_name, mount_point=self.mount_point) or {}
        except Exception:  # hvac.InvalidPath
            return
        val = response.get('data', {}).get('data', {}).get(self.key)
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                pass
        return val

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

    def enable_secrets_engine(self, mount_point=None):
        try:
            self.client.sys.enable_secrets_engine(
                backend_type='kv',
                path=(mount_point or self.mount_point),
                options={'version': 2}
            )
            return True
        except Exception:  # hvac.exceptions.InvalidRequest
            return False  # already exists

    def is_secrets_engine_enabled(self, mount_point=None) -> bool:
        mount_points = self.client.sys.list_mounted_secrets_engines()
        target_point = mount_point or self.mount_point
        return f'{target_point}/' in mount_points


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

    def get_secret_value(self, secret_name: str):
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

    def create_secret(self, secret_name: str,
                      secret_value: str | list | dict,
                      secret_type='SecureString') -> bool:
        try:
            if isinstance(secret_value, (list, dict)):
                secret_value = json.dumps(secret_value,
                                          sort_keys=True,
                                          separators=(",", ":"))
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
        pass

    def is_secrets_engine_enabled(self, mount_point=None) -> bool:
        return True
