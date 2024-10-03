from abc import ABC, abstractmethod
import json
import re

from botocore.client import ClientError

from helpers.constants import CAASEnv
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services import cache
from services.clients import Boto3ClientFactory
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


SecretValue = list | dict | str


SSM_NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9\/_.-]')


class AbstractSSMClient(ABC):
    @abstractmethod
    def get_secret_value(self, secret_name: str) -> SecretValue | None:
        ...

    @abstractmethod
    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString', ttl: int | None = None
                      ) -> bool:
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

    @staticmethod
    def prepare_name(name: str, prefix: str = 'custodian') -> str:
        safe_name = str(re.sub(SSM_NOT_AVAILABLE, '-', name))
        timestamp = utc_datetime().strftime('%m.%d.%Y.%H.%M.%S')
        return f'{prefix}.{safe_name}.{timestamp}'


class VaultSSMClient(AbstractSSMClient):
    mount_point = 'kv'
    key = 'data'

    def __init__(self):
        self._client = None  # hvac.Client

    def _init_client(self):
        import hvac
        token = CAASEnv.VAULT_TOKEN.get()
        endpoint = CAASEnv.VAULT_ENDPOINT.get()
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
        from hvac.exceptions import InvalidPath
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=secret_name, mount_point=self.mount_point) or {}
        except InvalidPath:
            return
        val = response.get('data', {}).get('data', {}).get(self.key)
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                pass
        return val

    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString', ttl: int | None = None
                      ) -> bool:
        self.client.secrets.kv.v2.create_or_update_secret(
            path=secret_name,
            secret={self.key: secret_value},
            mount_point=self.mount_point
        )
        if ttl:
            self.client.secrets.kv.v2.update_metadata(
                path=secret_name,
                delete_version_after=f'{int(ttl)}s',
                mount_point=self.mount_point
            )
        return True

    def delete_parameter(self, secret_name: str) -> bool:
        return bool(self.client.secrets.kv.v2.delete_metadata_and_all_versions(
            path=secret_name, mount_point=self.mount_point))

    def enable_secrets_engine(self, mount_point=None):
        from hvac.exceptions import InvalidRequest
        try:
            self.client.sys.enable_secrets_engine(
                backend_type='kv',
                path=(mount_point or self.mount_point),
                options={'version': 2}
            )
            return True
        except InvalidRequest:
            return False  # already exists

    def is_secrets_engine_enabled(self, mount_point=None) -> bool:
        mount_points = self.client.sys.list_mounted_secrets_engines()
        target_point = mount_point or self.mount_point
        return f'{target_point}/' in mount_points


class SSMClient(AbstractSSMClient):
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = Boto3ClientFactory('ssm').build(
                region_name=self._environment_service.aws_region()
            )
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
                      secret_type='SecureString', ttl: int | None = None
                      ) -> bool:
        try:
            if isinstance(secret_value, (list, dict)):
                secret_value = json.dumps(secret_value,
                                          sort_keys=True,
                                          separators=(",", ":"))
            self.client.put_parameter(
                Name=secret_name,
                Value=secret_value,
                Overwrite=True,
                Type=secret_type
            )
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


class CachedSSMClient(AbstractSSMClient):
    def __init__(self, client: AbstractSSMClient):
        self._cl = client
        self._cache = cache.factory()

    @property
    def client_without_cache(self) -> AbstractSSMClient:
        return self._cl

    @property
    def client(self):
        return self._cl.client

    def get_secret_value(self, secret_name: str) -> SecretValue | None:
        if val := self._cache.get(secret_name):
            _LOG.debug('Returning cached ssm value')
            return val
        secret = self._cl.get_secret_value(secret_name)
        if secret:
            _LOG.debug('Saving ssm value to cache')
            self._cache[secret_name] = secret
        return secret

    def create_secret(self, secret_name: str, secret_value: SecretValue,
                      secret_type='SecureString', ttl: int | None = None
                      ) -> bool:
        # do not use cache here because the secret_value that we send to
        # this method can differ from the one returned from cl.get_secret_
        # value (for example, vault will convert bytes to str)
        # this can be fixed, but currently just do not cache on create
        self._cache.pop(secret_name, None)
        return self._cl.create_secret(secret_name, secret_value,
                                      secret_type, ttl)

    def delete_parameter(self, secret_name: str) -> bool:
        result = self._cl.delete_parameter(secret_name)
        if result:
            self._cache.pop(secret_name, None)
        return result

    def enable_secrets_engine(self, mount_point=None):
        return self._cl.enable_secrets_engine(mount_point)

    def is_secrets_engine_enabled(self, mount_point=None) -> bool:
        return self._cl.is_secrets_engine_enabled(mount_point)
