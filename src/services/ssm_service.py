import re

from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services import cache
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)

SSM_NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9\/_.-]')


class SSMService:
    def __init__(self, client: AbstractSSMClient):
        self.client = client
        self.__secrets = cache.factory()

    def get_secret_value(self, secret_name):
        if val := self.__secrets.get(secret_name):
            _LOG.debug(f'Returning cached value: of {secret_name}')
            return val
        _LOG.debug(f'Requesting secret: {secret_name}')

        secret = self.client.get_secret_value(secret_name=secret_name)
        if secret:
            self.__secrets[secret_name] = secret
        return secret

    def create_secret_value(self, secret_name, secret_value):
        self.client.create_secret(secret_name=secret_name,
                                  secret_value=secret_value)

    def delete_secret(self, secret_name: str):
        self.client.delete_parameter(secret_name=secret_name)

    def enable_secrets_engine(self, mount_point=None):
        return self.client.enable_secrets_engine(mount_point)

    def is_secrets_engine_enabled(self, mount_point=None):
        return self.client.is_secrets_engine_enabled(mount_point)

    def save_data(self, name: str, value: str | dict,
                  prefix: str = 'custodian') -> str:
        """
        Creates safe ssm parameter name and saved data there
        """
        safe_name = str(re.sub(SSM_NOT_AVAILABLE, '-', name))
        timestamp = utc_datetime().strftime('%m.%d.%Y.%H.%M.%S')
        key = f'{prefix}.{safe_name}.{timestamp}'
        _LOG.debug(f'Saving some data to {key}')
        self.create_secret_value(secret_name=key, secret_value=value)
        return key
