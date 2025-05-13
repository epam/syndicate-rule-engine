import json
import tempfile

from executor.helpers.constants import (
    ENV_CLOUDSDK_CORE_PROJECT,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
)
from helpers.log_helper import get_logger
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)


class CredentialsService:
    def __init__(self, ssm_client: AbstractSSMClient):
        self.ssm = ssm_client

    def get_credentials_from_ssm(
        self, credentials_key: str, remove: bool = True
    ) -> dict | str | None:
        """
        Get our (not maestro) credentials from ssm. For AWS and AZURE
        these are already valid credentials envs. Must be just exported.
        For GOOGLE a file must be created additionally.
        :param credentials_key:
        :param remove:
        :return:
        """
        val = self.ssm.get_secret_value(credentials_key)
        if val is None:
            return
        if remove:
            _LOG.info(f'Removing secret {credentials_key}')
            self.ssm.delete_parameter(credentials_key)
        return val

    @staticmethod
    def google_credentials_to_file(credentials: dict) -> dict:
        with tempfile.NamedTemporaryFile('w', delete=False) as fp:
            json.dump(credentials, fp)
        return {
            ENV_GOOGLE_APPLICATION_CREDENTIALS: fp.name,
            ENV_CLOUDSDK_CORE_PROJECT: credentials.get('project_id'),
        }
