import json
import tempfile
from pathlib import Path

from modular_sdk.commons.constants import ENV_KUBECONFIG

from executor.helpers.constants import (
    ENV_CLOUDSDK_CORE_PROJECT,
    ENV_GOOGLE_APPLICATION_CREDENTIALS,
)
from helpers.log_helper import get_logger
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)


class CredentialsService:
    def __init__(
        self,
        ssm_client: AbstractSSMClient,
    ):
        self.ssm = ssm_client

    def get_credentials_from_ssm(
        self,
        credentials_key: str,
        remove: bool = True,
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

    def google_credentials_to_file(
        self,
        credentials: dict,
    ) -> dict:
        file_path = self._to_tmp_file(credentials)
        _LOG.debug(f'Writing credentials to {file_path}')

        return {
            ENV_GOOGLE_APPLICATION_CREDENTIALS: file_path,
            ENV_CLOUDSDK_CORE_PROJECT: credentials.get('project_id'),
        }

    def k8s_credentials_to_file(
        self,
        credentials: dict,
    ) -> dict:
        # file_path = self._to_tmp_file(credentials)
        # _LOG.debug(f'Writing credentials to {file_path}')

        # We write to the default kube config location because sometimes the
        # env var KUBECONFIG is invisible for Kubernetes libs
        file_path = self._to_default_kube_config_file(credentials)

        return {ENV_KUBECONFIG: file_path}

    @staticmethod
    def _to_tmp_file(
        credentials: dict,
    ) -> str:
        with tempfile.NamedTemporaryFile('w', delete=False) as fp:
            json.dump(credentials, fp)

        return fp.name

    @staticmethod
    def _to_default_kube_config_file(
        credentials: dict,
    ) -> str:
        path = Path.home() / '.kube' / 'config'
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            _LOG.warning(f'Failed to create kube directory {path.parent}: {e}')
        try:
            path.write_text(json.dumps(credentials))
        except Exception as e:
            _LOG.error(f'Failed writing kube config to {path}: {e}')
            raise
        _LOG.debug(f'Wrote kube config credentials to {path}')
        return str(path)
