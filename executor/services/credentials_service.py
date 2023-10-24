import json
import re
import tempfile
from datetime import datetime, timedelta, timezone
from functools import cached_property
from typing import Tuple, Dict, Callable, Optional, Union

from botocore.exceptions import ClientError
from modular_sdk.models.tenant import Tenant

from helpers.constants import AWS, GOOGLE, AZURE, \
    ENV_AWS_ACCESS_KEY_ID, ENV_AWS_SECRET_ACCESS_KEY, \
    ENV_AWS_SESSION_TOKEN, ENV_AWS_DEFAULT_REGION, \
    ENV_GOOGLE_APPLICATION_CREDENTIALS, ENV_CLOUDSDK_CORE_PROJECT
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.credentials_manager import CredentialsManager
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from services.ssm_service import SSMService

_LOG = get_logger(__name__)

VALID_CREDENTIALS_THRESHOLD_MINUTES = 15


class CredentialsService:
    def __init__(self, ssm_service: SSMService,
                 environment_service: EnvironmentService,
                 sts_client: StsClient):
        self.ssm_service = ssm_service
        self.environment_service = environment_service
        self.sts_client = sts_client

    @cached_property
    def credentials_cm_getter(self) -> Dict[str, Callable]:
        """
        Credentials manager getter. For historical reasons we keep lowercase
        cloud in credentials manager. + We keep `gcp` instead of `google`
        """
        return {
            AWS: self._cm_get_aws,
            AZURE: self._cm_get_azure,
            GOOGLE: self._cm_get_google,
            'aws': self._cm_get_aws,
            'azure': self._cm_get_azure,
            'google': self._cm_get_google,
            'gcp': self._cm_get_google
        }

    def _cm_get_aws(self, configuration: CredentialsManager) -> dict:
        cloud_identifier = configuration.cloud_identifier
        if configuration.credentials_key and configuration.expiration:
            _LOG.debug(f'Credentials key and expiration exist in config. '
                       f'Checking expiration')
            time_in_a_while = utc_datetime() + timedelta(
                minutes=VALID_CREDENTIALS_THRESHOLD_MINUTES)
            if (datetime.fromtimestamp(configuration.expiration,
                                       timezone.utc) > time_in_a_while):
                _LOG.debug(f'The account {cloud_identifier} credentials will'
                           f' expire in more than '
                           f'{VALID_CREDENTIALS_THRESHOLD_MINUTES} minutes. '
                           f'Acceptable :|')
                return self.get_credentials_from_ssm(
                    configuration.credentials_key, remove=False)

        _LOG.debug(f'Getting new credentials for account {cloud_identifier}')

        credentials_dict, expiration = self._get_temp_aws_credentials(
            configuration.trusted_role_arn)
        if not credentials_dict:
            return {}
        _LOG.debug('Going to save credentials to SSM')
        new_credentials_key = self._save_temp_credentials(
            credentials=credentials_dict,
            cloud_identifier=cloud_identifier
        )
        _LOG.debug(f'Temporary credentials key: {new_credentials_key}')
        if configuration.credentials_key:
            _LOG.info(f'Removing the old SSM parameter '
                      f'\'{configuration.credentials_key}\' with credentials')
            self.ssm_service.delete_secret_value(configuration.credentials_key)

        _LOG.debug(f'Updating credentialsManagerConfig for account '
                   f'{cloud_identifier} with new credentials key')
        configuration.credentials_key = new_credentials_key
        configuration.expiration = expiration.timestamp()
        configuration.save()
        return credentials_dict

    def _cm_get_azure(self, configuration: CredentialsManager) -> dict:
        if not configuration.credentials_key:
            _LOG.warning(f'No credentials key in: {configuration}')
            return {}
        return self.get_credentials_from_ssm(configuration.credentials_key,
                                             remove=False)

    def _cm_get_google(self, configuration: CredentialsManager) -> dict:
        """
        Not implemented
        """
        if not configuration.credentials_key:
            _LOG.warning(f'No credentials key in: {configuration}')
            return {}
        return self.get_credentials_from_ssm(configuration.credentials_key,
                                             remove=False)

    def _get_temp_aws_credentials(self, role_arn: str
                                  ) -> Tuple[dict, Optional[datetime]]:
        """
        AWS temp credentials
        """
        _LOG.debug(f'Assuming trusted role: {role_arn}')
        try:
            assume_role_result = self.sts_client.assume_role(role_arn=role_arn)
        except ClientError as e:
            _LOG.exception(f"Can't assume role with specified {role_arn}:")
            return {}, None
        credentials = assume_role_result['Credentials']
        return {
            ENV_AWS_ACCESS_KEY_ID: credentials['AccessKeyId'],
            ENV_AWS_SECRET_ACCESS_KEY: credentials['SecretAccessKey'],
            ENV_AWS_SESSION_TOKEN: credentials['SessionToken'],
            ENV_AWS_DEFAULT_REGION: self.environment_service.aws_region()
        }, credentials['Expiration']

    def _save_temp_credentials(self, credentials, cloud_identifier):
        not_available = r'[^a-zA-Z0-9\/_.-]'
        account_display_name = str(re.sub(not_available, '-',
                                          cloud_identifier))
        timestamp = utc_datetime().strftime('%m.%d.%Y.%H.%M.%S')
        credentials_key = f'caas.scan.{account_display_name}.{timestamp}'
        _LOG.debug(f'Saving temporary credentials to {credentials_key}')
        self.ssm_service.create_secret_value(
            secret_name=credentials_key,
            secret_value=credentials)
        return credentials_key

    @staticmethod
    def _adjust_cloud(cloud: str) -> str:
        """
        Backward compatibility. We use GCP everywhere, but Maestro
        Tenants use GOOGLE
        """
        cloud = cloud.lower()
        return 'gcp' if cloud == 'google' else cloud

    def get_credentials_for_tenant(self, tenant: Tenant) -> dict:
        """
        Credentials manager keeps clouds in lowercase :( historical reasons
        """
        cloud = self._adjust_cloud(tenant.cloud)
        configuration = CredentialsManager.get_nullable(
            hash_key=tenant.project, range_key=cloud)
        if not configuration or not configuration.enabled:
            _LOG.warning(f'Enabled credentials configurations does not '
                         f'exist for cloud: {cloud} and '
                         f'account {tenant.project}')
            return {}
        return self.get_credentials_from_cm(configuration)

    def get_credentials_from_cm(self, configuration: CredentialsManager
                                ) -> dict:
        """
        Get credentials from credentials manager
        """
        getter = self.credentials_cm_getter.get(configuration.cloud.lower())
        if not getter:
            _LOG.warning(f'No available cloud: {configuration.cloud}')
            return {}
        return getter(configuration)

    def get_credentials_from_ssm(self, credentials_key: Optional[str] = None,
                                 remove: Optional[bool] = True
                                 ) -> Union[str, dict]:
        """
        Get our (not maestro) credentials from ssm. For AWS and AZURE
        these are already valid credentials envs. Must be just exported.
        For GOOGLE a file must be created additionally.
        :param credentials_key:
        :param remove:
        :return:
        """
        if not credentials_key:
            _LOG.info('Credentials key not provided to '
                      'get_credentials_from_ssm function. Using key from env')
            credentials_key = self.environment_service.credentials_key()
        if not credentials_key:
            return {}
        try:
            value = self.ssm_service.get_secret_value(credentials_key) or {}
            if remove:
                _LOG.info(f'Removing secret {credentials_key}')
                self.ssm_service.delete_secret_value(credentials_key)
            return value
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def google_credentials_to_file(credentials: dict) -> dict:
        with tempfile.NamedTemporaryFile('w', delete=False) as fp:
            json.dump(credentials, fp)
        return {
            ENV_GOOGLE_APPLICATION_CREDENTIALS: fp.name,
            ENV_CLOUDSDK_CORE_PROJECT: credentials.get('project_id')
        }
