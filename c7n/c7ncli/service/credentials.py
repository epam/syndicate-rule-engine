import json
import os
import re
from pathlib import Path
from time import time
from typing import Dict, Callable, TypedDict, Optional

import boto3
from boto3 import _get_default_session
from botocore.exceptions import ClientError, PartialCredentialsError, \
    ProfileNotFound

from c7ncli.service.constants import ENV_AWS_ACCESS_KEY_ID, \
    ENV_AWS_SECRET_ACCESS_KEY, ENV_AWS_SESSION_TOKEN, ENV_AWS_DEFAULT_REGION, \
    ENV_AWS_REGION, DEFAULT_AWS_REGION, ENV_AZURE_TENANT_ID, \
    ENV_AZURE_CLIENT_ID, ENV_AZURE_CLIENT_SECRET, ENV_AZURE_SUBSCRIPTION_ID, \
    ENV_GOOGLE_APPLICATION_CREDENTIALS, AWS, GOOGLE, AZURE
from c7ncli.service.helpers import Color
from c7ncli.service.logger import get_user_logger

AWS_ACCESS_KEY = 'aws_access_key_id'
AWS_SECRET_KEY = 'aws_secret_access_key'
AWS_SESSION_TOKEN = 'aws_session_token'
AWS_ROLE_ARN = 'role_arn'
AWS_MFA_SERIAL = 'mfa_serial'
AWS_REGION = 'region'
AVAILABLE_PROFILE_OPTIONS = (
    AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN, AWS_REGION,
    AWS_MFA_SERIAL, AWS_ROLE_ARN
)

USER_LOG = get_user_logger(__name__)


class AWSCredentialsResolver:
    """
    AWS Credentials resolver
    """

    def __init__(self):
        self.sts = None
        self.region_name = None
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.aws_session_token = None
        self.aws_access_role_arn = None
        self.aws_serial_number = None
        self.aws_token_code = None
        self.aws_session_duration = None

    def init_credentials(
            self, aws_access_key_id: str = None,
            aws_secret_access_key: str = None,
            aws_session_token: str = None, region: str = None,
            mfa_serial: str = None, role_arn: str = None,
            duration: int = 3600, token_code: str = None):
        try:
            self.sts = boto3.client(
                'sts', region_name=region, aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token)
        except PartialCredentialsError as e:
            message = f'The given profile contains invalid credentials: {e}'
            raise ValueError(message)
        self.region_name = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.aws_access_role_arn = self._get_role_arn(role_arn)
        self.aws_serial_number = mfa_serial
        self.aws_token_code = token_code
        self.aws_session_duration = duration

    def _response(self, aws_access_key_id=None, aws_secret_access_key=None,
                  aws_session_token=None, region_name=None):
        self._credentials = {
            'aws_access_key': aws_access_key_id or self.aws_access_key_id,
            'aws_secret_access_key':
                aws_secret_access_key or self.aws_secret_access_key,
            'aws_session_token': aws_session_token or self.aws_session_token,
            'aws_default_region': region_name or self.region_name,
        }
        return self._credentials

    def prompt_mfa_code(self, message=None):
        message = message or f'Enter MFA code for ' \
                             f'\'{self.aws_serial_number}\': '
        mfa_code = input(message)
        while 1:
            if len(mfa_code) == 6 and re.match('[0-9]{6}', mfa_code):
                break
            mfa_code = input('Token code must consist of 6 numbers. '
                             'Try again: ')
        return mfa_code

    def _get_role_arn(self, role_name: str):
        if not role_name or role_name.startswith('arn:aws:iam'):
            return role_name
        account_id = self.sts.get_caller_identity()['Account']
        return f'arn:aws:iam::{account_id}:role/{role_name}'

    def _assume_role(self):
        params = dict(
            RoleArn=self.aws_access_role_arn,
            RoleSessionName=f'custodian_scan-{int(time())}',
            DurationSeconds=self.aws_session_duration
        )
        if self.aws_serial_number:
            params.update({
                'SerialNumber': self.aws_serial_number,
                'TokenCode': self.aws_token_code or self.prompt_mfa_code()
            })
        response = self.sts.assume_role(**params)
        return response['Credentials']

    def _get_session_token(self):
        params = dict(
            DurationSeconds=self.aws_session_duration,
        )
        if self.aws_serial_number:
            params.update({
                'SerialNumber': self.aws_serial_number,
                'TokenCode': self.aws_token_code or self.prompt_mfa_code()
            })
        response = self.sts.get_session_token(**params)
        return response['Credentials']

    def get(self):
        try:
            if (self.aws_access_key_id and self.aws_secret_access_key and
                    self.aws_session_token):
                print('Access, secret keys and session token are '
                      'found. Using them...')
                return self._response()
            elif (self.aws_access_key_id and self.aws_secret_access_key and
                  self.aws_access_role_arn):
                print(f'Access, secret keys and role arn are found. '
                      f'Assuming role \'{self.aws_access_role_arn}\'...')
                credentials = self._assume_role()
                return self._response(credentials['AccessKeyId'],
                                      credentials['SecretAccessKey'],
                                      credentials['SessionToken'])
            elif self.aws_access_key_id and self.aws_secret_access_key:
                print('Access and secret keys are '
                      'found. Getting session token...')
                credentials = self._get_session_token()
                return self._response(credentials['AccessKeyId'],
                                      credentials['SecretAccessKey'],
                                      credentials['SessionToken'])
            else:  # not used currently, but may be used in future
                print('Using default session credentials...')
                credentials = _get_default_session().get_credentials()
                credentials = credentials.get_frozen_credentials()
                return self._response(credentials.access_key,
                                      credentials.secret_key,
                                      credentials.token)
        except ClientError as e:
            raise ValueError(str(e))


def retrieve_profile_options(profile_name: str) -> dict:
    try:
        session = boto3.Session(profile_name=profile_name)
    except ProfileNotFound as e:
        raise ValueError(str(e))
    print(f'Retrieving options from profile \'{profile_name}\'')
    config = session._session.full_config.get('profiles', {})
    profile_config = config.get(profile_name, {})
    result = dict()
    result.update(config.get(profile_config.get('source_profile'), {}))
    result.update(profile_config)
    return {k: v for k, v in result.items() if k in AVAILABLE_PROFILE_OPTIONS}


class AWSCredentials(TypedDict):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: Optional[str]
    AWS_DEFAULT_REGION: Optional[str]


class AZURECredentials(TypedDict):
    AZURE_TENANT_ID: str
    AZURE_CLIENT_ID: str
    ENV_AZURE_CLIENT_SECRET: str


class GOOGLECredentials(TypedDict):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class EnvCredentialsResolver:
    def __init__(self, cloud: str):
        cloud = cloud.upper()
        assert cloud in {AWS, AZURE, GOOGLE}
        self._cloud = cloud

    @property
    def cloud(self) -> str:
        return self._cloud

    @property
    def env_resolver_map(self) -> Dict[str, Callable[[], Dict]]:
        return {
            AWS: self._aws_creds_env,
            AZURE: self._azure_creds_env,
            GOOGLE: self._google_creds_env
        }

    def _aws_creds_env(self) -> AWSCredentials:
        """
        TODO, maybe resolve from _get_default_session
        :return:
        """
        creds = {}
        _access_key = os.environ.get(ENV_AWS_ACCESS_KEY_ID)
        _secret_key = os.environ.get(ENV_AWS_SECRET_ACCESS_KEY)
        if not all((_access_key, _secret_key)):
            raise LookupError('Necessary AWS credentials envs not found')

        USER_LOG.info(Color.green(f'Access key found: {_access_key}'))
        creds[ENV_AWS_ACCESS_KEY_ID] = _access_key
        USER_LOG.info(Color.green('Secret access key found'))
        creds[ENV_AWS_SECRET_ACCESS_KEY] = _secret_key

        _token = os.environ.get(ENV_AWS_SESSION_TOKEN)
        if _token:
            USER_LOG.info(Color.green('Session token found'))
            creds[ENV_AWS_SESSION_TOKEN] = _token
        _default_region = (os.environ.get(ENV_AWS_DEFAULT_REGION) or
                           os.environ.get(ENV_AWS_REGION))
        if not _default_region:
            USER_LOG.info(Color.yellow(
                f'Region not found in envs. '
                f'Using {DEFAULT_AWS_REGION} as default AWS region'
            ))
            _default_region = DEFAULT_AWS_REGION
        else:
            USER_LOG.info(Color.green('Default region found'))
        creds[ENV_AWS_DEFAULT_REGION] = _default_region
        return creds

    def _azure_creds_env(self) -> AZURECredentials:
        _tenant_id = os.environ.get(ENV_AZURE_TENANT_ID)
        _client_id = os.environ.get(ENV_AZURE_CLIENT_ID)
        _client_secret = os.environ.get(ENV_AZURE_CLIENT_SECRET)
        if not all((_tenant_id, _client_id, _client_secret)):
            raise LookupError('Necessary AZURE credentials envs not found')
        creds = {
            ENV_AZURE_CLIENT_ID: _client_id,
            ENV_AZURE_TENANT_ID: _tenant_id,
            ENV_AZURE_CLIENT_SECRET: _client_secret,
        }
        _subscription_id = os.environ.get(ENV_AZURE_SUBSCRIPTION_ID)
        if _subscription_id:
            USER_LOG.info(Color.green('Subscription id found'))
            creds[ENV_AZURE_SUBSCRIPTION_ID] = _subscription_id
        else:
            USER_LOG.info(Color.yellow('Subscription id not found'))
        return creds

    def _google_creds_env(self) -> GOOGLECredentials:
        filename = os.environ.get(ENV_GOOGLE_APPLICATION_CREDENTIALS)
        # TODO maybe resolve 'GOOGLE_ACCESS_TOKEN' envs
        if not filename:
            raise LookupError('Necessary GOOGLE credentials envs not found')
        USER_LOG.info(
            Color.green(f'Env {ENV_GOOGLE_APPLICATION_CREDENTIALS} found')
        )
        filename = Path(filename)
        if not all((filename.exists(), filename.is_file())):
            raise LookupError(f'File {filename} not found')
        try:
            USER_LOG.info(
                Color.green(f'File {filename} exists')
            )
            with open(filename, 'r') as file:
                return json.load(file)  # hopefully the file is correct
        except json.JSONDecodeError as e:
            raise LookupError(f'File {filename} contains invalid JSON')
        except Exception as e:
            raise LookupError(f'Could not read {filename}')

    def resolve(self) -> Dict:
        resolver = self.env_resolver_map.get(self.cloud)
        USER_LOG.info(
            Color.green(f'Looking for credentials for {self.cloud}')
        )
        return resolver()
