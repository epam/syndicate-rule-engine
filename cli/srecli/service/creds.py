import json
from pathlib import Path

import boto3
import click
from botocore.exceptions import PartialCredentialsError, NoCredentialsError, ClientError

from srecli.service.constants import TenantModel, Env
from srecli.service.logger import get_logger

_LOG = get_logger(__name__)


class CredentialsLookupError(LookupError):
    ...


class CredentialsResolver:
    __slots__ = '_tenant'

    def __init__(self, tenant: TenantModel):
        self._tenant = tenant

    def resolve(self, **kwargs) -> dict:
        """
        Returns credentials for that tenant in envs format. Accepts user
        input also can look into envs and filesystem. Raises click exception
        if user provided invalid credentials or credentials that do not match
        the tenant. Raises CredentialsLookupError if it cannot find any
        credentials.
        Order:
        - parameters
        - envs
        - filesystem
        """


class GOOGLECredentialsResolver(CredentialsResolver):
    @staticmethod
    def _load_file(filename: str) -> dict:
        """
        Raises click.UsageError
        """
        path = Path(filename)

        if not path.exists() or not path.is_file():
            raise click.UsageError(f'{path} not found')
        try:
            with open(path, 'r') as file:
                return json.load(file)
        except Exception:
            _LOG.exception('Exception loading file')
            raise click.UsageError(f'cannot load JSON from {path}')

    def _check_tenant(self, data: dict):
        if data.get('project_id') != self._tenant['account_id']:
            raise click.UsageError('credentials project id does not '
                                   'match tenant account id')

    def resolve(self, *, google_application_credentials_path: str | None = None,
                **kwargs) -> dict:
        if google_application_credentials_path:
            data = self._load_file(google_application_credentials_path)
            self._check_tenant(data)
            return {
                Env.GOOGLE_APPLICATION_CREDENTIALS.value: data
            }
        from_env = Env.GOOGLE_APPLICATION_CREDENTIALS.get()
        if from_env:
            data = self._load_file(from_env)
            self._check_tenant(data)
            return {
                Env.GOOGLE_APPLICATION_CREDENTIALS.value: data
            }
        raise CredentialsLookupError('cannot resolve google credentials')


class AZURECredentialsResolver(CredentialsResolver):
    def _check_subscription(self, azure_subscription_id: str | None):
        if (azure_subscription_id and
                azure_subscription_id != self._tenant['account_id']):
            raise click.UsageError('--azure_subscription_id '
                                   'does not match with tenant account id')

    def resolve(self, *, azure_subscription_id: str | None = None,
                azure_tenant_id: str | None = None,
                azure_client_id: str | None = None,
                azure_client_secret: str | None = None, **kwargs) -> dict:
        azure_tenant_id = azure_tenant_id or Env.AZURE_TENANT_ID.get()
        azure_client_id = azure_client_id or Env.AZURE_CLIENT_ID.get()
        azure_client_secret = azure_client_secret or Env.AZURE_CLIENT_SECRET.get()
        azure_subscription_id = azure_subscription_id or Env.AZURE_SUBSCRIPTION_ID.get()

        if azure_tenant_id and azure_client_id and azure_client_secret:
            self._check_subscription(azure_subscription_id)
            return {
                Env.AZURE_SUBSCRIPTION_ID.value: azure_subscription_id or self._tenant['account_id'],
                Env.AZURE_CLIENT_ID.value: azure_client_id,
                Env.AZURE_TENANT_ID.value: azure_tenant_id,
                Env.AZURE_CLIENT_SECRET.value: azure_client_secret
            }
        if any((azure_tenant_id, azure_client_id, azure_client_secret)):
            raise click.UsageError(
                'Provide azure_tenant_id, azure_client_id and '
                'azure_client_secret together'
            )
        raise CredentialsLookupError('cannot resolve azure credentials')


class AWSCredentialsResolver(CredentialsResolver):
    def _is_valid_account_id(self, account_it: str) -> bool:
        return self._tenant['account_id'] == account_it

    def resolve(self, *, aws_access_key_id: str | None = None,
                aws_secret_access_key: str | None = None,
                aws_session_token: str | None = None, **kwargs) -> dict:
        try:
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
        except PartialCredentialsError as e:
            raise click.UsageError('Partial AWS credentials provided')
        try:
            resp = session.client('sts').get_caller_identity()
            if not self._is_valid_account_id(resp['Account']):
                if session.profile_name == 'default':
                    # default profile is kind of common case,
                    return {}
                raise click.UsageError(
                    f'Resolved credentials account id {resp["Account"]} '
                    f'does not match with tenant account id'
                )
        except NoCredentialsError:
            raise CredentialsLookupError('cannot resolve aws credentials')
        except ClientError:
            raise click.UsageError('Cannot get caller identity with credentials')
        cr = session.get_credentials()
        if not cr:
            raise CredentialsLookupError('cannot resolve aws credentials')

        creds = {
            Env.AWS_ACCESS_KEY_ID.value: cr.access_key,
            Env.AWS_SECRET_ACCESS_KEY.value: cr.secret_key,
        }
        token = cr.token
        if token:
            creds[Env.AWS_SESSION_TOKEN.value] = token
        return creds
