import boto3

from helpers.log_helper import get_logger
from time import time
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class StsClient:

    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('sts', self._environment.aws_region())
        return self._client

    def assume_role(self, role_arn: str, duration: int = 3600,
                    role_session_name: str = None):
        role_session_name = role_session_name or f'Custodian-scan-{time()}'
        params = {
            'RoleArn': role_arn,
            'RoleSessionName': role_session_name,
            'DurationSeconds': duration
        }
        return self.client.assume_role(**params)

    @staticmethod
    def get_caller_identity(credentials):
        client = boto3.client('sts', **credentials)
        return client.get_caller_identity()

    def get_account_id(self) -> str:
        _id = self._environment.account_id()
        if not _id:
            _LOG.warning('Valid account id not found in envs. '
                         'Calling \'get_caller_identity\'')
            _id = self.get_caller_identity()['Account']
        return _id
