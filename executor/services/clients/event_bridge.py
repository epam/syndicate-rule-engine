import boto3
from services.environment_service import EnvironmentService
from services.clients.sts import StsClient
from botocore.exceptions import ClientError


class EventBridgeClient:
    def __init__(self, environment_service: EnvironmentService,
                 sts_client: StsClient):
        self._environment = environment_service
        self._sts_client = sts_client
        self._client = None

    @staticmethod
    def sifted(request: dict) -> dict:
        return {k: v for k, v in request.items() if isinstance(
            v, (bool, int)) or v}

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client(
                'events', self._environment.aws_region())
        return self._client

    def enable_rule(self, rule_name) -> bool:
        params = dict(Name=rule_name)
        try:
            self.client.enable_rule(**params)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise

    def disable_rule(self, rule_name) -> bool:
        params = dict(Name=rule_name)
        try:
            self.client.disable_rule(**params)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise
