import json
import uuid
from typing import List

import boto3
from botocore.exceptions import ClientError

from services.clients.sts import StsClient
from services.environment_service import EnvironmentService


class RuleTarget:
    def __init__(self, arn: str, role_arn: str = None, _id: str = None,
                 _input: dict = None):
        self._arn = arn
        self._role_arn = role_arn
        self._id = _id or str(uuid.uuid4())
        self._input_transformer = {}
        self._input = _input

    def serialize(self) -> dict:
        result = {
            'Id': self._id,
            'Arn': self._arn
        }
        if self._role_arn:
            result['RoleArn'] = self._role_arn
        if self._input_transformer:
            result['InputTransformer'] = self._input_transformer
        if self._input:
            result['Input'] = json.dumps(self._input)
        return result

    def set_input_transformer(self, input_paths_map: dict,
                              input_template: dict):
        self._input_transformer['InputPathsMap'] = input_paths_map
        self._input_transformer['InputTemplate'] = json.dumps(input_template)


class BatchRuleTarget(RuleTarget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job_definition = None
        self._job_name = None
        self._array_properties = {}
        self._retry_strategy = {}

    @property
    def default_job_name(self):
        return f'custodian-scheduled-job-{uuid.uuid4()}'

    def set_params(self, job_definition: str, job_name: str = None,
                   array_size: int = None, attempts: int = None):
        self._job_definition = job_definition
        self._job_name = job_name or self.default_job_name
        if (array_size and isinstance(array_size, int) and
                2 <= array_size <= 10_000):
            self._array_properties['Size'] = array_size
        if attempts and isinstance(attempts, int) and 1 <= attempts <= 10:
            self._retry_strategy['Attempts'] = attempts

    def serialize(self) -> dict:
        result = super().serialize()
        params = {
            'JobDefinition': self._job_definition,
            'JobName': self._job_name,
        }
        if self._array_properties:
            params['ArrayProperties'] = self._array_properties
        if self._retry_strategy:
            params['RetryStrategy'] = self._retry_strategy
        result.update({
            'BatchParameters': params
        })
        return result


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

    def put_rule(self, rule_name: str, schedule: str, state: str = 'ENABLED',
                 description: str = 'Custodian managed rule') -> str:
        params = dict(Name=rule_name, ScheduleExpression=schedule,
                      State=state, Description=description)
        response = self.client.put_rule(**self.sifted(params))
        return response['RuleArn']

    def delete_rule(self, rule_name: str) -> dict:
        return self.client.delete_rule(Name=rule_name)

    def put_targets(self, rule_name: str, targets: List[RuleTarget]) -> dict:
        params = dict(Rule=rule_name, Targets=[t.serialize() for t in targets])
        return self.client.put_targets(**params)

    def remove_targets(self, rule_name: str, ids: list) -> bool:
        """
        Returns True in case the rule was found and the requests was
        successful. Returns False in the rule was not found.
        """
        params = dict(Rule=rule_name, Ids=ids)
        try:
            self.client.remove_targets(**params)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise

    def build_rule_arn(self, rule_name: str) -> str:
        return f'arn:aws:events:{self._environment.aws_region()}:' \
               f'{self._sts_client.get_account_id()}:rule/{rule_name}'

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

    def describe_rule(self, rule_name) -> dict:
        params = dict(Name=rule_name)
        try:
            return self.client.describe_rule(**params)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return {}
            raise
