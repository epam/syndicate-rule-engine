"""
https://github.com/cloud-custodian/cloud-custodian/pull/9477
"""

from concurrent.futures import as_completed

import jmespath
from botocore.exceptions import ClientError
from c7n import query
from c7n.filters import ValueFilter, Filter
from c7n.filters.core import OPERATORS
from c7n.tags import universal_augment
from c7n.utils import (
    local_session,
    type_schema,
)


class DescribeCodeSigningConfig(query.DescribeSource):
    def augment(self, resources):
        return universal_augment(self.manager, super().augment(resources))

    def get_resources(self, ids, cache=True):
        if not ids:
            return []

        client = local_session(self.manager.session_factory).client('lambda')
        _processed = set()
        result = []
        for rid in ids:
            arn = rid if rid.startswith('arn:') else self.manager.generate_arn(
                rid)
            if arn in _processed:
                continue
            _processed.add(arn)

            try:
                config = self.manager.retry(
                    client.get_code_signing_config, CodeSigningConfigArn=arn
                )
            except ClientError as e:
                code = e.response['Error']['Code']
                if code == 'ResourceNotFoundException':
                    self.manager.log.warning(
                        'Code signing config %s not found', arn)
                    continue
                raise
            result.append(config['CodeSigningConfig'])
        return result


class AWSLambdaCodeSigningConfig(query.QueryResourceManager):
    class resource_type(query.TypeInfo):
        service = 'lambda'
        arn = 'CodeSigningConfigArn'
        arn_type = 'code-signing-config'
        arn_separator = ':'
        enum_spec = ('list_code_signing_configs', 'CodeSigningConfigs', None)
        name = id = 'CodeSigningConfigId'
        date = 'LastModified'
        # config_type = 'AWS::Lambda::CodeSigningConfig'
        cfn_type = 'AWS::Lambda::CodeSigningConfig'
        id_prefix = 'csc-'
        universal_taggable = object()
        default_report_fields = (
            'CodeSigningConfigArn',
            'CodeSigningPolicies.UntrustedArtifactOnDeployment'
            'Description',
            'LastModified'
        )

    source_mapping = {
        'describe': DescribeCodeSigningConfig,
        'config': query.ConfigSource
    }


class AWSLambdaSigningConfigFilter(ValueFilter):
    """Filter lambda functions by code signing config.

    This filter will annotate the lambda function with the
    CodeSigningConfigArn if it has one.

    :example:

    .. code-block:: yaml

        policies:
          - name: lambda-code-signing-config
            resource: aws.lambda
            filters:
              - type: code-signing-config
    """
    schema = type_schema(
        'code-signing-config',
        rinherit=ValueFilter.schema
    )
    permissions = ('lambda:GetFunctionCodeSigningConfig',)
    annotation_key = "CodeSigningConfig"

    @staticmethod
    def _get_lambdas_by_config(client, arn):
        p = client.get_paginator('list_functions_by_code_signing_config')
        p.PAGE_ITERATOR_CLS = query.RetryPageIterator
        data = p.paginate(CodeSigningConfigArn=arn).build_full_result()
        return data.get('FunctionArns') or []

    def process(self, resources, event=None):
        # assuming that number of signing configs is much smaller that number
        # of functions
        sc = self.manager.get_resource_manager('code-signing-config')
        model = sc.get_model()
        configs = sc.resources()

        client = local_session(self.manager.session_factory).client('lambda')

        function_to_config = {}
        with self.executor_factory() as w:
            futures = {
                w.submit(self._get_lambdas_by_config, client, c[model.arn]): c
                for c in configs
            }
            for f in as_completed(futures):
                if f.exception():
                    self.log.error(
                        "Exception getting lambda functions by code signing config: %s",
                        f.exception()
                    )
                    continue
                for arn in f.result():
                    function_to_config[arn] = futures[f]
        for function in resources:
            function[self.annotation_key] = function_to_config.get(function['FunctionArn']) or {}
        return super().process(resources, event)

    def __call__(self, i):
        if self.annotate:
            item = i[self.annotation_key]
        else:
            item = i.pop(self.annotation_key)
        return super().__call__(item)


class AWSLambdaIamRolePolicy(Filter):
    """
    That filter gets role_name from lambda resource, then
    outlines all policies for this role, and gives result
    of GetRolePolicy method for checking, if policy allows
    acts for all resources "*"
    """
    RelatedIdsExpression = "VpcConfig.SecurityGroupIds[]"
    schema = type_schema('awslambda-iam-role-policy-filter',
                         conditions={'type': 'array', 'items': {
                             'type': 'object',
                             'required': ['key', 'op', 'value'],
                             'additionalProperties': False,
                             'properties': {
                                 'key': {'type': 'string'},
                                 'op': {'type': 'string'},
                                 'value': {
                                     '$ref': '#/definitions/filters_common/value'}
                             }
                         }})
    permissions = ('lambda:GetFunction',)

    def __init__(self, data, manager=None):
        super().__init__(data, manager)

    @staticmethod
    def get_policies(role_detail_list, role):
        result = []
        for role_detail in role_detail_list:
            if role_detail['RoleName'] == role:
                result.extend(role_detail['RolePolicyList'])
        return result

    def process(self, resources, event=None):
        self.iam_client = local_session(self.manager.session_factory).client(
            'iam')
        conditions = self.data.get('conditions')
        role_detail_list = self.iam_client.get_account_authorization_details(
            Filter=['Role']).get('RoleDetailList')
        result = []
        for aws_lambda in resources:
            # for getting attached role_name from aws lambda - aws_lambda['Role'].split('/')[1]
            lambda_role = aws_lambda['Role'].split('/')[1]
            policies = self.get_policies(role_detail_list, lambda_role)
            for policy in policies:
                # aws_lambda is valid if any its policy is valid
                if self._validate_policy_conditions(policy, lambda_role,
                                                    conditions):
                    result.append(aws_lambda)
                    break

        return result

    def _validate_policy_conditions(self, policy, lambda_role, conditions):
        # policy is valid if all conditions pass
        for condition in conditions:
            op = OPERATORS[condition.get('op')]
            key = condition.get('key')
            value = condition.get('value')
            if not self._validate_policy(policy, key, op, value):
                return False
        return True

    def _validate_policy(self, policy, condition_key, condition_op,
                         condition_value):
        value = jmespath.search(condition_key,
                                policy['PolicyDocument']['Statement'][0])
        return condition_op(condition_value, value)


def register() -> None:
    from c7n.resources.awslambda import AWSLambda
    from c7n.manager import resources
    from c7n.resources.resource_map import ResourceMap

    resources.register('code-signing-config', AWSLambdaCodeSigningConfig)
    ResourceMap[
        'aws.code-signing-config'] = f'{__name__}.{AWSLambdaCodeSigningConfig.__name__}'

    AWSLambda.filter_registry.register('code-signing-config',
                                       AWSLambdaSigningConfigFilter)
    AWSLambda.filter_registry.register('awslambda-iam-role-policy-filter',
                                       AWSLambdaIamRolePolicy)
