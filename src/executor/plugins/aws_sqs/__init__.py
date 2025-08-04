import ast

from c7n.filters.core import Filter
from c7n.utils import type_schema


class RedrivePolicySQSFilter(Filter):
    schema = type_schema('redrive-policy-sqs-filter')
    permissions = ('sqs:ListQueues',)

    def process(self, resources, event=None):
        accepted = []
        result = ["RedrivePolicy" not in resource for resource in resources]

        if False not in result:
            return resources

        for resource in resources:
            if "RedrivePolicy" not in resource:
                if self._is_valid_resource(resource, resources):
                    accepted.append(resource)
        return accepted

    def _is_valid_resource(self, is_valid_resourse, resources):
        for resource in resources:
            if 'RedrivePolicy' in resource:
                literal_key = ast.literal_eval(resource['RedrivePolicy'])
                if is_valid_resourse['QueueArn'] == literal_key['deadLetterTargetArn']:
                    return False

        return True

def register() -> None:
    from c7n.resources.sqs import SQS
    SQS.filter_registry.register('redrive-policy-sqs-filter', RedrivePolicySQSFilter)
