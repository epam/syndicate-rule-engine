"""
https://github.com/cloud-custodian/cloud-custodian/pull/9476
"""
from concurrent.futures import as_completed

from c7n.filters import ListItemFilter
from c7n.utils import (
    local_session, chunks, type_schema, group_by)


class DynamoDBAutoscalingFilter(ListItemFilter):
    schema = type_schema(
        'autoscaling',
        attrs={'$ref': '#/definitions/filters_common/list_item_attrs'},
        count={'type': 'number'},
        count_op={'$ref': '#/definitions/filters_common/comparison_operators'}
    )
    permissions = ('application-autoscaling:DescribeScalableTargets',)
    item_annotation_key = 'c7n:AutoscalingTargets'
    annotate_items = True

    def _process_resources_set(self, client, resources):
        targets = client.describe_scalable_targets(
            ServiceNamespace='dynamodb',
            ResourceIds=[f'table/{t["TableName"]}' for t in resources]
        ).get('ScalableTargets') or []
        grouped = group_by(targets, 'ResourceId')
        for t in resources:
            t[self.item_annotation_key] = grouped.get(
                f'table/{t["TableName"]}') or []

    def process(self, resources, event=None):
        cl = local_session(self.manager.session_factory).client(
            'application-autoscaling')
        with self.manager.executor_factory(max_workers=3) as w:
            futures = []
            # seems like one table can contain not more than 2 autoscaling
            # targets, (not including indexes). 50 is max number per call
            for resources_set in chunks(resources, 25):
                futures.append(
                    w.submit(self._process_resources_set, cl, resources_set))
            for f in as_completed(futures):
                if f.exception():
                    self.log.error(
                        "Exception describing scalable targets for dynamodb\n %s" % (
                            f.exception()
                        )
                    )
                    continue
        return super().process(resources, event)

    def get_item_values(self, resource):
        return resource.pop(self.item_annotation_key)


def register() -> None:
    from c7n.resources.dynamodb import Table

    Table.filter_registry.register('autoscaling', DynamoDBAutoscalingFilter)
