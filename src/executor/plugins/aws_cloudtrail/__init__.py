
import re

import jmespath
from botocore.exceptions import ClientError
from c7n.filters import Filter
from c7n.filters.core import OPERATORS
from c7n.utils import local_session, type_schema


class CloudTrailS3LoggingFilter(Filter):
    schema = type_schema('cloudtrail-s3-logging', enabled={'type': 'boolean'})
    permissions = ('cloudtrail:DescribeTrails',)

    def __call__(self, resource):
        return resource

    def process(self, resources, event=None):
        s3_client = local_session(self.manager.session_factory).client('s3')
        result = []

        if self.data.get('enabled', True):
            for cloudtrail_bucket in resources:
                try:
                    s3_bucket = s3_client.get_bucket_logging(
                        Bucket=cloudtrail_bucket['S3BucketName'])
                    if 'LoggingEnabled' in s3_bucket:
                        result.append(cloudtrail_bucket)

                except ClientError:
                    continue
        else:
            for cloudtrail_bucket in resources:
                try:
                    s3_bucket = s3_client.get_bucket_logging(
                        Bucket=cloudtrail_bucket['S3BucketName'])
                    if 'LoggingEnabled' not in s3_bucket:
                        result.append(cloudtrail_bucket)

                except ClientError:
                    continue

        return result

class CloudTrailS3LoggingPublicFilter(Filter):
    schema = type_schema('cloudtrail-s3-filter',
                         key={'type': 'string'},
                         op={'type': 'string'},
                         state={'type': 'string'},
                         # If we want to check existing of key
                         # in resource, we can use state field
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('cloudtrail:DescribeTrails',)

    def __call__(self, resource):
        return resource

    def process(self, resources, event=None):
        s3_client = local_session(self.manager.session_factory).client('s3')
        s3_api_fields = {'LoggingEnabled': s3_client.get_bucket_logging,
                         'PublicAccessBlockConfiguration': s3_client.get_public_access_block}
        result = []
        k = self.data.get('key').split('.')[0]

        for r in resources:
            if k in s3_api_fields:
                try:
                    entity = s3_api_fields[k](
                        Bucket='{}'.format(r['S3BucketName']))
                    if self.is_valid_state(k, entity):
                        result.append(r)
                    else:
                        key = jmespath.search(self.data.get('key'), entity)
                        op = OPERATORS[self.data.get('op')]
                        if op(key, self.data.get('value')):
                            result.append(r)
                except ClientError:
                    continue
            else:
                raise KeyError

        return result

    def is_valid_state(self, key, entity):
        if self.data.get('state') == 'present':
            if key in entity:
                return True
        elif self.data.get('state') == 'absent':
            if key not in entity:
                return True
        return False

class CloudTrailChangesAlarmExistsFilter(Filter):
    """
    Filter cloudtrails by a complex rule.

    :Example:

        policies:
          - name: cloudtrail_configuration_changes_alarm_exists
            resource: aws.cloudtrail
            filters:
              - type: configuration-changes-alarm-exists
                filter-pattern: "{($.eventName=CreateTrail) || ($.eventName=UpdateTrail) \
                                || ($.eventName=DeleteTrail) || ($.eventName=StartLogging) \
                                || ($.eventName=StopLogging)}"
                subscriptions-confirmed: null
    """
    schema = type_schema('configuration-changes-alarm-exists',
                         required=['filter-pattern',
                                   'subscriptions-confirmed'],
                         **{'filter-pattern': {'type': 'string'},
                            'subscriptions-confirmed': {},
                            'subscriptions-confirmed-op': {'type': 'string'}
                            })
    permissions = ('cloudtrail:DescribeTrails',)

    def __call__(self, resource):
        return resource

    def process(self, resources, event=None):
        cloud_watch_client = local_session(
            self.manager.session_factory).client('cloudwatch')
        cloud_watch_logs_client = local_session(
            self.manager.session_factory).client('logs')
        sns_client = local_session(self.manager.session_factory).client('sns')

        log_groups = cloud_watch_logs_client.describe_log_groups()['logGroups']
        metric_filters = cloud_watch_logs_client.describe_metric_filters()[
            'metricFilters']
        alarms = cloud_watch_client.describe_alarms()['MetricAlarms']
        sns_topics = sns_client.list_topics()['Topics']

        filter_pattern = self.data['filter-pattern']
        subscriptions_confirmed = self.data['subscriptions-confirmed']
        subscriptions_confirmed_op = OPERATORS[
            self.data.get('subscriptions-confirmed-op', 'eq')]
        log_groups_map = {lg['arn']: lg for lg in log_groups}
        metric_filter_map = {mf['logGroupName']: mf for mf in metric_filters
                             if re.search(filter_pattern, mf['filterPattern'])}
        alarm_map = {(a['MetricName'], a['Namespace']): a for a in alarms if
                     'MetricName' in a
                     and 'Namespace' in a}
        sns_topic_attributes_map = {}
        for sns_topic in sns_topics:
            topic_arn = sns_topic['TopicArn']
            attributes = sns_client.get_topic_attributes(
                TopicArn=topic_arn).get('Attributes')
            if attributes.get(
                    'SubscriptionsConfirmed') is not None and subscriptions_confirmed_op(
                int(attributes.get('SubscriptionsConfirmed')), int(subscriptions_confirmed)):
                sns_topic_attributes_map[topic_arn] = attributes

        filtered_resources = []
        for resource in resources:
            log_group = log_groups_map.get(
                resource.get('CloudWatchLogsLogGroupArn'))
            if log_group:
                metric_filter = metric_filter_map.get(
                    log_group.get('logGroupName'))
                if metric_filter:
                    for metric_transformation in metric_filter['metricTransformations']:
                        metric_transformation_name = metric_transformation.get(
                            'metricName')
                        metric_namespace = metric_transformation[
                            'metricNamespace']
                        alarm = alarm_map.get(
                            (metric_transformation_name, metric_namespace))
                        if metric_transformation_name and alarm:
                            for sns_topic in sns_topics:
                                if sns_topic.get('TopicArn') in alarm['AlarmActions']:
                                    sns_topic_attribute = sns_topic_attributes_map.get(
                                        sns_topic['TopicArn'])
                                    if sns_topic_attribute:
                                        filtered_resources.append(resource)

        return super(CloudTrailChangesAlarmExistsFilter, self).process(
            filtered_resources)


def register() -> None:
    from c7n.resources.cloudtrail import CloudTrail

    CloudTrail.filter_registry.register('cloudtrail-s3-logging', CloudTrailS3LoggingFilter)
    CloudTrail.filter_registry.register('cloudtrail-s3-filter', CloudTrailS3LoggingPublicFilter)

    CloudTrail.filter_registry.register('configuration-changes-alarm-exists',CloudTrailChangesAlarmExistsFilter)
