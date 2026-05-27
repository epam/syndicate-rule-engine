import re
from concurrent.futures import as_completed

import jmespath
from c7n.filters import Filter, ValueFilter
from c7n.utils import local_session, type_schema
from c7n.filters.core import OPERATORS
from executor.plugins.aws_cloudtrail import CloudTrailChangesAlarmExistsFilter
from c7n.resources.cloudtrail import get_trail_groups



def op(data, a, b):
    op = OPERATORS[data.get('op', 'eq')]
    return op(a, b)


class RDSSubscriptionFilter(ValueFilter):
    schema = type_schema('rds-sns-subscription-filter',
                         check_in={'$ref': '#/definitions/filters_common/value'},
                         key={'$ref': '#/definitions/filters_common/value'},
                         op={'$ref': '#/definitions/filters_common/value'},
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('rds:DescribeEventSubscriptions',)

    def process(self, resources, event=None):
        client = local_session(self.manager.session_factory).client('rds')
        client_sns = local_session(self.manager.session_factory).client('sns')
        described_subscriptions = client.describe_event_subscriptions()
        sns_topics = client_sns.list_topics()
        accepted_arns = []
        accepted = []
        if not self._check_if_rds_and_cluster(client):
            return accepted

        if not sns_topics.get('Topics') and described_subscriptions.get(
                'EventSubscriptionsList'):
            return [resources[0]]

        with self.executor_factory(max_workers=3) as w:
            for sns_topic in sns_topics['Topics']:
                for sub in described_subscriptions['EventSubscriptionsList']:
                    if sns_topic.get('TopicArn') and sub.get('SnsTopicArn') and \
                            sns_topic['TopicArn'] == sub['SnsTopicArn'] and \
                            sns_topic['TopicArn'] not in accepted_arns:
                        futures = {}
                        futures[w.submit(client_sns.get_topic_attributes,
                                         TopicArn=sns_topic['TopicArn'])] = sns_topic
                        # Starting of concurrent cycle
                        for future in as_completed(futures):
                            attribute = future.result()
                            if attribute.get('Attributes') and int(
                                    attribute['Attributes']['SubscriptionsConfirmed']) > 0:
                                jmespath_key = jmespath.search(self.data.get('key'), sub)
                                value = self.data.get('value')
                                if jmespath_key is not None and op(self.data, jmespath_key, value):
                                    accepted_arns.append(sns_topic['TopicArn'])
                                    accepted.append(sns_topic)
                                    break
                                break

        if len(accepted) > 0:
            return []
        return [resources[0]]

    def _check_in_perfomance(self, check_in):
        if ',' in check_in:
            checked = check_in.replace(' ', '')
            return checked.split(',')
        if isinstance(check_in, str):
            return [check_in]
        if isinstance(check_in, list):
            return check_in
        return False

    def _check_if_rds_and_cluster(self, client):
        check_in = self.data.get('check_in')
        if self._check_in_perfomance(check_in):
            evaluated_checkin = self._check_in_perfomance(check_in)
            if 'rds' in evaluated_checkin and 'cluster' in evaluated_checkin and \
                    len(client.describe_db_instances()['DBInstances']) > 0 and \
                    len(client.describe_db_clusters()['DBClusters']) > 0:
                return True
            elif 'rds' in evaluated_checkin and len(
                    client.describe_db_instances()['DBInstances']) > 0:
                return True
            else:
                return 'cluster' in evaluated_checkin and len(
                    client.describe_db_clusters()['DBClusters']) > 0
        return False


class EventRuleFilter(Filter):
    schema = type_schema('event-rule-filter',)
    permissions = ("iam:GetRole",)

    def process(self, resources, event=None):
        event_client = local_session(self.manager.session_factory).client('events')
        workspace_client = local_session(self.manager.session_factory).client('workspaces')
        list_workspaces = workspace_client.describe_workspaces()
        list_regex = [r'{\"detail-type\":\[\"WorkSpaces Access\"\],'
                      r'\"source\":\[\"aws\.workspaces\"\]}',
                      r'{\"source\":\[\"aws\.workspaces\"\],'
                      r'\"detail-type\":\[\"WorkSpaces Access\"\]}']
        if not list_workspaces.get('Workspaces') and len(list_workspaces.get('Workspaces')) == 0:
            return []
        event_rules = event_client.list_rules()
        if not event_rules.get('Rules'):
            return resources
        resulted = []
        for event in event_rules['Rules']:
            jmespath_key = jmespath.search('EventPattern', event)
            for reg in list_regex:
                if self.regex_match(jmespath_key, reg):
                    resulted.append(event)
                    break

        if len(resulted) > 0:
            return []
        return resources

    def regex_match(self, value, regex):
        return bool(re.match(regex, value, flags=re.IGNORECASE))

class CloudTrailsFilter(Filter):
    schema = {
        'type': 'object',
        'additionalProperties': False,
        'required': ['type'],
        'properties': {
            # Doesn't mix well as enum with inherits that extend
            'type': {'enum': ['cloudtrails']},
            'valueList': {'type': 'string'},
            'statusList': {'type': 'string'},
            'selectorList': {'type': 'string'},
            'configurationChangesAlarmList': {'type': 'string'},
            'value': {'$ref': '#/definitions/filters_common/value'},
            'op': {'$ref': '#/definitions/filters_common/comparison_operators'}
        }
    }
    schema_alias = True
    annotate = True

    permissions = ('cloudtrail:GetTrailStatus', 'cloudtrail:GetEventSelectors',)

    def process(self, resources, event=None):
        trail_client = local_session(self.manager.session_factory).client('cloudtrail')
        cloud_trails = trail_client.describe_trails()
        filtered_resources_map = {
            'valueList': None,
            'statusList': None,
            'selectorList': None,
            'configurationChangesAlarmList': None
        }
        trails_by_trail_arn_map = {}
        if cloud_trails['trailList']:
            for trail in cloud_trails['trailList']:
                trails_by_trail_arn_map[trail['TrailARN']] = trail

        if self.data.get('valueList'):
            filtered_resources = self.__process_cloudtrail_values(cloud_trails)
            filtered_resources_map['valueList'] = filtered_resources
        if self.data.get('statusList'):
            statuses = self.__load_statuses(cloud_trails)
            value_filter = self.__build_value_filter('statusList')
            filtered_resources = self.__process_cloudtrail_additional_values(
                'statusList', statuses, trails_by_trail_arn_map, value_filter)
            filtered_resources_map['statusList'] = filtered_resources
        if self.data.get('selectorList'):
            selectors = self.__load_selectors(cloud_trails, trail_client)
            value_filter = self.__build_value_filter('selectorList')
            filtered_resources = self.__process_cloudtrail_additional_values(
                'selectorList', selectors, trails_by_trail_arn_map, value_filter)
            filtered_resources_map['selectorList'] = filtered_resources
        if self.data.get('configurationChangesAlarmList'):
            value_filter = self.__build_configuration_changes_alarms_value_filter(
                'configurationChangesAlarmList')
            filtered_resources = value_filter.process(cloud_trails['trailList'])
            filtered_resources_map['configurationChangesAlarmList'] = filtered_resources

        result_filtered_resources = self.__find_intersection(
            trails_by_trail_arn_map, filtered_resources_map)

        return self.__process_result(resources, result_filtered_resources)

    @staticmethod
    def __find_intersection(trails_by_trail_arn_map, filtered_resources_map):
        result = []
        for trail_arn in trails_by_trail_arn_map.keys():
            add_trail = True
            for filtered_resources in filtered_resources_map.values():
                if filtered_resources is not None:
                    found = False
                    for filtered_resource in filtered_resources:
                        if filtered_resource['TrailARN'] == trail_arn:
                            found = True
                            break
                    if not found:
                        add_trail = False
                        break
            if add_trail:
                result.append(trails_by_trail_arn_map[trail_arn])
        return result

    def __process_cloudtrail_values(self, cloud_trails):
        value_filter = self.__build_value_filter('valueList')
        filtered_resources = []
        for trail in cloud_trails['trailList']:
            filtered_resource = value_filter.process([{'trailList': [trail]}])
            if filtered_resource:
                filtered_resources.append(trail)
        return filtered_resources

    @staticmethod
    def __process_cloudtrail_additional_values(
            policy_filter_field_name, values, trails_by_trail_arn_map, value_filter):
        filtered_values = []
        for value in values:
            filtered_value = value_filter.process([{policy_filter_field_name: [value]}])
            if filtered_value:
                filtered_values.append(value)
        filtered_resources = [
            trails_by_trail_arn_map[value['TrailARN']]
            for value in filtered_values]
        return filtered_resources

    def __build_value_filter(self, policy_filter_field_name):
        data = {
            'key': 'length(' + self.data.get(policy_filter_field_name) + ')',
            'op': 'gt',
            'value': 0
        }
        return ValueFilter(data, self.manager)

    def __build_configuration_changes_alarms_value_filter(self, policy_filter_field_name):
        data = {
            'filter-pattern': self.data.get(policy_filter_field_name),
            'subscriptions-confirmed': 0,
            'subscriptions-confirmed-op': 'ne'
        }
        return CloudTrailChangesAlarmExistsFilter(data, self.manager)

    def __load_statuses(self, cloud_trails):
        grouped_trails = get_trail_groups(self.manager.session_factory,
                                          cloud_trails['trailList'])
        for region, (client, trails) in grouped_trails.items():
            for trail in trails:
                if 'c7n:TrailStatus' in trail:
                    continue
                status = client.get_trail_status(Name=trail['TrailARN'])
                status.pop('ResponseMetadata')
                trail['c7n:TrailStatus'] = status

        statuses = []
        for trail in cloud_trails['trailList']:
            status = {}
            status.update(trail['c7n:TrailStatus'])
            status.update({'TrailARN': trail['TrailARN']})
            statuses.append(status)
        return statuses

    @staticmethod
    def __load_selectors(cloud_trails, client_trail):
        selectors = []
        for trail in cloud_trails['trailList']:
            selector = client_trail.get_event_selectors(TrailName=trail['TrailARN'])
            selector.update({'TrailARN': trail['TrailARN']})
            selectors.append(selector)
        return selectors

    def __process_result(self, resources, filtered_resources):
        return resources if op(
            self.data, len(filtered_resources), self.data.get('value')) else []


class AnalyzerFindingsFilter(Filter):
    """Checks all analyzers findings on status - ACTIVE

    :example:

    .. code-block:: yaml

      policies:
        - name: analyzer-findings
          resource: account
          filters:
            - type: analyzer-findings-filter
    """

    schema = type_schema('analyzer-findings-filter')
    schema_alias = False
    permissions = ('access-analyzer:ListAnalyzers',)

    def process(self, resources, event=None):
        client = local_session(self.manager.session_factory).client('accessanalyzer')
        account_analyzers = self.manager.retry(client.list_analyzers)['analyzers']
        accepted = []
        for analyzer in account_analyzers:
            findings = client.list_findings(analyzerArn=analyzer['arn'])
            if findings.get('findings'):
                for finding in findings['findings']:
                    if 'status' in finding and finding['status'] == 'ACTIVE':
                        accepted.append(finding)

        return accepted


class AccountIAMRoleLightFilter(Filter):
    schema = type_schema('account-iam-role-light-filter',
                         value={'$ref': '#/definitions/filters_common/value'})
    permissions = ('iam:ListAttachedRolePolicies',)

    def process(self, resources, event=None):
        client = local_session(self.manager.session_factory).client('iam')
        roles = client.list_roles()
        if not roles.get('Roles'):
            return [resources[0]]

        with self.executor_factory(max_workers=3) as w:
            futures = {}
            for role in roles['Roles']:
                futures[w.submit(client.list_attached_role_policies,
                                 RoleName=role['RoleName'])] = role
                for future in as_completed(futures):
                    if future.result().get('AttachedPolicies'):
                        attached = future.result()['AttachedPolicies']
                        managed_policies = self._managed_attached_policies(attached)
                        if self.data.get('value') in managed_policies:
                            return []

        return [resources[0]]

    def _managed_attached_policies(self, attached):
        return [attache['PolicyName'] for attache in attached]


def register() -> None:
    from c7n.resources.account import Account

    Account.filter_registry.register('rds-sns-subscription-filter', RDSSubscriptionFilter)
    Account.filter_registry.register('event-rule-filter', EventRuleFilter)
    Account.filter_registry.register('cloudtrails', CloudTrailsFilter)
    Account.filter_registry.register('analyzer-findings-filter', AnalyzerFindingsFilter)
    Account.filter_registry.register('account-iam-role-light-filter', AccountIAMRoleLightFilter)

