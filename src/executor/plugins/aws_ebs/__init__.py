"""
https://github.com/cloud-custodian/cloud-custodian/pull/9448
"""

from c7n.filters import ListItemFilter
from c7n.utils import local_session, type_schema


class ElasticBeanstalkConfigurationSettingsFilter(ListItemFilter):
    """Filter for Elastic Beanstalk environment to look at deployed configurations set

    The schema to supply to the attrs follows the schema here:
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/elasticbeanstalk/client/describe_configuration_settings.html

    :example:

    .. code-block:: yaml

            policies:
              - name: managed-actions-disabled
                resource: aws.elasticbeanstalk-environment
                filters:
                  - type: configuration-settings
                    attrs:
                      - OptionName: ManagedActionsEnabled
                      - Value: "false"

    """
    schema = type_schema(
        'configuration-settings',
        attrs={'$ref': '#/definitions/filters_common/list_item_attrs'},
        count={'type': 'number'},
        count_op={'$ref': '#/definitions/filters_common/comparison_operators'}
    )
    item_annotation_key = 'c7n:OptionSettings'
    annotate_items = True
    permissions = ('elasticbeanstalk:DescribeConfigurationSettings',)

    def get_item_values(self, resource):
        client = local_session(self.manager.session_factory).client(
            'elasticbeanstalk')
        res = client.describe_configuration_settings(
            ApplicationName=resource['ApplicationName'],
            EnvironmentName=resource['EnvironmentName']
        )['ConfigurationSettings']
        if not res:
            return []
        configuration_set = res[0]  # deployed configuration set
        return configuration_set.get('OptionSettings', [])


def register() -> None:
    from c7n.resources.elasticbeanstalk import ElasticBeanstalkEnvironment

    ElasticBeanstalkEnvironment.filter_registry.register(
        'configuration-settings', ElasticBeanstalkConfigurationSettingsFilter)
