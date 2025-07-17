import datetime
from collections.abc import Iterator

from c7n_azure.resources.arm import ArmResourceManager


class ActivityLog(ArmResourceManager):
    """Activity Log Resource

        :example:

        .. code-block:: yaml

            policies:
              - name: azure-activity-log
                resource: azure.activity-log
                filters:
                  - type: value
                    key: operationName.value
                    op: eq
                    value: Microsoft.Resources/subscriptions/resourcegroups/delete
    """

    class resource_type(ArmResourceManager.resource_type):
        class EventTimestampFilter(Iterator):
            """Forms the required filter parameter dynamically

            As the API accepts the dates only within a 90-day period from now in UTC, an iterator
            is implemented instead of a static dict. The API actually allows up to an additional
            hour of delay, so 90 days are simply subtracted without additional normalization.
            """

            def __next__(self):
                if hasattr(self, 'returned'):
                    raise StopIteration
                setattr(self, 'returned', True)
                now_minus_90 = (
                                       datetime.datetime.utcnow() - datetime.timedelta(
                                       days=90)
                                       ).isoformat('T') + 'Z'
                return 'filter', f'eventTimestamp ge \'{now_minus_90}\''

        doc_groups = ['Monitors']

        service = 'azure.mgmt.monitor'
        client = 'MonitorManagementClient'
        enum_spec = ('activity_logs', 'list', EventTimestampFilter())
        resource_type = 'Microsoft.Insights/ActivityLogs'


class ActivityLogAlert(ArmResourceManager):
    """Activity Log Alert Resource
        :example:
        .. code-block:: yaml
            policies:
              - name: azure-activity-log-alert
                resource: azure.activity-log-alert
                filters:
                  - type: value
                    key: properties.enabled
                    value: true
    """

    class resource_type(ArmResourceManager.resource_type):
        doc_groups = ['Monitors']

        service = 'azure.mgmt.monitor'
        client = 'MonitorManagementClient'
        enum_spec = ('activity_log_alerts', 'list_by_subscription_id', None)
        resource_type = 'Microsoft.Insights/ActivityLogAlerts'


def register() -> None:
    from c7n_azure.provider import resources
    from c7n_azure.resources.resource_map import ResourceMap

    resources.register('activity-log', ActivityLog)
    resources.register('activity-log-alert', ActivityLogAlert)
    ResourceMap['azure.activity-log'] = f'{__name__}.{ActivityLog.__name__}'
    ResourceMap[
        'azure.activity-log-alert'] = f'{__name__}.{ActivityLogAlert.__name__}'
