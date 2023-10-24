import copy
from datetime import datetime, timedelta

from modular_sdk.commons.error_helper import RESPONSE_SERVICE_UNAVAILABLE_CODE

from helpers import build_response, RESPONSE_BAD_REQUEST_CODE, get_logger
from helpers.constants import TENANT_NAME_ATTR
from services import SERVICE_PROVIDER
from services.abstract_api_handler_lambda import AbstractApiHandlerLambda
from services.batch_results_service import BatchResultsService
from services.clients.event_bridge import EventBridgeClient, RuleTarget
from services.clients.iam import IAMClient
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.modular_service import ModularService
from services.notification_service import NotificationService
from services.setting_service import SettingsService
from helpers.reports import AccountsData
from typing import Optional

SUBJECT = '{tenant_name} Vulnerabilities report {start_date} - {end_date}'
FUNCTION_NAME = 'custodian-notification-handler'

_LOG = get_logger(__name__)

# todo remove this lambda and change it with notification service from Modular


class NotificationHandler(AbstractApiHandlerLambda):
    def __init__(self, notification_service: NotificationService,
                 batch_results_service: BatchResultsService,
                 environment_service: EnvironmentService,
                 modular_service: ModularService, s3_client: S3Client,
                 settings_service: SettingsService, iam_client: IAMClient,
                 event_bridge_client: EventBridgeClient):
        self.notification_service = notification_service
        self.environment_service = environment_service
        self.batch_results_service = batch_results_service
        self.modular_service = modular_service
        self.s3_client = s3_client
        self.settings_service = settings_service
        self.iam_client = iam_client
        self.event_bridge_client = event_bridge_client

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        """
        Sends scheduled notifications to the admins about the results of
        event-driven jobs

        Event must contain 'tenant_name' (required), 'submitted_at',
        'rule_name' parameters in order to determine who to send the
        notifications and in which time range. That's why EB rules should
        contain constant JSON
        """
        tenant = event.get(TENANT_NAME_ATTR)
        submitted_at = event.get('submitted_at')
        rule_name = event.get('rule_name')
        start: Optional[datetime] = event.get('start')
        end: Optional[datetime] = event.get('end')
        if not tenant:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Invalid tenant name: {tenant}'
            )

        tenant_item = self.modular_service.get_tenant(tenant)
        if not rule_name:
            _LOG.warning(
                'Cannot get rule name: cannot set new last \'submitted_at\' '
                'timestamp')

        cloud_data = {
            'all_regions': set(),
            'total_checks': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_vulnerabilities': 0,
            'total_critical': 0,
            'total_high': 0,
            'total_medium': 0,
            'total_low': 0,
            'total_info': 0,
            'total_unknown': 0,
            'accounts': []}
        data = {
            'aws': copy.deepcopy(cloud_data),
            'azure': copy.deepcopy(cloud_data),
            'gcp': copy.deepcopy(cloud_data)
        }

        if start or end:  # TODO move it to a separate Pydantic validator
            if start and end:
                if (start - end).days > 31:
                    return build_response(
                        code=RESPONSE_BAD_REQUEST_CODE,
                        content='Invalid time range: must be less than 31 days'
                    )
            elif start:
                end = start + timedelta(days=7)
            else:
                start = end - timedelta(days=7)

            _LOG.debug(
                f'Retrieving data between two dates: '
                f'{start.isoformat()} - {end.isoformat()}'
            )
            results = self.batch_results_service.get_between_period_by_tenant(
                tenant_name=tenant,
                start=str(start.timestamp()),
                end=str(end.timestamp())
            )

        else:
            end = datetime.utcnow()
            start = end - timedelta(days=7)
            _LOG.debug(f'Retrieving data for '
                       f'last week = since {start.isoformat()}')
            results = self.batch_results_service.get_between_period_by_tenant(
                tenant_name=tenant, start=str(start.timestamp())
            )
        # todo return when this lambda will be triggered by cron
        # else:
        #     _LOG.debug(f'Retrieving data from \'submitted_at\' date: '
        #                f'{submitted_at}')
        #     results = self.batch_results_service.get_by_tenant(
        #         tenant_name=tenant, submitted_at=submitted_at)
        if not results:
            return build_response(
                content='No results found for this period, nothing to report'
            )
        account_mapping = self._get_account_mapping(results)

        customer = self.modular_service.get_customer(results[0].customer_name)
        if tenant_item.contacts and tenant_item.contacts.primary_contacts:
            admins = tenant_item.contacts.primary_contacts
        else:
            admins = customer.admins

        for account_id, items in account_mapping.items():
            _LOG.debug(f'Processing results within {account_id}')
            tenant_obj = next(
                self.modular_service.i_get_tenants_by_acc(account_id, True), None
            )
            filtered_regions = self.modular_service.get_tenant_regions(tenant)
            if tenant_obj:
                data[tenant_obj.cloud]['all_regions'].update(
                    filtered_regions)

            # TODO thread it
            findings_content = self.s3_client.get_json_file_content(
                bucket_name=self.environment_service.get_statistics_bucket_name(),
                full_file_name=f'findings/{account_id}.json')
            _LOG.debug('Findings content was downloaded')
            severity_mapping = {
                'critical': 0,
                'high': 0,
                'medium': 0,
                'low': 0,
                'unknown': 0,
                'info': 0,
            }
            for _, item in findings_content.items():
                if any(value for value in item['resources'].values()):
                    severity_mapping[item['severity'].lower()] += 1

            succeeded = len([i for i in items if i.status == 'SUCCEEDED'])
            failed = len([i for i in items if i.status == 'FAILED'])
            vulnerabilities = len(findings_content)
            data[tenant_obj.cloud]['total_checks'] += len(items)
            data[tenant_obj.cloud]['total_successful'] += succeeded
            data[tenant_obj.cloud]['total_failed'] += failed
            data[tenant_obj.cloud]['total_vulnerabilities'] += \
                vulnerabilities
            data[tenant_obj.cloud]['total_critical'] += \
                severity_mapping['critical']
            data[tenant_obj.cloud]['total_high'] += \
                severity_mapping['high']
            data[tenant_obj.cloud]['total_medium'] += \
                severity_mapping['medium']
            data[tenant_obj.cloud]['total_low'] += \
                severity_mapping['low']
            data[tenant_obj.cloud]['total_info'] += \
                severity_mapping['info']
            data[tenant_obj.cloud]['total_unknown'] += \
                severity_mapping['unknown']
            _LOG.debug('Some data was collected')
            resource_type_mapping = self._get_resource_type(
                findings_content)
            severity_chart = self.notification_service.build_pie_chart(
                labels=list(severity_mapping.keys()),
                values=list(severity_mapping.values()),
                colors=['maroon', 'red', 'darkorange', 'royalblue',
                        'slategray', 'g']
            )
            checks_chart = self.notification_service.build_donut_chart(
                succeeded=succeeded, failed=failed
            )
            resource_type_chart = self.notification_service.build_pie_chart(
                labels=list(resource_type_mapping.keys()),
                values=list(resource_type_mapping.values())
            )
            _LOG.debug('Some charts were built')

            data[tenant_obj.cloud]['accounts'].append(
                AccountsData(**{
                    'regions': filtered_regions,
                    'account_name': tenant_obj.display_name,
                    'total_checks': len(items),
                    'success': succeeded,
                    'fail': failed,
                    'last_sync': datetime.strptime(
                        max([i.submitted_at for i in items]),
                        '%Y-%m-%dT%H:%M:%S.%fZ').strftime(
                        '%B %d, %Y %H:%M:%S'),
                    'vulnerabilities': {
                        'length': vulnerabilities,
                        **severity_mapping
                    },
                    'charts': {
                        'checks_performed': checks_chart,
                        'severity': severity_chart,
                        'resource_type': resource_type_chart
                    }
                }))

        if rule_name:
            submitted_at = sorted(results, key=lambda i: i.submitted_at,
                                  reverse=True)[0]
            target = RuleTarget(
                _id=FUNCTION_NAME,
                arn=self.iam_client.build_lambda_arn(
                    FUNCTION_NAME,
                    region=self.environment_service.aws_region(),
                    alias=self.environment_service.lambdas_alias_name()
                ),
                _input={
                    'rule_name': rule_name,
                    'tenant_name': tenant,
                    'submitted_at': submitted_at.submitted_at
                }
            )
            self.event_bridge_client.put_targets(rule_name, [target])

        result = self.notification_service.send_event_driven_notification(
            recipients=admins, subject=SUBJECT.format(tenant_name=tenant,
                                                      start_date=start,
                                                      end_date=end),
            data={
                'tenant_name': tenant,
                'date': datetime.utcnow().strftime('%B %d, %Y'),
                'period': f'{start.strftime("%B %d, %Y")} - '
                          f'{end.strftime("%B %d, %Y")}',
                'contacts': self.settings_service.get_custodian_contacts(),
                'data': {k: v for k, v in data.items() if v['accounts']}
            })
        if result:
            return build_response(
                content=f'Report was successfully send to {", ".join(admins)}')
        else:
            return build_response(
                content='An error occurred while sending the report.'
                        'Please contact the support team.',
                code=RESPONSE_SERVICE_UNAVAILABLE_CODE
            )

    @staticmethod
    def _get_account_mapping(results: list):
        account_mapping = dict()
        for item in results:
            account_mapping.setdefault(item.cloud_identifier, []).append(
                item
            )
        return account_mapping

    @staticmethod
    def _get_resource_type(findings: dict):
        resource_type_mapping = dict()
        for content in findings.values():
            resource_type = content.get('resourceType')
            if resource_type:
                resource_type = resource_type.replace('aws.', '')
                resource_type_mapping.setdefault(resource_type, 0)
                resource_type_mapping[resource_type] += 1
        return resource_type_mapping


HANDLER = NotificationHandler(
    notification_service=SERVICE_PROVIDER.notification_service(),
    modular_service=SERVICE_PROVIDER.modular_service(),
    s3_client=SERVICE_PROVIDER.s3(),
    batch_results_service=SERVICE_PROVIDER.batch_results_service(),
    environment_service=SERVICE_PROVIDER.environment_service(),
    settings_service=SERVICE_PROVIDER.settings_service(),
    event_bridge_client=SERVICE_PROVIDER.events(),
    iam_client=SERVICE_PROVIDER.iam()
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
