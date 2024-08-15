import re
from abc import ABC, abstractmethod
from http import HTTPStatus
from secrets import token_hex
from typing import Optional

from botocore.exceptions import ClientError
from modular_sdk.models.tenant import Tenant

from helpers.lambda_response import ResponseFactory
from helpers.constants import ALL_ATTR, BatchJobEnv
from helpers.log_helper import get_logger
from models.scheduled_job import ScheduledJob
from services.clients.batch import BatchClient
from services.clients.event_bridge import EventBridgeClient, BatchRuleTarget
from services.clients.iam import IAMClient
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)

# since each EventBridge rule will contain only one target, we can use
# the same id everywhere and do not call `events.list_targets_by_rule`
# before removing the target.
TARGET_ID = 'custodian-batch-job-target'
NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9_-]')


class AbstractJobScheduler(ABC):

    @staticmethod
    def safe_name(name: str) -> str:
        """
        64 characters - EventBridge rule's name limit.
        16 characters - len(token_hex(8))
        48 = 64 - 16
        """
        name = str(re.sub(NOT_AVAILABLE, '-', name))
        return f'{name[:48]}-{token_hex(8)}'

    def safe_name_from_tenant(self, tenant: Tenant) -> str:
        return self.safe_name(
            f'custodian-job-{tenant.customer_name}-{tenant.name}'
        )

    @abstractmethod
    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None,
                     rulesets: list[str] | None = None) -> ScheduledJob:
        """
        Adds a new job to the system: creates DB item and the rule itself
        """

    @abstractmethod
    def deregister_job(self, _id: str):
        """
        Ensures that the job with given id does not exist in the system,
        neither in DB nor in EventBridge or whatever
        """

    @abstractmethod
    def update_job(self, item: ScheduledJob, is_enabled: Optional[bool] = None,
                   schedule: Optional[str] = None):
        """
        Updates the data of registered job
        """

    @staticmethod
    def _scan_regions_from_env(envs: dict) -> list[str]:
        return envs.get(BatchJobEnv.TARGET_REGIONS.value, '').split(',')


class EventBridgeJobScheduler(AbstractJobScheduler):

    def __init__(self, client: EventBridgeClient,
                 environment_service: EnvironmentService,
                 iam_client: IAMClient,
                 batch_client: BatchClient):
        self._client = client
        self._environment = environment_service
        self._iam_client = iam_client
        self._batch_client = batch_client

    @staticmethod
    def _rule_description_from_scratch(tenant: Tenant,
                                       schedule: str = None) -> str:
        result = f'Custodian managed rule for scheduled job.\n' \
                 f'Tenant: {tenant.name} - ' \
                 f'{tenant.cloud.upper()}:' \
                 f'{tenant.project or ""};\n' \
                 f'Customer: {tenant.customer_name}\n'
        if schedule:
            result += f'Schedule: {schedule};'
        return result

    @staticmethod
    def _rule_description_from_existing_one(existing: str,
                                            schedule: str) -> str:
        result = '\n'.join(existing.split('\n')[:-1])
        result += f'\nSchedule: {schedule};'
        return result

    def _put_rule_asserting_valid_schedule_expression(self, *args, **kwargs):
        try:
            _ = self._client.put_rule(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ValidationException':
                _LOG.warning(f'User has sent invalid schedule '
                             f'expression: {args}, {kwargs}')
                raise ResponseFactory(HTTPStatus.BAD_REQUEST).errors([{
                    'location': ['schedule'],
                    'description': 'Invalid schedule expression. '
                                   'Use expression that is valid for AWS EventBridge, i.e. cron(0 12 * * ? *), cron(5,35 14 * * ? *), rate(2 hours)'
                }]).exc()
            raise

    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None,
                     rulesets: list[str] | None = None) -> ScheduledJob:
        _id = self.safe_name(name) if name else \
            self.safe_name_from_tenant(tenant)
        _LOG.info(f'Registering new scheduled job with id \'{_id}\'')
        params = dict(
            rule_name=_id, schedule=schedule,
            description=self._rule_description_from_scratch(tenant, schedule))
        self._put_rule_asserting_valid_schedule_expression(**params)
        _LOG.debug(f'EventBridge rule was created')
        target = BatchRuleTarget(
            _id=TARGET_ID,
            arn=self._batch_client.get_custodian_job_queue_arn(),
            role_arn=self._iam_client.build_role_arn(
                self._environment.event_bridge_service_role()),
        )
        environment[BatchJobEnv.SUBMITTED_AT.value] = '<submitted_at>'
        environment[BatchJobEnv.SCHEDULED_JOB_NAME.value] = _id
        target.set_input_transformer(
            {'submitted_at': '$.time'},
            self._batch_client.build_container_overrides(
                environment=environment, titled=True
            ))
        target.set_params(
            self._batch_client.get_custodian_job_definition_arn(),
            f'{_id}-scheduled-job'
        )
        _ = self._client.put_targets(_id, [target, ])
        _LOG.debug('Batch queue target was added to the created rule')
        _job = ScheduledJob(
            id=_id,
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            context=dict(
                schedule=schedule,
                scan_regions=self._scan_regions_from_env(environment),
                scan_rulesets=rulesets,
                is_enabled=True
            ),
        )
        _job.save()
        _LOG.info(f'Scheduled job with name \'{_id}\' was added')
        return _job

    def deregister_job(self, _id: str):
        _LOG.info(f'Removing the job with id \'{_id}\'')
        _LOG.debug(f'Removing EventBridge rule target with id \'{TARGET_ID}\'')
        if self._client.remove_targets(_id, ids=[TARGET_ID]):
            _LOG.info('Rule`s target was removed. Removing the rule itself')
            self._client.delete_rule(_id)
        else:
            _LOG.warning(f'Rule \'{_id}\' was not found during removing '
                         f'targets. Skipping')
        _LOG.info(f'Removing DynamoDB item with id \'{_id}\'')
        ScheduledJob(id=_id).delete()
        _LOG.info('The job was successfully deregistered')

    def update_job(self, item: ScheduledJob, is_enabled: Optional[bool] = None,
                   schedule: Optional[str] = None):
        if not isinstance(is_enabled, bool) and not schedule:
            return
        rule = self._client.describe_rule(rule_name=item.id)
        params = dict(
            rule_name=item.id,
            schedule=rule['ScheduleExpression'],
            state=rule['State']
        )
        actions = []
        if isinstance(is_enabled, bool):
            params['state'] = 'ENABLED' if is_enabled else 'DISABLED'
            actions.append(ScheduledJob.context['is_enabled'].set(is_enabled))
        if schedule:
            params['schedule'] = schedule
            params['description'] = self._rule_description_from_existing_one(
                rule['Description'],
                schedule
            )
            actions.append(ScheduledJob.context['schedule'].set(schedule))
        self._put_rule_asserting_valid_schedule_expression(**params)
        _LOG.debug('EventBridge rule was updated')
        item.update(actions=actions)

        _LOG.info(f'Scheduled job \'{item.id}\' was successfully updated')
