from abc import ABC, abstractmethod
from secrets import token_hex
from typing import Optional
import re

from botocore.exceptions import ClientError
from connections.batch_extension.base_job_client import BaseBatchClient
from helpers.constants import BATCH_ENV_SUBMITTED_AT, \
    BATCH_ENV_TARGET_REGIONS, BATCH_ENV_TARGET_RULESETS, \
    BATCH_ENV_SCHEDULED_JOB_NAME, BATCH_ENV_TARGET_RULESETS_VIEW, \
    BATCH_ENV_LICENSED_RULESETS, ALL_ATTR
from helpers.log_helper import get_logger
from models.scheduled_job import ScheduledJob
from services.clients.event_bridge import EventBridgeClient, BatchRuleTarget
from services.clients.iam import IAMClient
from services.environment_service import EnvironmentService
from helpers import build_response, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from models.modular.tenants import Tenant

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
            f'custodian-job-{tenant.customer_name}_{tenant.name}'
        )

    @abstractmethod
    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None) -> ScheduledJob:
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
    def _update_job_obj_with(obj: ScheduledJob, tenant: Tenant,
                             schedule: str, envs: dict):
        """
        Retrieves some necessary attributes from account obj and job
        envs and sets them to the scheduled_job obj
        """
        rule_sets = []
        standard = envs.get(BATCH_ENV_TARGET_RULESETS_VIEW)
        if standard and isinstance(standard, str):
            rule_sets.extend(standard.split(','))
        licensed = envs.get(BATCH_ENV_LICENSED_RULESETS)
        if licensed and isinstance(licensed, str):
            rule_sets.extend(
                each.split(':', maxsplit=1)[-1]
                for each in licensed.split(',') if ':' in each
            )
        if not rule_sets:
            rule_sets.append(ALL_ATTR)
        obj.update_with(
            customer=tenant.customer_name,
            tenant=tenant.name,
            schedule=schedule,
            scan_regions=envs.get(BATCH_ENV_TARGET_REGIONS, '').split(','),
            scan_rulesets=rule_sets
        )


class EventBridgeJobScheduler(AbstractJobScheduler):

    def __init__(self, client: EventBridgeClient,
                 environment_service: EnvironmentService,
                 iam_client: IAMClient,
                 batch_client: BaseBatchClient):
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
                message = e.response['Error']['Message']
                _LOG.warning(f'User has sent invalid schedule '
                             f'expression: {args}, {kwargs}')
                return build_response(code=RESPONSE_BAD_REQUEST_CODE,
                                      content=f'Validation error: {message}')
            raise

    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None) -> ScheduledJob:
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
        environment[BATCH_ENV_SUBMITTED_AT] = '<submitted_at>'
        environment[BATCH_ENV_SCHEDULED_JOB_NAME] = _id
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
        _job = ScheduledJob(id=_id)
        self._update_job_obj_with(_job, tenant, schedule, environment)
        _job.save()
        _LOG.debug('Scheduled job`s data was saved to Dynamodb')
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
        _id = item.id
        enabling_rule_map = {
            True: self._client.enable_rule,
            False: self._client.disable_rule
        }
        _LOG.info(f'Updating scheduled job with id \'{_id}\'')
        params = dict(rule_name=_id)

        existing_rule = self._client.describe_rule(**params)
        if not existing_rule:
            _LOG.error('The EventBridge rule somehow disparaged')
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Cannot find rule for scheduled job \'{_id}\'. '
                        f'Please recreate the job')

        if isinstance(is_enabled, bool):
            enabling_rule_map.get(is_enabled)(**params)

        if schedule:
            new_description = self._rule_description_from_existing_one(
                    existing_rule.get('Description'), schedule)
            params.update(schedule=schedule, description=new_description,
                          state=existing_rule.get('State'))
            self._put_rule_asserting_valid_schedule_expression(**params)
            _LOG.debug('EventBridge rule was updated')

        item.update_with(is_enabled=is_enabled, schedule=schedule)
        item.save()
        _LOG.debug('Scheduled job`s data was updated in Dynamodb')
        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated')
