from datetime import datetime, timezone
from typing import Union

from helpers import build_response
from helpers.constants import BATCH_ENV_SCHEDULED_JOB_NAME, \
    BATCH_ENV_TARGET_RULESETS_VIEW, BATCH_ENV_LICENSED_RULESETS, \
    BATCH_ENV_TARGET_REGIONS, BATCH_ENV_AFFECTED_LICENSES, BATCH_ENV_JOB_TYPE, \
    BATCH_EVENT_DRIVEN_JOB_TYPE, BATCH_ENV_BATCH_RESULTS_ID, \
    BATCH_MULTI_ACCOUNT_EVENT_DRIVEN_JOB_TYPE, BATCH_ENV_BATCH_RESULTS_IDS, \
    BATCH_ENV_TENANT_NAME
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.job import Job
from services import SERVICE_PROVIDER
from services.abstract_lambda import AbstractLambda, PARAM_NATIVE_JOB_ID
from services.batch_results_service import BatchResultsService
from services.job_service import JobService
from services.license_manager_service import LicenseManagerService
from services.ruleset_service import RulesetService
from services.ssm_service import SSMService
from services.modular_service import ModularService

PARAM_STOPPED_AT = 'stoppedAt'
PARAM_STARTED_AT = 'startedAt'
PARAM_CREATED_AT = 'createdAt'
PARAM_DETAIL = 'detail'
PARAM_STATUS = 'status'

PARAM_FAILED = 'FAILED'
PARAM_SUCCEEDED = 'SUCCEEDED'
PARAM_CREDENTIALS_KEY = 'CREDENTIALS_KEY'

ENV_ACCOUNT_ID = 'ACCOUNT_ID'
ENV_SUBMITTED_AT = 'SUBMITTED_AT'
ENV_EVENT_DRIVEN = 'EVENT_DRIVEN'

_LOG = get_logger('custodian-job-updater')


class JobUpdater(AbstractLambda):

    def __init__(self, job_service: JobService, ssm_service: SSMService,
                 license_manager_service: LicenseManagerService,
                 ruleset_service: RulesetService,
                 batch_results_service: BatchResultsService,
                 modular_service: ModularService):
        self.job_service = job_service
        self.ssm_service = ssm_service
        self.license_manager_service = license_manager_service
        self.ruleset_service = ruleset_service
        self.batch_result_service = batch_results_service
        self.modular_service = modular_service

    def _update_job_in_lm(self, job: Union[str, Job], detail: dict) -> int:
        """
        Updates the job in License Manager. Returns alteration's status code.
        """
        _LOG.info('The job is licensed. Updating job in License Manager')
        if isinstance(job, Job):
            _LOG.info(f'Job instance is given: {job}. Probably the job is '
                      f'standard (scheduled)')
            job_id = job.job_id
            created_at = job.created_at
            started_at = job.started_at
            stopped_at = job.stopped_at
        elif isinstance(job, str):
            _LOG.info(f'Just job id is given: {job}. Probably the job '
                      f'is event-driven')
            job_id = job
            _get_iso = lambda p: self.timestamp_to_iso(detail.get(p)) if p in detail else None
            created_at = _get_iso(PARAM_CREATED_AT)
            started_at = _get_iso(PARAM_STARTED_AT)
            stopped_at = _get_iso(PARAM_STOPPED_AT)
        else:
            _LOG.error(f'Not available type of job: {job.__class__.__name__}. '
                       f'Skipping.')
            return 404
        return self.license_manager_service.update_job_in_license_manager(
            job_id=job_id,
            created_at=created_at,
            started_at=started_at,
            stopped_at=stopped_at,
            status=detail['status']
        )

    def process_standard_job(self, job_id: str, detail: dict,
                             environment: dict) -> None:
        _LOG.info(f'Processing a standard job with id {job_id}')
        job_item = self.job_service.get_job(job_id)
        if not job_item:
            _LOG.warning(f'Job with id {job_id} does not exist in DB. '
                         f'It will be created')
            job_item = self._create_job(job_id, environment)
        actions = []
        if not job_item.created_at and detail.get(PARAM_CREATED_AT):
            actions.append(
                Job.created_at.set(self.timestamp_to_iso(detail[PARAM_CREATED_AT]))
            )
        if not job_item.started_at and detail.get(PARAM_STARTED_AT):
            actions.append(
                Job.started_at.set(self.timestamp_to_iso(detail[PARAM_STARTED_AT]))
            )
        if detail.get(PARAM_STOPPED_AT):
            if not job_item.stopped_at:
                actions.append(
                    Job.stopped_at.set(self.timestamp_to_iso(detail[PARAM_STOPPED_AT]))
                )
        if not job_item.job_queue:
            actions.append(
                Job.job_queue.set(detail['jobQueue'])
            )
        if not job_item.job_definition:
            actions.append(
                Job.job_definition.set(detail['jobDefinition'])
            )

        if not job_item.scan_regions:
            actions.append(
                Job.scan_regions.set(self._get_job_scan_regions(environment))
            )

        if not job_item.scan_rulesets:
            actions.append(
                Job.scan_rulesets.set(self._get_job_scan_rulesets(environment))
            )

        actions.append(Job.status.set(detail[PARAM_STATUS]))

        if detail['status'] in (PARAM_FAILED, PARAM_SUCCEEDED):
            self._delete_temporary_credentials(detail=detail)
        job_item.update(actions)

        if detail.get(PARAM_STOPPED_AT) and self.is_licensed_job(environment):
            code = self._update_job_in_lm(job=job_item, detail=detail)
            if code == 404:
                _LOG.warning(
                    f'No jobs with id \'{job_id}\' found on license '
                    f'manager side.')
        _LOG.info('Processing has finished')

    def process_event_driven_job(self, job_id: str, detail: dict,
                                 environment: dict) -> None:
        _LOG.info(f'Processing an event-driven job with id {job_id}')
        batch_result = self.batch_result_service.get(
            environment.get(BATCH_ENV_BATCH_RESULTS_ID) or 'mock-id'
        )
        assert batch_result, 'How come an event-driven job is executed ' \
                             'without BatchResults item?'
        if detail.get(PARAM_STOPPED_AT) and self.is_licensed_job(environment):
            code = self._update_job_in_lm(job=job_id, detail=detail)
            if code == 404:
                _LOG.warning(
                    f'No jobs with id \'{job_id}\' found on license '
                    f'manager side.')
        batch_result.status = detail[PARAM_STATUS]
        batch_result.job_id = job_id
        _LOG.info(f'Saving an updated BatchResults item: {batch_result}')
        batch_result.save()
        _LOG.info('Processing has finished')

    def process_multi_account_event_driven_job(self, job_id, detail,
                                               environment):
        _LOG.info(f'Processing a multi account '
                  f'event-driven job with id {job_id}')
        status = detail[PARAM_STATUS]
        if status == PARAM_SUCCEEDED:
            # in case this job is succeeded, each BatchResult has its
            # individual status which was set inside the job
            return
        ids: str = environment.get(BATCH_ENV_BATCH_RESULTS_IDS)
        if not ids:
            return
        for _id in ids.split(','):
            batch_result = self.batch_result_service.get(_id)
            if not batch_result:
                _LOG.warning(f'Batch result with id {_id} not found')
                continue
            batch_result.status = status
            batch_result.job_id = job_id
            batch_result.save()
        _LOG.info('Processing has finished')

    def handle_request(self, event, context):
        detail = event[PARAM_DETAIL]
        job_id = detail[PARAM_NATIVE_JOB_ID]
        environment = self.convert_batch_environment(
            detail.get('container', {}).get('environment', [])
        )
        if environment.get(BATCH_ENV_JOB_TYPE) == BATCH_MULTI_ACCOUNT_EVENT_DRIVEN_JOB_TYPE:
            self.process_multi_account_event_driven_job(job_id, detail, environment)
        if environment.get(BATCH_ENV_JOB_TYPE) == BATCH_EVENT_DRIVEN_JOB_TYPE:
            self.process_event_driven_job(job_id, detail, environment)
        else:
            self.process_standard_job(job_id, detail, environment)
        return build_response(content={'job_id': job_id})

    @staticmethod
    def is_licensed_job(environment: dict) -> bool:
        """
        Returns true in case the job is licensed. A licensed job is the
        one which involves at least one licensed ruleset.
        """
        return bool(environment.get(BATCH_ENV_AFFECTED_LICENSES))

    @staticmethod
    def timestamp_to_iso(timestamp):
        """
        Batch events contains timestamps in UTC
        """
        date = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return utc_iso(_from=date)

    @staticmethod
    def _get_job_scan_regions(environment: dict) -> list:
        scan_regions = []
        _regions = environment.get(BATCH_ENV_TARGET_REGIONS)
        if _regions and isinstance(_regions, str):
            scan_regions.extend(_regions.split(','))
        if not scan_regions:
            scan_regions.append('ALL')
        return scan_regions

    @staticmethod
    def _get_job_scan_rulesets(environment: dict) -> list:
        scan_rulesets = []
        _rulesets = environment.get(BATCH_ENV_TARGET_RULESETS_VIEW)
        if _rulesets and isinstance(_rulesets, str):
            scan_rulesets.extend(_rulesets.split(','))
        _l_rulesets = environment.get(BATCH_ENV_LICENSED_RULESETS)
        if _l_rulesets and isinstance(_l_rulesets, str):
            scan_rulesets.extend(
                each.split(':', maxsplit=1)[-1]
                for each in _l_rulesets.split(',') if ':' in each
            )

        if not scan_rulesets:
            scan_rulesets.append('ALL')
        return scan_rulesets

    def _delete_temporary_credentials(self, detail):
        _LOG.info(f'Deleting used temporary credentials secret')
        job_envs = detail.get('container', {}).get('environment', [])
        for job_env in job_envs:
            if job_env.get('name') == PARAM_CREDENTIALS_KEY:
                credentials_key = job_env.get('value')
                _LOG.debug(f'Deleting smm parameter: \'{credentials_key}\'')
                self.ssm_service.delete_secret(
                    secret_name=credentials_key)
                break

    @staticmethod
    def convert_batch_environment(environment: dict) -> dict:
        envs = {}
        for env in environment:
            envs[env.get('name')] = env.get('value')
        return envs

    def _create_job(self, job_id: str, environment: dict) -> Job:
        tenant = self.modular_service.get_tenant(
            environment.get(BATCH_ENV_TENANT_NAME))
        params = dict(
            job_id=job_id,
            job_owner=tenant.customer_name,
            # because we cannot access user_id
            tenant_display_name=tenant.name,
            customer_display_name=tenant.customer_name,
            submitted_at=environment.get(ENV_SUBMITTED_AT),
            scheduled_rule_name=environment.get(BATCH_ENV_SCHEDULED_JOB_NAME))
        return self.job_service.create(params)


HANDLER = JobUpdater(
    job_service=SERVICE_PROVIDER.job_service(),
    ssm_service=SERVICE_PROVIDER.ssm_service(),
    license_manager_service=SERVICE_PROVIDER.license_manager_service(),
    ruleset_service=SERVICE_PROVIDER.ruleset_service(),
    batch_results_service=SERVICE_PROVIDER.batch_results_service(),
    modular_service=SERVICE_PROVIDER.modular_service()
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
