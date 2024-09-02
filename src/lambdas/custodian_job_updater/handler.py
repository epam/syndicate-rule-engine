from datetime import datetime, timezone
from http import HTTPStatus
from typing import Literal, TypedDict, cast

from helpers.constants import BatchJobEnv, BatchJobType, JobState
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from services import SERVICE_PROVIDER
from services.abs_lambda import EventProcessorLambdaHandler
from services.job_service import JobUpdater
from services.license_manager_service import LicenseManagerService
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger('custodian-job-updater')


class Env(TypedDict):
    name: str
    value: str


class StateChangeEventDetailContainer(TypedDict):
    image: str
    command: list[str]
    volumes: list
    environment: list[Env]
    mountPoints: list
    ulimits: list
    networkInterfaces: list
    resourceRequirements: list[dict]
    secrets: list


class StateChangeEventDetail(TypedDict, total=False):
    jobArn: str
    jobName: str
    jobId: str
    jobQueue: str
    status: str  # JobState
    attempts: list
    createdAt: int | None  # java ts
    startedAt: int | None
    stoppedAt: int | None
    retryStrategy: dict
    dependsOn: list
    jobDefinition: str
    parameters: dict
    container: StateChangeEventDetailContainer
    tags: dict
    propagateTags: bool
    platformCapabilities: list


class StateChangeEvent(TypedDict):
    version: str
    id: str
    # detail-type
    source: Literal['aws.batch']
    account: str
    time: str  # utc iso
    region: str
    resources: list[str]
    detail: StateChangeEventDetail


class JobUpdaterHandler(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, ssm: AbstractSSMClient,
                 license_manager_service: LicenseManagerService):
        self.ssm = ssm
        self.license_manager_service = license_manager_service

    def update_standard(self, detail: StateChangeEventDetail,
                        environment: dict[str, str]):
        _LOG.info('Updating a standard job')
        job_id = environment.get(BatchJobEnv.CUSTODIAN_JOB_ID.value)
        updater = JobUpdater.from_job_id(job_id)
        updater.status = detail['status']
        if not updater.job.created_at and detail.get('createdAt'):
            updater.created_at = self.timestamp_to_iso(detail['createdAt'])
        if not updater.job.started_at and detail.get('startedAt'):
            updater.started_at = self.timestamp_to_iso(detail['startedAt'])
        if not updater.job.stopped_at and detail.get('stoppedAt'):
            updater.stopped_at = self.timestamp_to_iso(detail['stoppedAt'])
        if not updater.job.queue:
            updater.queue = detail['jobQueue']
        if not updater.job.definition:
            updater.definition = detail['jobDefinition']
        updater.update()
        if detail['status'] in (JobState.FAILED, JobState.SUCCEEDED):
            self._delete_temporary_credentials(environment)

        if detail.get('stoppedAt') and self.is_licensed_job(environment):
            job = updater.job
            self.license_manager_service.cl.update_job(
                job_id=job.id,
                customer=job.customer_name,
                created_at=job.created_at,
                stopped_at=job.stopped_at,
                started_at=job.started_at,
                status=job.status
            )
        _LOG.info('Processing has finished')

    def handle_request(self, event: dict, context):
        dt = event.get('detail-type')
        if dt != 'Batch Job State Change':
            message = f'Not expected detail type came: {dt}. Skipping'
            _LOG.warning(message)
            return build_response(code=HTTPStatus.BAD_REQUEST,
                                  content=message)
        event = cast(StateChangeEvent, event)
        detail = event['detail']
        environment = self.convert_batch_environment(
            detail['container']['environment']
        )
        # event driven jobs are updated inside batch
        if environment.get(BatchJobEnv.JOB_TYPE.value) == BatchJobType.STANDARD:
            # not scheduled and not ed
            self.update_standard(detail, environment)
        return build_response()

    @staticmethod
    def is_licensed_job(environment: dict) -> bool:
        """
        Returns true in case the job is licensed. A licensed job is the
        one which involves at least one licensed ruleset.
        """
        return bool(environment.get(BatchJobEnv.AFFECTED_LICENSES.value))

    @staticmethod
    def timestamp_to_iso(timestamp: float) -> str:
        """
        Batch events contains timestamps in UTC
        """
        date = datetime.fromtimestamp(timestamp / 1e3, tz=timezone.utc)
        return utc_iso(_from=date)

    def _delete_temporary_credentials(self, environment: dict[str, str]):
        _LOG.info(f'Deleting used temporary credentials secret')
        key = environment.get(BatchJobEnv.CREDENTIALS_KEY.value)
        if not key:
            return
        _LOG.debug(f'Deleting smm parameter: \'{key}\'')
        self.ssm.delete_parameter(secret_name=key)

    @staticmethod
    def convert_batch_environment(environment: list[Env]) -> dict:
        return {env['name']: env['value'] for env in environment}


HANDLER = JobUpdaterHandler(
    ssm=SERVICE_PROVIDER.ssm,
    license_manager_service=SERVICE_PROVIDER.license_manager_service,
)


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
