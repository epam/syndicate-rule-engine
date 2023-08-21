"""
[Up-to-date description]

Available environment variables:
- ...
- ...

Usage: python executor.py

Exit codes:
- 0: success;
- 1: unexpected system error;
- 2: Job execution is not granted by the License Manager;
- 126: Job is event-driven and cannot be executed in consequence of invalid
  credentials or conceivably some other temporal reason. Retry is allowed.
"""
import json
import os
import subprocess
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from copy import deepcopy
from itertools import chain
from pathlib import Path
from pathlib import PurePosixPath
from typing import List, Union, Tuple, Dict, Optional, Set

from botocore.exceptions import ClientError
from c7n.commands import load_policies
from c7n.config import Config
from c7n.policy import Policy
from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from modular_sdk.commons.constants import \
    TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE, \
    TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE
from modular_sdk.services.environment_service import EnvironmentContext
from msrestazure.azure_exceptions import CloudError

from helpers.constants import *
from helpers.exception import ExecutorException
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from helpers.timeit import timeit
from integrations.security_hub.dump_findings_policy import DumpFindingsPolicy
from models.batch_results import BatchResults
from models.job import Job
from models.modular.application import CustodianLicensesApplicationMeta
from models.modular.customer import Customer
from models.modular.parents import ParentMeta
from models.modular.tenants import Tenant
from services import SERVICE_PROVIDER as SP
from services.batch_service import BatchService
from services.credentials_service import CredentialsService
from services.environment_service import EnvironmentService
from services.integration_service import IntegrationService
from services.job_updater_service import JobUpdaterService
from services.license_manager_service import LicenseManagerService, \
    BalanceExhaustion, InaccessibleAssets
from services.modular_service import ModularService
from services.modular_service import TenantService
from services.notification_service import NotificationService
from services.os_service import OSService
from services.policy_service import PolicyService
from services.report_service import FindingsCollection, DETAILED_REPORT_FILE, \
    REPORT_FILE, DIFFERENCE_FILE
from services.report_service import ReportService
from services.ruleset_service import RulesetService
from services.s3_service import S3Service
from services.scheduler_service import SchedulerService
from services.setting_service import SettingService
from services.ssm_service import SSMService
from services.statistics_service import StatisticsService, \
    STATISTICS_FILE, API_CALLS_FILE

environment_service: Optional[EnvironmentService] = None
ssm_service: Optional[SSMService] = None
credentials_service: Optional[CredentialsService] = None
os_service: Optional[OSService] = None
policy_service: Optional[PolicyService] = None
ruleset_service: Optional[RulesetService] = None
s3_service: Optional[S3Service] = None
batch_service: Optional[BatchService] = None
report_service: Optional[ReportService] = None
statistics_service: Optional[StatisticsService] = None
job_updater_service: Optional[JobUpdaterService] = None
license_manager_service: Optional[LicenseManagerService] = None
modular_service: Optional[ModularService] = None
tenant_service: Optional[TenantService] = None
scheduler_service: Optional[SchedulerService] = None
setting_service: Optional[SettingService] = None
notification_service: Optional[NotificationService] = None
integration_service: Optional[IntegrationService] = None
SERVICES = {'environment_service', 'ssm_service', 'credentials_service',
            'os_service', 'policy_service', 'ruleset_service', 's3_service',
            'batch_service', 'report_service', 'statistics_service',
            'job_updater_service',
            'notification_service', 'license_manager_service',
            'modular_service',
            'tenant_service', 'scheduler_service', 'setting_service',
            'integration_service'}

_LOG = get_logger(__name__)


def init_services(services: Optional[set] = None):
    """
    Assigns services instances to their corresponding variables in the
    global scope. If `services` param is not given, all the services
    will be instantiated.
    :parameter services: Optional[set]
    :return: None
    """
    services = services or SERVICES
    assert services.issubset(SERVICES), \
        f'{SERVICES - services} are not available'
    for s in services:
        _globals: dict = globals()
        if not _globals.get(s):
            _LOG.info(f'Initializing {s.replace("_", " ")}')
            _globals[s] = getattr(SP, s, lambda _: None)()


init_services({'environment_service'})

TIME_THRESHOLD: Optional[float] = None
WORK_DIR: Optional[str] = None
CLOUD_COMMON_REGION = {
    AWS: environment_service.aws_default_region(),
    AZURE: AZURE_COMMON_REGION,
    GOOGLE: GCP_COMMON_REGION
}
ERROR_WHILE_LOADING_POLICIES_MESSAGE = 'unexpected error occurred while ' \
                                       'loading policies files: {policies}'


class Runner:
    @property
    def cloud(self):
        return None

    def __init__(self, policies: List[Policy], findings_dir: str):
        self._policies = policies
        self._findings_dir = findings_dir
        self._skipped, self._failed = {}, {}
        self._is_ongoing = False
        self._finishing_reason = None
        self._lock = threading.Lock()

    @property
    def skipped(self) -> dict:
        return self._skipped

    @property
    def failed(self) -> dict:
        return self._failed

    def start(self):
        self._is_ongoing = True
        for policy in self._policies:
            self._handle_errors(policy=policy)
        self._is_ongoing = False

    def start_threads(self):
        self._is_ongoing = True
        with ThreadPoolExecutor() as executor:
            future_policy = {
                executor.submit(self._call_policy, policy): policy
                for policy in self._policies
            }
            for future in as_completed(future_policy):
                self._handle_errors(policy=future_policy[future],
                                    future=future)
        self._is_ongoing = False

    def _call_policy(self, policy: Policy):
        if TIME_THRESHOLD <= utc_datetime().timestamp():
            if self._is_ongoing:
                _LOG.warning('Job time threshold has been exceeded. '
                             'All the consequent rules will be skipped.')
            self._is_ongoing = False
            self._finishing_reason = \
                'Job time exceeded the maximum possible ' \
                'execution time. The rule was not executed ' \
                'and was skipped'
        if not self._is_ongoing:
            self._add_skipped_item(self.get_policy_region(policy),
                                   policy.name, self._finishing_reason)
            return
        # policy()
        DumpFindingsPolicy(policy=policy, output_dir=self._findings_dir)()

    @staticmethod
    def _skipped_item(name: str, reason: str) -> dict:
        return {name: {'reason': reason}}

    @staticmethod
    def _failed_item(name: str, error: Exception) -> dict:
        e = error.response['Error']['Message'] if isinstance(
            error, ClientError) else str(error)
        return {name: [e, ''.join(traceback.format_exception(
            type(error), error, error.__traceback__))
                       ]}

    def _add_skipped_item(self, region: str, name: str, reason: str):
        with self._lock:
            self._skipped.setdefault(region, {}).update(
                self._skipped_item(name, reason))

    def _add_failed_item(self, region: str, name: str, error: Exception):
        with self._lock:
            self._failed.setdefault(region, {}).update(
                self._failed_item(name, error)
            )

    @staticmethod
    def get_policy_region(policy: Policy) -> str:
        """Returns the policy's region which will be used for reports: if the
        policy is multi-regional, 'multiregion' is returned, if
        the policy is not global, its real region is returned"""
        region = policy.options.region
        return MULTIREGION if str(policy.data.get(
            'metadata', {}).get('multiregional')).lower() in [
                                  'true', 't', 'yes'] or not region else region

    def _handle_errors(self, policy: Policy, future: Future = None):
        raise NotImplementedError


class AWSRunner(Runner):
    @property
    def cloud(self) -> str:
        return AWS

    def _handle_errors(self, policy: Policy, future: Future = None):
        name, region = policy.name, self.get_policy_region(policy)
        try:
            future.result() if future else self._call_policy(policy)
        except ClientError as error:
            error_code = error.response['Error']['Code']
            error_reason = error.response['Error']['Message']

            if error_code in ACCESS_DENIED_ERROR_CODE.get(self.cloud):
                _LOG.warning(f'Policy \'{name}\' is skipped. '
                             f'Reason: \'{error_reason}\'')
                self._add_skipped_item(region, name, error_reason)
            elif error_code in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud):
                _LOG.warning(
                    f'Policy \'{name}\' is skipped due to invalid '
                    f'credentials. All the subsequent rules will be skipped')
                self._add_skipped_item(region, name, error_reason)
                self._is_ongoing = False
                self._finishing_reason = error_reason
            else:
                _LOG.warning(f'Policy \'{name}\' has failed. '
                             f'Client error occurred. '
                             f'Code: \'{error_code}\'. '
                             f'Reason: {error_reason}')
                self._add_failed_item(region, name, error)
        except Exception as error:
            _LOG.error(f'Policy \'{name}\' has failed. Unexpected '
                       f'error has occurred: \'{error}\'')
            self._add_failed_item(region, name, error)


class AZURERunner(Runner):
    @property
    def cloud(self) -> str:
        return AZURE

    def _handle_errors(self, policy: Policy, future: Future = None):
        name, region = policy.name, self.get_policy_region(policy)
        try:
            future.result() if future else self._call_policy(policy)
        except CloudError as error:
            error_code = error.error
            error_reason = error.message.split(':')[-1].strip()
            if error_code in INVALID_CREDENTIALS_ERROR_CODES.get(self.cloud):
                _LOG.warning(
                    f'Policy \'{name}\' is skipped due to invalid '
                    f'credentials. All the subsequent rules will be skipped')
                self._add_skipped_item(region, name, error_reason)
                self._is_ongoing = False
                self._finishing_reason = error_reason
            else:
                _LOG.warning(f'Policy \'{name}\' has failed. '
                             f'Client error occurred. '
                             f'Code: \'{error_code}\'. '
                             f'Reason: {error_reason}')
                self._add_failed_item(region, name, error)
        except Exception as error:
            _LOG.error(f'Policy \'{name}\' has failed. Unexpected '
                       f'error has occurred: \'{error}\'')
            self._add_failed_item(region, name, error)


class GCPRunner(Runner):
    @property
    def cloud(self) -> str:
        return GOOGLE

    def _handle_errors(self, policy: Policy, future: Future = None):
        name, region = policy.name, self.get_policy_region(policy)
        try:
            future.result() if future else self._call_policy(policy)
        except GoogleAuthError as error:
            error_reason = str(error.args[-1])
            _LOG.warning(
                f'Policy \'{name}\' is skipped due to invalid '
                f'credentials. All the subsequent rules will be skipped')
            self._add_skipped_item(region, name, error_reason)
            self._is_ongoing = False
            self._finishing_reason = error_reason
        except HttpError as error:
            if error.status_code == 403:
                self._add_skipped_item(region, name, error.reason)
            else:
                self._add_failed_item(region, name, error)
        except Exception as error:
            _LOG.error(f'Policy \'{name}\' has failed. Unexpected '
                       f'error has occurred: \'{error}\'')
            self._add_failed_item(region, name, error)


class Scan:
    def __init__(self, policies_files: list, cloud: str,
                 work_dir: str = None, findings_dir: str = None,
                 cache_period: int = 30):
        self._job_id = environment_service.batch_job_id()
        self._work_dir = work_dir or os_service.create_workdir(self._job_id)
        self._policies_files = policies_files
        self._cloud = cloud
        self._cache_period = cache_period
        self._findings_dir = findings_dir

        self._policies: List[Policy] = []
        self._runner: Union[type(None), Runner] = None

    @staticmethod
    def _load_policies_by_one(config: Config) -> List[Policy]:
        """In case we want to continue the scan even if one policy file is
        invalid"""
        _LOG.info('Loading policies files one by one')
        _all_policies = []
        _files = config.get('configs') or []
        for _file in _files:
            config.update({'configs': [_file, ]})
            try:
                _policies = load_policies(config)
                _all_policies.extend(_policies)
            except (Exception, SystemExit) as error:
                _message = ERROR_WHILE_LOADING_POLICIES_MESSAGE.format(
                    policies=Path(_file).name)
                _LOG.warning(_message[0].upper() + _message[1:] + f'; {error}')
        return _all_policies

    @staticmethod
    def _load_policies(config: Config) -> List[Policy]:
        _LOG.info('Loading all the policies files at once')
        try:
            return load_policies(config)
        except (Exception, SystemExit) as error:
            # TODO for Custom-Core: raise something more informative
            #  instead of sys.exit(1)
            _message = ERROR_WHILE_LOADING_POLICIES_MESSAGE.format(
                policies=", ".join(Path(_file).name for _file in
                                   config.get('configs') or []))
            _LOG.error(_message[0].upper() + _message[1:] + f'; {error}')
            raise ExecutorException(
                step_name=STEP_LOAD_POLICIES,
                reason=_message
            )

    @staticmethod
    def _is_multiregional(policy: Policy) -> bool:
        return policy.data.get('metadata', {}).get('multiregional') == 'true'

    def _set_policy_output(self, policy: Policy) -> None:
        if self._is_multiregional(policy):
            policy.options.output_dir = self._output_dir()
        else:
            policy.options.output_dir = self._output_dir(policy.options.region)

    def load_from_regions_to_rules(self, mapping: dict) -> List[Policy]:
        """
        Slow option due to multiple executions of load_policies for the
        same yaml files, but easy to write and understand
        Expected mapping:
        {
            'eu-central-1': {'epam-aws-005..', 'epam-aws-006..'},
            'eu-west-1': {'epam-aws-006..', 'epam-aws-007..'}
        }
        """
        multiregional = set()  # track already loaded multi-regional policies
        all_policies = []
        config = self._config()
        for region, rules in mapping.items():
            config.regions = [region, ]
            config.policy_filters = list(rules - multiregional)

            _policies = self._load_policies(config)
            all_policies.extend(_policies)
            multiregional.update(p.name for p in filter(self._is_multiregional,
                                                        _policies))
        for policy in all_policies:
            self._set_policy_output(policy)
        return all_policies

    def load_from_rules_to_regions(self, mapping: dict) -> List[Policy]:
        """
        Probably faster solution than the one above but a bit tricky since I
        use deepcopy, and I'm actually not quite sure whether it does no harm
        for Custom-Core's policies or memory.

        The thing is: we must avoid using Custom-Core's `load_policies`
        (still once it must be used), because it's too slow, especially
        for large YAMLs, and we are aware of precedents where it occupied
        too much RAM and did not free it (the container used to say `Adios`
        in the middle of execution).
        Here we invoke that method only once and then distribute policies
        across different regions using deepcopy
        Expected mapping:
        {
            'epam-aws-005..': {'eu-central-1'},
            'epam-aws-006..': {'eu-west-1', 'eu-central-1'},
            'epam-aws-007..': {'eu-west-1'}
        }
        """
        all_policies = []
        config = self._config()  # multi-regional output dir
        config.regions = []  # One instance of each rule for default region
        loaded = self._load_policies(config)
        for policy in loaded:
            if self._is_multiregional(policy):
                all_policies.append(policy)
                continue

            demanded_regions = mapping.get(policy.name) or set()
            pr = policy.options.region
            if pr in demanded_regions:
                self._set_policy_output(policy)
                all_policies.append(policy)
            for left_region in demanded_regions - {pr}:
                policy_copy = deepcopy(policy)
                policy_copy.options.region = left_region
                self._set_policy_output(policy_copy)
                all_policies.append(policy_copy)
        return all_policies

    @property
    def policies(self) -> List[Policy]:
        if not self._policies:
            _LOG.debug('Loading policies from files using \'load_policies\' '
                       'Custom-Core\'s function')
            config = self._config()
            # self._policies = self._load_policies_by_one(config)
            self._policies = self._load_policies(config)
            _LOG.debug('The policies were loaded from YAML to '
                       'Custom Core collection or Policies')
            _LOG.info(f'Setting \'{MULTIREGION}\' output dir for global '
                      f'policies')
            for policy in self._policies:
                self._set_policy_output(policy)
        return self._policies

    @policies.setter
    def policies(self, value: List[Policy]):
        self._policies = value

    @property
    def runner(self) -> Runner:
        if not self._runner:
            _cloud_to_runner_class = {
                AWS: AWSRunner,
                AZURE: AZURERunner,
                GOOGLE: GCPRunner
            }
            runner_class = _cloud_to_runner_class.get(self._cloud)
            self._runner = runner_class(
                self.policies, self._findings_dir
            )
        return self._runner

    @property
    def regions(self) -> list:
        regions = environment_service.target_regions()
        _LOG.debug(f'Regions to scan before making changes: {regions}')
        if self._cloud == GOOGLE:
            # apparently, there is no difference what region(s) we set here :)
            regions = []
        elif self._cloud == AZURE:
            # the same thing :)
            regions = []
            # later the whole report will be restricted to only those regions
            # that are activated by user. They are
            # in environment_service.target_regions()
        _LOG.debug(f'Regions to scan after making changes: {regions}')
        return regions

    def _output_dir(self, region: str = None) -> str:
        """
        Default output dir is multiregional.
        """
        return str(Path(self._work_dir, region or MULTIREGION).absolute())

    def _config(self, regions: Optional[List] = None) -> Config:
        options = {
            'region': CLOUD_COMMON_REGION.get(self._cloud),
            'regions': regions or self.regions,
            'cache': 'cloud-custodian.cache',
            'cache_period': self._cache_period,
            'command': 'c7n.commands.run',
            'config': None,
            'configs': self._policies_files,
            'output_dir': self._output_dir(),  # multi-regional
            'subparser': 'run',
            'policy_filters': [],
            'resource_types': [],
            'verbose': None,
            'quiet': False,
            'debug': False,
            'skip_validation': False,
            'vars': None,
        }
        return Config.empty(**options)

    @timeit
    def execute(self) -> Tuple[Dict, Dict]:
        is_concurrent = environment_service.is_concurrent()
        _LOG.info(f'Starting {"concurrent" if is_concurrent else ""} '
                  f'scan for job \'{self._job_id}\'')
        self.runner.start_threads() if is_concurrent else self.runner.start()
        _LOG.info(f'Scan for job \'{self._job_id}\' has ended')
        return self.runner.skipped, self.runner.failed


def _exception_to_str(e: Exception) -> str:
    e_str = str(e)
    return e_str if isinstance(
        e, ExecutorException) else f'{type(e).__name__}: {e_str}'


def fetch_licensed_ruleset_list(tenant: Tenant, licensed: dict):
    """
    Designated to execute preliminary licensed Job instantiation, which
    verifies permissions to create a demanded entity.
    :parameter tenant: Tenant of the issuer
    :parameter licensed: Dict - non-empty collection of licensed rulesets
    :raises: ExecutorException - given parameter absence or prohibited action
    :return: List[Dict]
    """
    job_id = environment_service.batch_job_id()

    lm_service = license_manager_service
    payload = dict(
        job_id=job_id,
        customer=tenant.customer_name,
        tenant=tenant.name,
        ruleset_map=licensed
    )
    _iterator = (item[0] for item in payload.items() if item[1] is None)
    absent = next(_iterator, None)
    if absent is not None:
        raise ExecutorException(reason=f'\'{absent}\' has not been assigned',
                                step_name=STEP_GRANT_JOB)

    _LOG.debug(f'Going to license a Job:\'{job_id}\'.')

    licensed_job, issue = None, ''

    try:
        licensed_job: Optional[dict] = lm_service.instantiate_licensed_job_dto(
            **payload
        )
    except BalanceExhaustion as fj:
        issue = str(fj)

    except InaccessibleAssets as ij:
        issue = str(ij)
        rulesets = list(ij)

        customer_name = tenant.customer_name
        customer: Customer = modular_service.get_customer(
            customer=customer_name)

        scheduled_job_name = environment_service.scheduled_job_name()
        mail_configuration = setting_service.get_mail_configuration()

        if scheduled_job_name and mail_configuration and rulesets and customer:
            header = f'Scheduled-Job:\'{scheduled_job_name}\' of ' \
                     f'\'{customer_name}\' customer'

            _LOG.info(f'{header} - is going to be retrieved.')
            job = scheduler_service.get(
                name=scheduled_job_name, customer=customer_name
            )
            if not job:
                _LOG.error(f'{header} - could not be found.')

            if not scheduler_service.update_job(item=job, is_enabled=False):
                _LOG.error(f'{header} - could not be deactivated.')
            else:
                _LOG.info(f'{header} - has been deactivated')
                subject = f'{tenant.name} job-rescheduling notice'
                if not notification_service.send_rescheduling_notice_notification(
                        recipients=customer.admins, subject=subject,
                        tenant=tenant, scheduled_job_name=scheduled_job_name,
                        ruleset_list=rulesets,
                        customer=customer_name
                ):
                    _LOG.error('Job-Rescheduling notice was not sent.')
                else:
                    _LOG.info('Job-Rescheduling notice has been sent.')

        elif not mail_configuration:
            _LOG.warning(
                'No mail configuration has been attached, skipping '
                f' job-rescheduling notice of \'{scheduled_job_name}\'.'
            )

    if not licensed_job:
        reason = 'Job execution could not be granted.'
        if issue:
            reason += f' {issue}'
        raise ExecutorException(reason=reason, step_name=STEP_GRANT_JOB)

    _LOG.info(f'Job {job_id} has been permitted to be commenced.')
    return lm_service.instantiate_job_sourced_ruleset_list(
        licensed_job_dto=licensed_job
    )


def log_requirements():
    """
    Logs all the installed requirements. Can be useful for debug
    """
    p = subprocess.run('pip freeze'.split(), capture_output=True)
    if p.returncode == 0:
        _LOG.debug('Installed requirements: ')
        _LOG.debug(p.stdout.decode())


# just common hunks of code for scan flows (three functions below)


def get_licensed_ruleset_dto_list(tenant: Tenant) -> list:
    """
    Preliminary step, given an affected license and respective ruleset(s)
    """
    affected_license = environment_service.affected_licenses() or []
    licensed_rulesets = environment_service.licensed_ruleset_map(
        license_key_list=affected_license
    )
    licensed_ruleset_dto_list = []
    if affected_license and licensed_rulesets:
        licensed_ruleset_dto_list = fetch_licensed_ruleset_list(
            tenant=tenant, licensed=licensed_rulesets
        )
    return licensed_ruleset_dto_list


def upload_to_siem(tenant: Tenant, started_at: str,
                   detailed_report: Dict[str, List[Dict]],
                   findings_dir: str, job_id: str):
    # dojo
    dojo_adapters = integration_service.get_dojo_adapters(tenant)
    if dojo_adapters:
        dojo_report = report_service.formatted_to_dojo_policy_report(
            detailed_report=detailed_report, cloud=tenant.cloud
        )
        # TODO push in thread in case multiple available
        for adapter in dojo_adapters:
            try:
                adapter.push_notification(
                    job_id=job_id,
                    started_at=started_at,
                    customer_display_name=tenant.customer_name,
                    tenant_display_name=tenant.name,
                    policy_report=dojo_report
                )
            except Exception as e:
                _LOG.warning(
                    f'Unexpected error occurred pushing findings to dojo {e}')
    # SH
    for adapter in integration_service.get_security_hub_adapters(tenant):
        try:
            adapter.push_notification(
                findings_folder=findings_dir
            )
        except Exception as e:
            _LOG.warning(
                f'Unexpected error occurred pushing findings to SH {e}')


def load_reports(work_dir: str, skipped_policies: dict,
                 failed_policies: dict, tenant: Tenant
                 ) -> Tuple[Dict, Dict, Dict, Dict]:
    """
    Just common hunk of code. Maybe someday it will be refactored
    :param tenant:
    :param work_dir:
    :param skipped_policies:
    :param failed_policies:ram cloud:
    :return:
    """
    cloud = tenant.cloud
    statistics, api_calls = statistics_service.collect_statistics(
        work_dir=work_dir,
        failed_policies=failed_policies,
        skipped_policies=skipped_policies,
        tenant=tenant
    )
    raw_detailed_report = report_service.generate_detailed_report(
        work_dir=work_dir
    )
    if cloud == AZURE:
        raw_detailed_report[MULTIREGION] = raw_detailed_report.pop(
            AZURE_COMMON_REGION)
        # ----- this code can be commented -----
        raw_detailed_report = report_service.reformat_azure_report(
            detailed_report=raw_detailed_report,
            target_regions=set(environment_service.target_regions())
            # TODO get target regions from BatchResult for ED jobs?
        )
        # ----- this code can be commented -----

    _LOG.debug('Reformatting detailed report')
    detailed_report = report_service.format_detailed_report(
        detailed_report=raw_detailed_report,
        cloud_name=cloud
    )
    report = report_service.generate_report(
        detailed_report=raw_detailed_report)
    return report, detailed_report, statistics, api_calls


def retrieve_batch_result(br_uuid: str) -> BatchResults:
    _LOG.info(f'The job is event-driven. Querying BatchResults '
              f'item with id: `{br_uuid}`')
    _batch_results = BatchResults.get_nullable(hash_key=br_uuid)
    if not _batch_results:
        raise ExecutorException(
            step_name=STEP_GET_BATCH_RESULTS_ED,
            reason=f'BatchResults item with id {br_uuid} not found '
                   f'for the event-driven job'
        )
    elif _batch_results.status == STATUS_SUCCEEDED:
        raise ExecutorException(
            step_name=STEP_BATCH_RESULT_ALREADY_SUCCEEDED,
            reason=f'BatchResults item with id {br_uuid} has status `SUCCEEDED`'
        )
    return _batch_results


def get_credentials(tenant: Tenant,
                    batch_results: Optional[BatchResults] = None) -> dict:
    """
    Tries to retrieve credentials to scan the given tenant with such
    priorities:
    1. env "CREDENTIALS_KEY" - gets key name and then gets credentials from SSM.
       This is the oldest solution, in can sometimes be used if the job is
       standard and a user has given credentials directly; The SSM parameter
       is removed after the creds are received.
    2. Only for event-driven jobs. Gets credentials_key (SSM parameter name)
       from "batch_result.credentials_key". Currently, the option is obsolete.
       API in no way will set credentials key there. But maybe some time...
    3. 'CUSTODIAN_ACCESS' key in the tenant's parent_map. It points to the
       parent with type 'CUSTODIAN_ACCESS' as well. That parent is linked
       to an application with credentials
    4. Customer's custodian application - access_application_id. When a
       Custodian application is activated for a certain customer,
       access_application_id can be set for each CLOUD (AWS, AZURE, GCP). If
       the application exists and access_application_id exists, its creds
       are used.
    5. Credentials from Custodian's CredentialsManager. It's also kind of
       an old solution, native to Custodian-as-a-service. It has right to live.
    6. Maestro management_parent_id -> management creds. Tries to resolve
       management parent from tenant and then management credentials. This
       option can be used only if the corresponding env is set to 'true'.
       Must be explicitly allowed because the option is not safe.
    """
    mcs = modular_service.modular_client.maestro_credentials_service()
    _log_start = 'Trying to get credentials from '
    credentials: dict = {}
    # 1.
    if not credentials:
        _LOG.info(_log_start + '\'CREDENTIALS_KEY\' env')
        credentials = credentials_service.get_credentials_from_ssm()
        if credentials and tenant.cloud == GOOGLE:
            credentials = credentials_service.google_credentials_to_file(
                credentials)
    # 2.
    if not credentials and batch_results and batch_results.credentials_key:
        _LOG.info(_log_start + 'batch_results.credentials_key')
        credentials = credentials_service.get_credentials_from_ssm(
            batch_results.credentials_key)
        if credentials and tenant.cloud == GOOGLE:
            credentials = credentials_service.google_credentials_to_file(
                credentials)
    # 3.
    if not credentials:
        _LOG.info(_log_start + '`CUSTODIAN_ACCESS` parent')
        application = modular_service.get_tenant_application(
            tenant, TENANT_PARENT_MAP_CUSTODIAN_ACCESS_TYPE
        )
        if application:
            _creds = mcs.get_by_application(application, tenant)
            if _creds:
                credentials = _creds.dict()
    # 4.
    if not credentials:
        _LOG.info(_log_start + 'customer`s access_application_id for cloud')
        application = modular_service.get_tenant_application(
            tenant, TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE
        )
        if application:
            assert application.type == CUSTODIAN_LICENSES_TYPE, \
                'Something wrong with applications configuration'
            meta = CustodianLicensesApplicationMeta.from_dict(
                application.get_json().get('meta') or {})
            access_application_id = meta.access_application_id(tenant.cloud)
            if access_application_id:
                _creds = mcs.get_by_application(access_application_id, tenant)
                if _creds:  # not a dict
                    credentials = _creds.dict()
    # 5.
    if not credentials:
        _LOG.info(_log_start + 'CredentialsManager')
        credentials = credentials_service.get_credentials_for_tenant(tenant)
        if credentials and tenant.cloud == GOOGLE:
            credentials = credentials_service.google_credentials_to_file(
                credentials)
    # 6.
    if not credentials and environment_service.is_management_creds_allowed():
        _LOG.info(_log_start + 'Maestro management parent & application')
        _creds = mcs.get_by_tenant(tenant=tenant)
        if _creds:  # not a dict
            credentials = _creds.dict()

    if credentials:
        credentials = mcs.complete_credentials_dict(
            credentials=credentials,
            tenant=tenant
        )
    return credentials


def get_rules_to_exclude(tenant: Tenant) -> Set[str]:
    """
    Returns a set of rules to exclude for the given tenant.
    :param tenant:
    :return:
    """
    exclude = set()
    tenant_setting = modular_service.get_tenant_bound_setting(tenant)
    if tenant_setting:
        _LOG.info('Updating rule to exclude with ones from tenant setting')
        exclude.update(
            tenant_setting.value.as_dict().get(RULES_TO_EXCLUDE) or []
        )
    parent = modular_service.get_tenant_parent(
        tenant, TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE
    )
    if parent:
        meta = ParentMeta.from_dict(parent.meta.as_dict())
        exclude.update(meta.rules_to_exclude or [])
    return exclude


def batch_results_job(batch_results: BatchResults):
    started_at = utc_iso()
    work_dir = Path(WORK_DIR, batch_results.id)
    work_dir.mkdir(parents=True, exist_ok=True)
    findings_dir = os.path.join(work_dir, FINDINGS_FOLDER)
    tenant: Tenant = tenant_service.get_tenant(batch_results.tenant_name)
    cloud: str = tenant.cloud.upper()
    credentials = get_credentials(tenant, batch_results)
    if not credentials:
        raise ExecutorException(
            step_name=STEP_ASSERT_CREDENTIALS,
            reason=f'Could not resolve credentials '
                   f'for account: {tenant.project}'
        )

    whole_ruleset = policy_service.assure_event_driven_ruleset(cloud)
    prepared_ruleset = policy_service.separate_ruleset(
        from_=whole_ruleset,
        work_dir=work_dir,
        rules_to_exclude=get_rules_to_exclude(tenant),
        rules_to_keep=set(
            chain.from_iterable(batch_results.regions_to_rules().values()))
        # or set(batch_results.rules_to_regions().keys())
    )
    scan = Scan(policies_files=[str(prepared_ruleset)], cloud=cloud,
                work_dir=str(work_dir), findings_dir=findings_dir)
    with EnvironmentContext(credentials, reset_all=False):
        _LOG.info('Loading policies for multiple regions')
        scan.policies = scan.load_from_regions_to_rules(
            batch_results.regions_to_rules()
        )
        _LOG.info('Loading has finished')
        skipped_policies, failed_policies = scan.execute()

    report, detailed_report, statistics, api_calls = load_reports(
        str(work_dir), skipped_policies, failed_policies, tenant
    )
    _LOG.debug('Preparing a user detailed report.')
    findings_stash_path = str(PurePosixPath(
        FINDINGS_FOLDER, utc_datetime().date().isoformat(),
        f'{tenant.project}.json'))
    saved_findings = s3_service.get_json_file_content(
        bucket_name=environment_service.statistics_bucket_name(),
        path=str(findings_stash_path))
    received_findings = FindingsCollection.from_detailed_report(
        detailed_report)

    if saved_findings:
        _LOG.info('Saved findings were found. '
                  'Deserializing them to a FindingsCollection')
        saved_findings = FindingsCollection.deserialize(saved_findings)
    else:
        _LOG.info('Saved findings were not found. '
                  'Creating an empty FindingsCollection')
        saved_findings = FindingsCollection()
    _LOG.debug('Retrieving difference between received and saved findings')
    difference: FindingsCollection = received_findings - saved_findings
    _LOG.debug(f'Difference received with {len(difference)} items')
    # if 'standard_points' in difference.keys_to_keep:
    #     difference.keys_to_keep.remove('standard_points')
    _LOG.debug('Updating the existing findings with the received ones')
    saved_findings.update(received_findings)

    _LOG.debug('------------ Uploading to S3 block begins ------------')
    # force 10 workers because the used node has 1 vCPU, but we need
    # more of them in order to upload multiple files concurrently
    executor = ThreadPoolExecutor(max_workers=10)
    _LOG.debug(f'ThreadPoolExecutor for uploading s3 files was created. '
               f'Number of workers: {executor._max_workers}')
    statistics_b = environment_service.statistics_bucket_name()
    reports_b = environment_service.reports_bucket_name()
    _LOG.debug(f'Statistics bucket: {statistics_b}\n'
               f'Reports bucket: {reports_b}')
    _upload = s3_service.client.put_object
    _path = lambda *_files: str(PurePosixPath(batch_results.id, *_files))

    _LOG.debug('Submitting thread pool executor jobs')
    executor.submit(_upload, statistics_b, findings_stash_path,
                    saved_findings.json())
    executor.submit(_upload, reports_b, _path(DETAILED_REPORT_FILE),
                    json.dumps(detailed_report, separators=(',', ':')))
    executor.submit(_upload, reports_b, _path(DIFFERENCE_FILE),
                    difference.json())
    executor.submit(_upload, reports_b, _path(REPORT_FILE),
                    json.dumps(report, separators=(',', ':')))
    executor.submit(_upload, statistics_b, _path(STATISTICS_FILE),
                    json.dumps(statistics, separators=(',', ':')))
    executor.submit(_upload, statistics_b, _path(API_CALLS_FILE),
                    json.dumps(api_calls, separators=(',', ':')))
    for root, dirs, files in os.walk(findings_dir):
        for file in files:
            full_path = Path(root, file)
            with open(full_path, 'r') as obj:
                body = obj.read()
            relative_path = full_path.relative_to(findings_dir)
            executor.submit(_upload, reports_b,
                            _path(FINDINGS_FOLDER, relative_path), body)
    _LOG.info('All "upload to s3" threads were submitted. Moving on '
              'keeping the executor open. We will wait for '
              'it to finish in the end')
    _LOG.debug('------------ Uploading to S3 block ends ------------')
    _LOG.debug('------------ Uploading to SIEMs block begins ------------')
    upload_to_siem(
        tenant, started_at, detailed_report, findings_dir, batch_results.id
    )
    _LOG.debug('------------ Uploading to SIEMs block ends ------------')

    _LOG.debug('Waiting for "upload to S3" executor to finish')
    executor.shutdown(wait=True)
    _LOG.debug('Executor was shutdown')


def multi_account_event_driven_job() -> int:
    for br_uuid in environment_service.batch_results_ids():
        batch_results: Optional[BatchResults] = None
        try:
            batch_results = retrieve_batch_result(br_uuid)
            _LOG.info(f'Starting job for batch result {br_uuid}')
            batch_results_job(batch_results)
            _LOG.info(f'Job for batch result {br_uuid} has finished')
            _LOG.info('Updating batch results item')
            batch_results.rules = {}  # in order to reduce the size of the item
            batch_results.rulesets = []
            batch_results.credentials_key = None
            batch_results.status = STATUS_SUCCEEDED  # is set in job-updater
        except ExecutorException as ex_exception:
            _LOG.error(f'An error \'{ex_exception}\' occurred during the job. '
                       f'Setting job failure reason.')
            if isinstance(batch_results, BatchResults):  # may be none
                batch_results.status = STATUS_FAILED
                batch_results.reason = _exception_to_str(ex_exception)
        except Exception as exception:
            _LOG.error(f'An unexpected error \'{exception}\' occurred during '
                       f'the job. Setting job failure reason.')
            if isinstance(batch_results, BatchResults):
                batch_results.status = STATUS_FAILED
                batch_results.reason = _exception_to_str(exception)
        if isinstance(batch_results, BatchResults):
            _LOG.info('Saving batch results item')
            batch_results.stopped_at = utc_iso()
            batch_results.save()
    if environment_service.is_docker() and WORK_DIR:
        os_service.clean_workdir(work_dir=WORK_DIR)
    return 0


def standard_job() -> int:
    work_dir = WORK_DIR
    exit_code: int = 0
    findings_dir = os.path.join(work_dir, FINDINGS_FOLDER)
    try:
        job_updater_service.set_created_at()

        _tenant: Tenant = tenant_service.get_tenant()
        _cloud: str = _tenant.cloud.upper()
        _job: Job = job_updater_service.job
        _LOG.info(
            f'{environment_service.job_type().capitalize()} job \'{_job.job_id}\' has started;\n'
            f'Cloud: \'{_cloud.upper()}\';\n'
            f'Tenant: \'{_tenant.name}\';\n'
            f'Current custodian custom core version: '
            f'\'{environment_service.current_custom_core_version()}\';\n'
            f'Minimum custodian custom core version: '
            f'\'{environment_service.min_custom_core_version()}\';')
        _LOG.debug(f'Entire sys.argv: {sys.argv}\n'
                   f'Environment: {environment_service}')

        job_updater_service.set_started_at()

        licensed_ruleset_dto_list = get_licensed_ruleset_dto_list(_tenant)
        standard_ruleset_dto_list = list(
            r.get_json() for r in ruleset_service.target_rulesets())

        job_updater_service.update_scheduled_job()
        credentials = get_credentials(_tenant)
        if not credentials:
            raise ExecutorException(
                step_name=STEP_ASSERT_CREDENTIALS,
                reason=f'Could not resolve credentials for account: {_tenant.project}'
            )
        policies_files = policy_service.get_policies(
            work_dir=work_dir,
            ruleset_list=standard_ruleset_dto_list + licensed_ruleset_dto_list,
            rules_to_exclude=get_rules_to_exclude(_tenant),
            rules_to_keep=_job.rules_to_scan
        )

        scan = Scan(policies_files=policies_files, cloud=_cloud,
                    findings_dir=findings_dir)

        with EnvironmentContext(credentials, reset_all=False):
            skipped_policies, failed_policies = scan.execute()

        report, detailed_report, statistics, api_calls = load_reports(
            str(work_dir), skipped_policies, failed_policies, _tenant
        )

        findings_stash_path = str(PurePosixPath(
            FINDINGS_FOLDER, utc_datetime().date().isoformat(),
            f'{_tenant.project}.json'))

        received_findings = FindingsCollection.from_detailed_report(
            detailed_report
        )
        saved_findings = s3_service.get_json_file_content(
            bucket_name=environment_service.statistics_bucket_name(),
            path=str(findings_stash_path)
        )
        if saved_findings:
            _LOG.info('Saved findings were found. '
                      'Deserializing them to a FindingsCollection')
            saved_findings = FindingsCollection.deserialize(saved_findings)
        else:
            _LOG.info('Saved findings were not found. '
                      'Creating an empty FindingsCollection')
            saved_findings = FindingsCollection()

        _LOG.debug('Updating the existing findings with the received ones')
        saved_findings.update(received_findings)

        # force 10 workers because the used node has 1 vCPU, but we need
        # more of them in order to upload multiple files concurrently
        executor = ThreadPoolExecutor(max_workers=10)
        _LOG.debug(f'ThreadPoolExecutor for uploading s3 files was created. '
                   f'Number of workers: {executor._max_workers}')
        statistics_b = environment_service.statistics_bucket_name()
        reports_b = environment_service.reports_bucket_name()
        _LOG.debug(f'Statistics bucket: {statistics_b}\n'
                   f'Reports bucket: {reports_b}')
        _upload = s3_service.client.put_object
        _path = lambda *_files: str(PurePosixPath(_job.job_id, *_files))

        _LOG.debug('Submitting thread pool executor jobs')
        executor.submit(_upload, statistics_b, findings_stash_path,
                        saved_findings.json())
        executor.submit(_upload, statistics_b, _path(STATISTICS_FILE),
                        json.dumps(statistics, separators=(',', ':')))
        executor.submit(_upload, statistics_b, _path(API_CALLS_FILE),
                        json.dumps(api_calls, separators=(',', ':')))
        executor.submit(_upload, reports_b, _path(DETAILED_REPORT_FILE),
                        json.dumps(detailed_report, separators=(',', ':')))
        executor.submit(_upload, reports_b, _path(REPORT_FILE),
                        json.dumps(report, separators=(',', ':')))
        for root, dirs, files in os.walk(findings_dir):
            for file in files:
                full_path = Path(root, file)
                with open(full_path, 'r') as obj:
                    body = obj.read()
                relative_path = full_path.relative_to(findings_dir)
                executor.submit(
                    _upload, reports_b, _path(
                        FINDINGS_FOLDER, relative_path), body)
        _LOG.info('All "upload to s3" threads were submitted. Moving on '
                  'keeping the executor open.')

        _LOG.info('Going to upload to SIEM')
        upload_to_siem(
            tenant=_tenant, started_at=_job.started_at,
            detailed_report=detailed_report, findings_dir=findings_dir,
            job_id=_job.job_id
        )

        _LOG.debug('Waiting for "upload to S3" executor to finish')
        executor.shutdown(wait=True)
        _LOG.debug('Executor was shutdown')

        job_updater_service.set_succeeded_at()
        _LOG.info(f'Job \'{_job.job_id}\' has ended')
    except ExecutorException as ex_exception:
        _LOG.error(f'An error \'{ex_exception}\' occurred during the job. '
                   f'Setting job failure reason.')
        reason = _exception_to_str(ex_exception)
        job_updater_service.set_failed_at(reason)

        exit_code = 1
        _log = 'An error occurred on step `{step}`. Setting error code to {code}'
        if ex_exception.step_name == STEP_GRANT_JOB:
            exit_code = 2
            _LOG.debug(_log.format(step=STEP_GRANT_JOB, code=str(exit_code)))
    except Exception as exception:
        _LOG.error(f'An unexpected error \'{exception}\' occurred during '
                   f'the job. Setting job failure reason.')
        reason = _exception_to_str(exception)
        job_updater_service.set_failed_at(reason)
        exit_code = 1
    finally:
        if environment_service.is_docker() and work_dir:
            os_service.clean_workdir(work_dir=work_dir)
    return exit_code


def main(command: Optional[list] = None, environment: Optional[dict] = None):
    """
    :parameter command: Optional[list]
    :parameter environment: Optional[dict]
    :return: None
    """
    log_requirements()
    global TIME_THRESHOLD, WORK_DIR
    command = command or []
    environment = environment or {}
    environment_service.override_environment(environment)
    init_services()
    TIME_THRESHOLD = batch_service.get_time_left()
    WORK_DIR = os_service.create_workdir(environment_service.batch_job_id())

    function = standard_job
    if environment_service.is_multi_account_event_driven():
        function = multi_account_event_driven_job

    exit_code = function()
    _LOG.info(
        f'Function: \'{function.__name__}\' finished with code {exit_code}')
    sys.exit(exit_code)


if __name__ == '__main__':
    main(command=sys.argv)
