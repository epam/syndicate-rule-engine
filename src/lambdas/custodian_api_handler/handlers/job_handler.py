from datetime import timedelta
from functools import cached_property
from itertools import chain
from typing import Optional, Set, Tuple, List, Iterable

from botocore.exceptions import ClientError
from modular_sdk.commons.constants import TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE
from modular_sdk.commons.error_helper import RESPONSE_SERVICE_UNAVAILABLE_CODE
from modular_sdk.models.pynamodb_extension.base_model import LastEvaluatedKey as Lek

from helpers import build_response, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_FORBIDDEN_CODE, RESPONSE_BAD_REQUEST_CODE, adjust_cloud, \
    RESPONSE_CREATED, \
    RESPONSE_NO_CONTENT
from helpers.constants import POST_METHOD, GET_METHOD, DELETE_METHOD, \
    TENANT_ATTR, PARAM_TARGET_RULESETS, PARAM_TARGET_REGIONS, \
    CHECK_PERMISSION_ATTR, CUSTOMER_ATTR, TENANT_LICENSE_KEY_ATTR, \
    BATCH_ENV_SUBMITTED_AT, \
    PARAM_USER_ID, JOB_ID_ATTR, TENANTS_ATTR, LIMIT_ATTR, NEXT_TOKEN_ATTR, \
    CUSTOMER_DISPLAY_NAME_ATTR, JOB_SUCCEEDED_STATUS, JOB_FAILED_STATUS, \
    PARAM_CREDENTIALS, AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GOOGLE_CLOUD_ATTR, \
    MULTIREGION, PATCH_METHOD, SCHEDULE_ATTR, NAME_ATTR, \
    BATCH_SCHEDULED_JOB_TYPE, ENABLED, GCP_CLOUD_ATTR, RULES_TO_SCAN_ATTR
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.time_helper import utc_datetime
from lambdas.custodian_api_handler.handlers import AbstractHandler, Mapping
from models.licenses import License
from models.modular.application import CustodianLicensesApplicationMeta
from models.modular.tenants import Tenant
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services.assemble_service import AssembleService
from services.clients.batch import BatchClient
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from services.job_service import JobService, JOB_PENDING_STATUSES
from services.license_manager_service import LicenseManagerService
from services.license_service import LicenseService
from services.rule_meta_service import RuleService
from services.modular_service import ModularService
from services.ruleset_service import RulesetService
from services.scheduler_service import SchedulerService
from services.ssm_service import SSMService

_LOG = get_logger(__name__)


class JobHandler(AbstractHandler):
    def __init__(self, modular_service: ModularService,
                 environment_service: EnvironmentService,
                 job_service: JobService,
                 license_service: LicenseService,
                 license_manager_service: LicenseManagerService,
                 ruleset_service: RulesetService,
                 assemble_service: AssembleService,
                 batch_client: BatchClient,
                 sts_client: StsClient,
                 ssm_service: SSMService,
                 scheduler_service: SchedulerService,
                 rule_service: RuleService):
        self._modular_service = modular_service
        self._environment_service = environment_service
        self._job_service = job_service
        self._license_service = license_service
        self._license_manager_service = license_manager_service
        self._ruleset_service = ruleset_service
        self._assemble_service = assemble_service
        self._batch_client = batch_client
        self._sts_client = sts_client
        self._ssm_service = ssm_service
        self._scheduler_service = scheduler_service
        self._rule_service = rule_service

    @classmethod
    def build(cls) -> 'JobHandler':
        return cls(
            modular_service=SERVICE_PROVIDER.modular_service(),
            environment_service=SERVICE_PROVIDER.environment_service(),
            job_service=SERVICE_PROVIDER.job_service(),
            license_service=SERVICE_PROVIDER.license_service(),
            license_manager_service=SERVICE_PROVIDER.license_manager_service(),
            ruleset_service=SERVICE_PROVIDER.ruleset_service(),
            assemble_service=SERVICE_PROVIDER.assemble_service(),
            batch_client=SERVICE_PROVIDER.batch(),
            sts_client=SERVICE_PROVIDER.sts_client(),
            ssm_service=SERVICE_PROVIDER.ssm_service(),
            scheduler_service=SERVICE_PROVIDER.scheduler_service(),
            rule_service=SERVICE_PROVIDER.rule_service()
        )

    @cached_property
    def mapping(self) -> Mapping:
        """
        These are licensed jobs endpoints. They use only licensed rule-sets.
        And this is the main business case
        :return:
        """
        return {
            '/jobs': {
                POST_METHOD: self.post,
                GET_METHOD: self.query,
            },
            '/jobs/standard': {
                POST_METHOD: self.post_standard,
            },
            '/jobs/{job_id}': {
                GET_METHOD: self.get,
                DELETE_METHOD: self.delete
            },
            '/scheduled-job': {
                POST_METHOD: self.post_scheduled,
                GET_METHOD: self.query_scheduled,
            },
            '/scheduled-job/{name}': {
                GET_METHOD: self.get_scheduled,
                DELETE_METHOD: self.delete_scheduled,
                PATCH_METHOD: self.patch_scheduled
            }
        }

    def _obtain_tenant(self, tenant_name: str,
                       customer: Optional[str] = None) -> Tenant:
        tenant = self._modular_service.get_tenant(tenant=tenant_name)
        if not self._modular_service.is_tenant_valid(tenant, customer):
            _message = f'Active tenant `{tenant_name}` not found'
            _LOG.info(_message)
            return build_response(code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                  content=_message)
        return tenant

    def post_standard(self, event: dict) -> dict:
        """
        Post job for the given tenant. Only not-licensed rule-sets
        """
        customer: str = event.get(CUSTOMER_ATTR)
        tenant_name: str = event.get(TENANT_ATTR)
        target_rulesets: set = event.get(PARAM_TARGET_RULESETS)
        target_regions: set = event.get(PARAM_TARGET_REGIONS)
        credentials: dict = event.get(PARAM_CREDENTIALS)

        tenant = self._obtain_tenant(tenant_name, customer)

        credentials_key = None
        if credentials:
            if not self._environment_service.skip_cloud_identifier_validation():
                _LOG.info('Validating cloud identifier')
                self._validate_cloud_identifier(
                    credentials=credentials,
                    cloud_identifier=tenant.project,
                    cloud=tenant.cloud.upper()
                )
            credentials_key = self._ssm_service.save_data(
                name=tenant.name, value=credentials
            )
        if tenant.cloud not in self._environment_service.allowed_clouds_to_scan():
            _message = f'Scan for `{tenant.cloud}` is not allowed'
            _LOG.info(_message)
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content=_message)
        if not self._environment_service.allow_simultaneous_jobs_for_one_tenant():
            latest = self._job_service.get_last_tenant_job(
                tenant_name, JOB_PENDING_STATUSES)
            if latest:
                return build_response(
                    code=RESPONSE_FORBIDDEN_CODE,
                    content=f'Job {latest.job_id} is already running '
                            f'for tenant {tenant_name}'
                )
        left = self._validate_tenant_last_scan(tenant.name)
        if left:
            return build_response(
                code=RESPONSE_FORBIDDEN_CODE,
                content=f'This tenant can be scanned after {left}'
            )

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=target_regions,
            tenant=tenant
        )
        ids = list(
            (item.id, item.name, item.version) for item in
            self.retrieve_standard_rulesets(tenant, target_rulesets)
        )
        if not ids:
            return build_response(code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                  content='No standard rule-sets found')
        # todo add rules_to_scan here. Currently business does not need this
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            target_regions=list(regions_to_scan),
            target_rulesets=ids,
            credentials_key=credentials_key
        )
        return self._submit_batch_job(event=event, tenant=tenant, envs=envs)

    def retrieve_standard_rulesets(self, tenant: Tenant, names: Set[str]
                                   ) -> Iterable[Ruleset]:
        cloud = adjust_cloud(tenant.cloud)
        assert cloud in (AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR)
        if names:
            return chain.from_iterable([
                self._ruleset_service.iter_standard(
                    customer=tenant.customer_name, name=name, cloud=cloud,
                    event_driven=False, limit=1, active=True
                ) for name in names
            ])
        else:
            return self._ruleset_service.iter_standard(
                customer=tenant.customer_name, cloud=cloud,
                event_driven=False, active=True
            )

    def post(self, event) -> dict:
        """
        Post job for the given tenant. Only licensed rule-sets
        """
        customer: str = event[CUSTOMER_ATTR]
        tenant_name: str = event.get(TENANT_ATTR)
        target_rulesets: set = event.get(PARAM_TARGET_RULESETS)
        target_regions: set = event.get(PARAM_TARGET_REGIONS)
        check_permission: bool = event.get(CHECK_PERMISSION_ATTR)
        credentials: dict = event.get(PARAM_CREDENTIALS)
        rules_to_scan: set = event.get(RULES_TO_SCAN_ATTR)

        tenant = self._obtain_tenant(tenant_name, customer)

        credentials_key = None
        if credentials:
            if not self._environment_service.skip_cloud_identifier_validation():
                _LOG.info('Validating cloud identifier')
                self._validate_cloud_identifier(
                    credentials=credentials,
                    cloud_identifier=tenant.project,
                    cloud=tenant.cloud.upper()
                )
            credentials_key = self._ssm_service.save_data(
                name=tenant.name, value=credentials
            )

        if tenant.cloud not in self._environment_service.allowed_clouds_to_scan():
            _message = f'Scan for `{tenant.cloud}` is not allowed'
            _LOG.info(_message)
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content=_message)
        if not self._environment_service.allow_simultaneous_jobs_for_one_tenant():
            latest = self._job_service.get_last_tenant_job(
                tenant_name, JOB_PENDING_STATUSES)
            if latest:
                return build_response(
                    code=RESPONSE_FORBIDDEN_CODE,
                    content=f'Job {latest.job_id} is already running '
                            f'for tenant {tenant_name}'
                )

        left = self._validate_tenant_last_scan(tenant.name)
        if left:
            return build_response(
                code=RESPONSE_FORBIDDEN_CODE,
                content=f'This tenant can be scanned after {left}'
            )

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=target_regions,
            tenant=tenant
        )
        application = self._modular_service.get_tenant_application(
            tenant, TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE
        )
        if not application:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Custodian application has not been '
                        f'linked to tenant: {tenant.name}'
            )
        meta = CustodianLicensesApplicationMeta(**application.meta.as_dict())
        license_key = meta.license_key(tenant.cloud)
        if not license_key:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Customer {customer} has not been assigned an '
                        f'AWS License yet'
            )
        _license = self._license_service.get_license(license_key)
        if not _license or self._license_service.is_expired(_license):
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Affected license has expired'
            )
        tenant_license_key = _license.customers.as_dict().get(
            customer, {}).get(TENANT_LICENSE_KEY_ATTR)

        if check_permission:
            self.ensure_job_is_allowed(tenant, tenant_license_key)

        # TODO put here custom-core version resolving/updating logic
        affected_licensed, licensed_rulesets = self.retrieve_rulesets_v1(
            _license, tenant, target_rulesets
        )
        if not licensed_rulesets:
            # actually is this block is executed, something really wrong
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='No rule-sets found in license'
            )
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            target_regions=list(regions_to_scan),
            affected_licenses=affected_licensed,
            licensed_rulesets=licensed_rulesets,
            credentials_key=credentials_key
        )
        return self._submit_batch_job(
            event=event,
            tenant=tenant,
            envs=envs,
            rules_to_scan=self._rule_service.resolved_names(
                names=rules_to_scan,
                clouds={adjust_cloud(tenant.cloud)}
            )
        )

    def _submit_batch_job(self, event: dict, tenant: Tenant, envs: dict,
                          rules_to_scan: Optional[Iterable[str]] = None):
        submitted_at = envs.get(BATCH_ENV_SUBMITTED_AT)
        job_owner = event.get(PARAM_USER_ID)
        job_name = f'{tenant.name}-{job_owner}-{submitted_at}'
        job_name = ''.join(ch if ch.isalnum() or ch in {'-', '_'}
                           else '_' for ch in job_name)
        _LOG.debug(f'Going to submit AWS Batch job with name {job_name}')

        response = self._batch_client.submit_job(
            job_name=job_name,
            job_queue=self._environment_service.get_batch_job_queue(),
            job_definition=self._environment_service.get_batch_job_def(),
            environment_variables=envs,
            command=f'python /executor/executor.py'
        )
        _LOG.debug(f'Batch response: {response}')
        if not response:
            return build_response(code=RESPONSE_SERVICE_UNAVAILABLE_CODE,
                                  content='AWS Batch failed to respond')
        ttl_days = self._environment_service.jobs_time_to_live_days()
        ttl = None
        if ttl_days:
            ttl = timedelta(days=ttl_days)
        job = self._job_service.create(dict(
            job_id=response['jobId'],
            job_owner=job_owner,
            tenant_display_name=tenant.name,
            customer_display_name=tenant.customer_name,
            submitted_at=submitted_at,
            ttl=ttl,
            rules_to_scan=list(rules_to_scan or [])
        ))
        self._job_service.save(job)
        return build_response(
            code=RESPONSE_CREATED,
            content=self._job_service.get_job_dto(job=job)
        )

    def retrieve_rulesets_v1(self, _license: License, tenant: Tenant,
                             target_rulesets: Set[str]
                             ) -> Tuple[List, List]:
        """
        This option is written based on our old logic of rule-sets resolving.
        It also checks whether the licensed rule-sets belongs to tenant's
        cloud. We don't need this, (according to business logic, each \
        license can have rule-sets that belong to only one cloud), but...

        This option makes more queries to DB, but it actually can be used )
        """
        it = self._ruleset_service.iter_by_lm_id(_license.ruleset_ids)
        # cloud filter just in case business logic changes. By default, a
        # licensed is not supposed to contain rule-sets from different clouds
        it = filter(
            lambda ruleset: ruleset.cloud == adjust_cloud(tenant.cloud), it
        )
        it = filter(
            lambda ruleset: ruleset.name in target_rulesets if
            target_rulesets else True, it
        )
        rule_sets = list(it)
        licensed_rulesets = [f'0:{rs.license_manager_id}' for rs in rule_sets]
        affected_licenses = [
            _license.customers.as_dict().get(tenant.customer_name, {}).get(
                TENANT_LICENSE_KEY_ATTR)
        ]
        return affected_licenses, licensed_rulesets

    def retrieve_rulesets_v2(self, _license: License, tenant: Tenant,
                             target_rulesets: Set[str]
                             ) -> Tuple[List, List]:
        """
        This option is faster, makes fewer requests to DB and a bit less safe.
        It expects that each ruleset in license belong to one cloud
        (which is correct, but who knows when it will be changed). Also,
        this method does not filter rule-sets by given names, expecting that
        all the licensed rule-sets must be used.
        It's kind of equivalent to the method above, but this is more
        straightforward, much more optimized and covers business
        requirements as of 20 March as well.
        It uses our old format to transfer licensed rule-sets to docker
        """
        licensed_rulesets = [f'0:{_id}' for _id in _license.ruleset_ids]
        affected_licenses = [
            _license.customers.as_dict().get(tenant.customer_name, {}).get(
                TENANT_LICENSE_KEY_ATTR)
        ]
        return affected_licenses, licensed_rulesets

    def ensure_job_is_allowed(self, tenant: Tenant, tlk: str):
        _LOG.info(f'Going to check for permission to exhaust'
                  f'{tlk} TenantLicense(s).')
        if not self._license_manager_service.is_allowed_to_license_a_job(
                customer=tenant.customer_name, tenant=tenant.name,
                tenant_license_keys=[tlk]):
            message = f'Tenant:\'{tenant.name}\' could not be granted ' \
                      f'to start a licensed job.'
            return build_response(
                content=message, code=RESPONSE_FORBIDDEN_CODE
            )
        _LOG.info(f'Tenant:\'{tenant.name}\' has been granted '
                  f'permission to submit a licensed job.')

    def _resolve_regions_to_scan(self, target_regions: Set[str],
                                 tenant: Tenant) -> Set[str]:
        if tenant.cloud == GOOGLE_CLOUD_ATTR:
            return {MULTIREGION, }  # cannot scan individual regions
        tenant_region = self._modular_service.get_tenant_regions(tenant)
        missing = target_regions - tenant_region
        if missing:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Regions: {", ".join(missing)} not active '
                        f'in tenant: {tenant.name}'
            )
        if not target_regions:  # all available must be scanned
            target_regions = tenant_region
        return target_regions

    def _validate_tenant_last_scan(self, tenant_name: str
                                   ) -> Optional[timedelta]:
        """
        Gets the latest job by this tenant and validates it against
        last_scan_threshold
        """
        threshold = self._environment_service.get_last_scan_threshold()
        if not threshold:
            return
        last_job = self._job_service.get_last_tenant_job(
            tenant_name, status=[JOB_SUCCEEDED_STATUS]
        )
        if not last_job:
            return
        allowed_after = utc_datetime(_from=last_job.submitted_at) + timedelta(
            seconds=threshold)
        now = utc_datetime()
        if allowed_after < now:
            return
        return allowed_after - now

    def query(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)
        limit = event.get(LIMIT_ATTR)

        old_lek = Lek.deserialize(event.get(NEXT_TOKEN_ATTR) or None)
        new_lek = Lek()
        _LOG.info('Job id was not given. Filtering jobs by other params')
        cursor = self._job_service.list(
            tenants, customer, limit=limit, lek=old_lek.value)
        entities = list(cursor)
        new_lek.value = cursor.last_evaluated_key
        _exclude = set()
        if customer:
            _exclude.add(CUSTOMER_DISPLAY_NAME_ATTR)
        return build_response(
            content=(
                self._job_service.get_job_dto(job, _exclude)
                for job in entities
            ),
            meta={NEXT_TOKEN_ATTR: new_lek.serialize()} if new_lek else None
        )

    def get(self, event) -> dict:
        job_id = event.get(JOB_ID_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)
        _LOG.info('Job id was given, querying using it')
        job = self._job_service.get_job(job_id)
        if not job or not self._job_service.is_allowed(job, customer, tenants):
            jobs = []  # todo 404 would be better, but historically it's an empty list
        else:
            jobs = [job, ]
        return build_response(
            content=(self._job_service.get_job_dto(j) for j in jobs)
        )

    def delete(self, event) -> dict:
        job_id = event.get(JOB_ID_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        tenants = event.get(TENANTS_ATTR)
        user = event.get(PARAM_USER_ID)

        job = self._job_service.get_job(job_id)
        if not job or not self._job_service.is_allowed(job, customer, tenants):
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Job with id \'{job_id}\' was not found'
            )

        if job.status == JOB_SUCCEEDED_STATUS or \
                job.status == JOB_FAILED_STATUS:
            message = f'Can not terminate job with status {job.status}'
            _LOG.warning(message)
            return build_response(content=message,
                                  code=RESPONSE_BAD_REQUEST_CODE)

        reason = f'Initiated by user \'{user}\' ' \
                 f'(customer \'{customer or SYSTEM_CUSTOMER}\')'
        self._job_service.set_job_failed_status(job, reason)
        self._job_service.save(job)

        _LOG.info(f"Going to terminate job with id '{job_id}'")
        self._batch_client.terminate_job(
            job_id=job_id,
            reason=reason
        )  # reason is just for AWS BatchClient here
        return build_response(
            content=f'The job with id \'{job_id}\' will be terminated'
        )

    def post_scheduled(self, event: dict) -> dict:
        _LOG.info(f'Post scheduled-job action: {event}')
        customer = event[CUSTOMER_ATTR]
        schedule = event[SCHEDULE_ATTR]
        target_rulesets: set = event.get(PARAM_TARGET_RULESETS)
        target_regions: set = event.get(PARAM_TARGET_REGIONS)
        name = event.get(NAME_ATTR)

        tenant = self._obtain_tenant(event.get(TENANT_ATTR), customer)
        if tenant.cloud not in self._environment_service.allowed_clouds_to_scan():
            _message = f'Scan for `{tenant.cloud}` is not allowed'
            _LOG.info(_message)
            return build_response(code=RESPONSE_FORBIDDEN_CODE,
                                  content=_message)
        # the same flow as for not scheduled jobs
        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=target_regions,
            tenant=tenant
        )
        application = self._modular_service.get_tenant_application(
            tenant, TENANT_PARENT_MAP_CUSTODIAN_LICENSES_TYPE
        )
        if not application:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Custodian application has not been '
                        f'activated for customer: {customer}'
            )
        meta = CustodianLicensesApplicationMeta(**application.meta.as_dict())
        license_key = meta.license_key(tenant.cloud)
        if not license_key:
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Customer {customer} has not been assigned an '
                        f'AWS License yet'
            )
        _license = self._license_service.get_license(license_key)
        if self._license_service.is_expired(_license):
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Affected license has expired'
            )
        # TODO put here custom-core version resolving/updating logic
        affected_licensed, licensed_rulesets = self.retrieve_rulesets_v1(
            _license, tenant, target_rulesets
        )
        if not licensed_rulesets:
            # actually is this block is executed, something really wrong
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='No rule-sets found in license'
            )
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            target_regions=list(regions_to_scan),
            affected_licenses=affected_licensed,
            licensed_rulesets=licensed_rulesets,
            job_type=BATCH_SCHEDULED_JOB_TYPE
        )

        job = self._scheduler_service.register_job(
            tenant, schedule, envs, name
        )
        return build_response(
            code=RESPONSE_CREATED,
            content=self._scheduler_service.dto(job)
        )

    def query_scheduled(self, event: dict) -> dict:
        customer = event.get(CUSTOMER_ATTR)
        tenants: list = event.get(TENANTS_ATTR)

        items = self._scheduler_service.list(
            customer=customer, tenants=set(tenants)
        )
        return build_response(content=(
            self._scheduler_service.dto(item) for item in items
        ))

    def get_scheduled(self, event: dict) -> dict:
        name = event[NAME_ATTR]
        tenants: list = event.get(TENANTS_ATTR)
        customer = event.get(CUSTOMER_ATTR)
        items = self._scheduler_service.list(
            name=name, customer=customer, tenants=set(tenants)
        )
        return build_response(content=(
            self._scheduler_service.dto(item) for item in items
        ))

    def delete_scheduled(self, event: dict) -> dict:
        _LOG.info(f'Delete scheduled-job action: {event}')
        name = event[NAME_ATTR]
        customer = event.get(CUSTOMER_ATTR)
        tenants: list = event.get(TENANTS_ATTR)

        item = self._scheduler_service.get(name, customer, set(tenants))
        if not item:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Scheduled job {name} not found'
            )
        self._scheduler_service.deregister_job(name)
        return build_response(code=RESPONSE_NO_CONTENT)

    def patch_scheduled(self, event: dict) -> dict:
        _LOG.info(f'Update scheduled-job action: {event}')
        name = event[NAME_ATTR]
        is_enabled = event.get(ENABLED)
        customer = event.get(CUSTOMER_ATTR)
        schedule = event.get(SCHEDULE_ATTR)
        tenants: list = event.get(TENANTS_ATTR)

        item = self._scheduler_service.get(name, customer, set(tenants))
        if not item:
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Scheduled job {name} not found'
            )

        self._scheduler_service.update_job(
            item, is_enabled, schedule=schedule
        )
        return build_response(content=self._scheduler_service.dto(item))

    def _validate_cloud_identifier(self, cloud_identifier: int,
                                   credentials: dict, cloud: str):
        identifier_validators_mapping = {
            AWS_CLOUD_ATTR: self._validate_aws_account_id,
            AZURE_CLOUD_ATTR: self._validate_azure_subscription_id,
            GOOGLE_CLOUD_ATTR: self._validate_gcp_project_id
        }
        validator = identifier_validators_mapping.get(cloud)
        if not validator:
            return
        if not validator(credentials, cloud_identifier):
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content='Target account identifier didn\'t match with'
                        ' one provided in the credentials. Check your '
                        'credentials and try again.'
            )

    def _validate_aws_account_id(self, credentials: dict,
                                 target_account_id: int):
        credentials_lower = {
            k.lower(): v for k, v in credentials.items() if
            k != 'AWS_DEFAULT_REGION'
        }
        try:
            account_id = self._sts_client.get_caller_identity(
                credentials=credentials_lower)['Account']
            return account_id == target_account_id
        except ClientError:
            message = 'Invalid AWS credentials provided.'
            _LOG.warning(message)
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=message
            )

    def _validate_azure_subscription_id(self, credentials: dict,
                                        target_subscription_id: int):
        # try:
        #     self.azure_subscriptions_service.validate_credentials(
        #         tenant_id=credentials.get(AZURE_TENANT_ID),
        #         client_id=credentials.get(AZURE_CLIENT_ID),
        #         client_secret=credentials.get(AZURE_CLIENT_SECRET),
        #         subscription_id=credentials.get(AZURE_SUBSCRIPTION_ID)
        #     )
        # except CustodianException as e:
        #     return build_response(
        #         code=e.code,
        #         content=e.content
        #     )
        # subscription_id = credentials.get(AZURE_SUBSCRIPTION_ID)
        # return subscription_id == target_subscription_id
        # todo commented due to heavy dependency
        #  to azure packages (lambda size is 30+mb)
        return True

    @staticmethod
    def _validate_gcp_project_id(credentials: dict, target_project_id: int):
        return credentials.get('project_id') == target_project_id
