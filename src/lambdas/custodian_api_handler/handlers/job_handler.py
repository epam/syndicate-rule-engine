from datetime import timedelta
from functools import cached_property
from http import HTTPStatus
from itertools import chain
from typing import Iterable

from botocore.exceptions import ClientError
from modular_sdk.models.pynamodb_extension.base_model import \
    LastEvaluatedKey as Lek
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from helpers import adjust_cloud
from helpers.constants import (
    BatchJobType,
    Cloud,
    CustodianEndpoint,
    GLOBAL_REGION,
    HTTPMethod,
    JobState,
    RuleDomain,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.system_customer import SYSTEM_CUSTOMER
from handlers import AbstractHandler, Mapping
from models.job import Job
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER
from services import modular_helpers
from services.abs_lambda import ProcessedEvent
from services.assemble_service import AssembleService
from services.clients.batch import BatchClient
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from services.rbac_service import TenantsAccessPayload
from services.job_lock import TenantSettingJobLock
from services.job_service import JobService
from services.license_manager_service import LicenseManagerService
from services.license_service import License, LicenseService
from services.platform_service import PlatformService
from services.rule_meta_service import RuleNamesResolver, RuleService
from services.ruleset_service import RulesetService
from services.scheduler_service import SchedulerService
from services.ssm_service import SSMService
from validators.swagger_request_models import (
    BaseModel,
    JobGetModel,
    JobPostModel,
    K8sJobPostModel,
    ScheduledJobGetModel,
    ScheduledJobPatchModel,
    ScheduledJobPostModel,
    StandardJobPostModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class JobHandler(AbstractHandler):
    def __init__(self, tenant_service: TenantService,
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
                 rule_service: RuleService,
                 platform_service: PlatformService):
        self._tenant_service = tenant_service
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
        self._platform_service = platform_service

    @classmethod
    def build(cls) -> 'JobHandler':
        return cls(
            tenant_service=SERVICE_PROVIDER.modular_client.tenant_service(),
            environment_service=SERVICE_PROVIDER.environment_service,
            job_service=SERVICE_PROVIDER.job_service,
            license_service=SERVICE_PROVIDER.license_service,
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            assemble_service=SERVICE_PROVIDER.assemble_service,
            batch_client=SERVICE_PROVIDER.batch,
            sts_client=SERVICE_PROVIDER.sts,
            ssm_service=SERVICE_PROVIDER.ssm_service,
            scheduler_service=SERVICE_PROVIDER.scheduler_service,
            rule_service=SERVICE_PROVIDER.rule_service,
            platform_service=SERVICE_PROVIDER.platform_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        """
        These are licensed jobs endpoints. They use only licensed rule-sets.
        And this is the main business case
        :return:
        """
        return {
            CustodianEndpoint.JOBS: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.query,
            },
            CustodianEndpoint.JOBS_STANDARD: {
                HTTPMethod.POST: self.post_standard,
            },
            CustodianEndpoint.JOBS_K8S: {
                HTTPMethod.POST: self.post_k8s
            },
            CustodianEndpoint.JOBS_JOB: {
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete
            },
            CustodianEndpoint.SCHEDULED_JOB: {
                HTTPMethod.POST: self.post_scheduled,
                HTTPMethod.GET: self.query_scheduled,
            },
            CustodianEndpoint.SCHEDULED_JOB_NAME: {
                HTTPMethod.GET: self.get_scheduled,
                HTTPMethod.DELETE: self.delete_scheduled,
                HTTPMethod.PATCH: self.patch_scheduled
            }
        }

    def _obtain_tenant(self, tenant_name: str, tap: TenantsAccessPayload,
                       customer: str | None = None) -> Tenant:
        tenant = self._tenant_service.get(tenant_name)
        if tenant and not tap.is_allowed_for(tenant.name):
            tenant = None
        modular_helpers.assert_tenant_valid(tenant, customer)
        return tenant

    @validate_kwargs
    def post_standard(self, event: StandardJobPostModel,
                      _tap: TenantsAccessPayload):
        """
        Post job for the given tenant. Only not-licensed rule-sets
        """

        tenant = self._obtain_tenant(event.tenant_name, _tap, event.customer)

        credentials_key = None
        if event.credentials:
            credentials = event.credentials.dict()
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

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=event.target_regions,
            tenant=tenant
        )

        if not self._environment_service.allow_simultaneous_jobs_for_one_tenant():
            lock = TenantSettingJobLock(event.tenant_name)
            if job_id := lock.locked_for(regions_to_scan):
                return build_response(
                    code=HTTPStatus.FORBIDDEN,
                    content=f'Some requested regions are already being '
                            f'scanned in another tenant`s job {job_id}'
                )

        ids = list(
            (item.id, item.name, item.version) for item in
            self.retrieve_standard_rulesets(tenant, event.target_rulesets)
        )
        if not ids:
            return build_response(code=HTTPStatus.NOT_FOUND,
                                  content='No standard rule-sets found')

        ttl_days = self._environment_service.jobs_time_to_live_days()
        ttl = None
        if ttl_days:
            ttl = timedelta(days=ttl_days)
        job = self._job_service.create(
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            regions=list(regions_to_scan),
            rulesets=[f'{i[1]}:{i[2]}' for i in ids],
            ttl=ttl
        )
        self._job_service.save(job)
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            job_id=job.id,
            target_regions=list(regions_to_scan),
            target_rulesets=ids,
            credentials_key=credentials_key,
        )
        bid = self._submit_job_to_batch(tenant=tenant, job=job, envs=envs)
        self._job_service.update(job, bid)
        TenantSettingJobLock(tenant.name).acquire(job.id, regions_to_scan)
        return build_response(
            code=HTTPStatus.CREATED,
            content=self._job_service.dto(job)
        )

    def retrieve_standard_rulesets(self, tenant: Tenant, names: set[str]
                                   ) -> Iterable[Ruleset]:
        cloud = adjust_cloud(tenant.cloud)
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

    def resolve_rulesets(self, licenses: Iterable[License], tenant: Tenant,
                         domain: RuleDomain, names: set[str]
                         ) -> dict[str, list[Ruleset]]:
        """
        Resolves licensed rulesets that will be used for that scan.
        Logic here is somewhat tangled, but it should work as expected
        :param licenses:
        :param tenant:
        :param domain:
        :param names:
        :return: (affected_licenses, licensed_rulesets, rulesets):
        - affected_license: [tenant_license_key1, tenant_license_key2]
        - licensed_rulesets: ['0:Full AWS 1', '0:Full AWS 1']
        """
        # TODO tests
        mapping = {}  # ruleset id to list of tenant license keys
        for lic in licenses:
            tlk = lic.customers.get(tenant.customer_name,
                                    {}).get('tenant_license_key')
            if not tlk:
                continue
            for _id in lic.ruleset_ids:
                mapping.setdefault(_id, []).append(tlk)

        # in case one ruleset is given by multiple tenant license keys,
        # we just use the first one, because it is excessive configuration,
        # this shouldn't happen
        reversed_mapping = {}  # tenant_license_key to rulesets
        for _id, tenant_license_keys in mapping.items():
            ruleset = self._ruleset_service.by_lm_id(_id)
            if not ruleset or ruleset.cloud != domain.value:
                continue
            if names and ruleset.name not in names:
                continue
            # ruleset is acceptable
            reversed_mapping.setdefault(tenant_license_keys[0],
                                        []).append(ruleset)
        return reversed_mapping

    @validate_kwargs
    def post(self, event: JobPostModel, _tap: TenantsAccessPayload):
        """
        Post job for the given tenant. Only licensed rule-sets
        """
        tenant = self._obtain_tenant(event.tenant_name, _tap, event.customer)
        domain = RuleDomain.from_tenant_cloud(tenant.cloud)
        if not domain:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Cannot start job for tenant with cloud {tenant.cloud}'
            ).exc()

        credentials_key = None
        if event.credentials:
            credentials = event.credentials.dict()
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

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=event.target_regions,
            tenant=tenant
        )

        if not self._environment_service.allow_simultaneous_jobs_for_one_tenant():
            lock = TenantSettingJobLock(event.tenant_name)
            if job_id := lock.locked_for(regions_to_scan):
                return build_response(
                    code=HTTPStatus.FORBIDDEN,
                    content=f'Some requested regions are already being '
                            f'scanned in another tenant`s job {job_id}'
                )
        licenses = [
            l[1] for l in self._license_service.iter_tenant_licenses(tenant)
        ]
        if not licenses:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'There is no linked licenses for this tenant'
            ).exc()
        if all([lic.is_expired() for lic in licenses]):
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'All activated licenses have expired'
            ).exc()
        mapping = self.resolve_rulesets(
            licenses=licenses,
            tenant=tenant,
            domain=domain,
            names=event.target_rulesets
        )
        _LOG.debug(f'Resolved rulesets mapping: {mapping}')
        if not mapping:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'No appropriate licensed rulesets found for the requests scan'
            ).exc()
        # currently allow only jobs that exhaust one tenant license
        tenant_license_key, rulesets = next(iter(mapping.items()))
        self.ensure_job_is_allowed(tenant, tenant_license_key)
        affected_licenses = [tenant_license_key]
        licensed_rulesets = [f'0:{r.license_manager_id}' for r in rulesets]

        rules_to_scan = event.rules_to_scan
        if rules_to_scan:
            _LOG.info('Rules to scan were provided. Resolving them')
            available = set(chain.from_iterable(r.rules for r in rulesets))
            resolver = RuleNamesResolver(
                resolve_from=list(available),
                allow_multiple=True
            )
            resolved, not_resolved = [], []
            for rule, is_resolved in resolver.resolve_multiple_names(
                    event.rules_to_scan):
                if is_resolved:
                    resolved.append(rule)
                else:
                    not_resolved.append(rule)
            if not_resolved:
                return build_response(
                    code=HTTPStatus.BAD_REQUEST,
                    content=f'These rules are not allowed by your '
                            f'{tenant.cloud} '
                            f'license: {", ".join(not_resolved)}'
                )
            rules_to_scan = resolved

        ttl_days = self._environment_service.jobs_time_to_live_days()
        ttl = None
        if ttl_days:
            ttl = timedelta(days=ttl_days)
        job = self._job_service.create(
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            regions=list(regions_to_scan),
            rulesets=[r.license_manager_id for r in rulesets],
            rules_to_scan=list(rules_to_scan or []),
            ttl=ttl
        )
        self._job_service.save(job)
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            job_id=job.id,
            target_regions=list(regions_to_scan),
            affected_licenses=affected_licenses,
            licensed_rulesets=licensed_rulesets,
            credentials_key=credentials_key
        )
        bid = self._submit_job_to_batch(tenant=tenant, job=job, envs=envs)
        self._job_service.update(job, bid)
        TenantSettingJobLock(tenant.name).acquire(job.id, regions_to_scan)
        return build_response(
            code=HTTPStatus.CREATED,
            content=self._job_service.dto(job)
        )

    def _submit_job_to_batch(self, tenant: Tenant, job: Job, envs: dict
                             ) -> str:
        job_name = f'{tenant.name}-{job.submitted_at}'
        job_name = ''.join(
            ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in job_name
        )
        _LOG.debug(f'Going to submit AWS Batch job with name {job_name}')
        response = self._batch_client.submit_job(
            job_name=job_name,
            job_queue=self._environment_service.get_batch_job_queue(),
            job_definition=self._environment_service.get_batch_job_def(),
            environment_variables=envs,
        )
        _LOG.debug(f'Batch job was submitted: {response}')
        return response['jobId']

    def ensure_job_is_allowed(self, tenant: Tenant, tlk: str):
        _LOG.info(f'Going to check for permission to exhaust'
                  f'{tlk} TenantLicense(s).')
        if not self._license_manager_service.is_allowed_to_license_a_job(
                customer=tenant.customer_name, tenant=tenant.name,
                tenant_license_keys=[tlk]):
            message = f'Tenant:\'{tenant.name}\' could not be granted ' \
                      f'to start a licensed job with tenant license {tlk}'
            return build_response(
                content=message, code=HTTPStatus.FORBIDDEN
            )
        _LOG.info(f'Tenant:\'{tenant.name}\' has been granted '
                  f'permission to submit a licensed job.')

    def _resolve_regions_to_scan(self, target_regions: set[str],
                                 tenant: Tenant) -> set[str]:
        cloud = modular_helpers.tenant_cloud(tenant)
        if cloud == Cloud.AZURE or cloud == Cloud.GOOGLE:
            return {GLOBAL_REGION, }  # cannot scan individual regions
        tenant_region = modular_helpers.get_tenant_regions(tenant)
        missing = target_regions - tenant_region
        if missing:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Regions: {", ".join(missing)} not active '
                f'in tenant: {tenant.name}'
            ).exc()
        if not target_regions:  # all available must be scanned
            target_regions = tenant_region
        return target_regions

    @validate_kwargs
    def query(self, event: JobGetModel):
        old_lek = Lek.deserialize(event.next_token)
        new_lek = Lek()

        if event.tenant_name:
            cursor = self._job_service.get_by_tenant_name(
                tenant_name=event.tenant_name,
                status=event.status,
                limit=event.limit,
                start=event.start_iso,
                end=event.end_iso,
                last_evaluated_key=old_lek.value,
            )
        else:
            cursor = self._job_service.get_by_customer_name(
                customer_name=event.customer,
                status=event.status,
                limit=event.limit,
                start=event.start_iso,
                end=event.end_iso,
                last_evaluated_key=old_lek.value
            )
        jobs = list(cursor)
        new_lek.value = cursor.last_evaluated_key
        return ResponseFactory().items(
            it=map(self._job_service.dto, jobs),
            next_token=new_lek.serialize() if new_lek else None
        ).build()

    @validate_kwargs
    def get(self, event: BaseModel, job_id: str):
        _LOG.info('Job id was given, querying using it')
        job = self._job_service.get_nullable(job_id)
        if not job or event.customer and job.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                self._job_service.not_found_message(job_id)
            ).exc()
        return build_response(content=self._job_service.dto(job))

    @validate_kwargs
    def delete(self, event: BaseModel, job_id: str, _pe: ProcessedEvent):
        job = self._job_service.get_nullable(job_id)
        if not job or event.customer and job.customer_name != event.customer:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=self._job_service.not_found_message(job_id)
            )
        if job.status in (JobState.SUCCEEDED, JobState.FAILED):
            message = f'Can not terminate job with status {job.status}'
            _LOG.warning(message)
            return build_response(content=message,
                                  code=HTTPStatus.BAD_REQUEST)

        user_id = _pe['cognito_username']
        reason = f'Initiated by user \'{user_id}\' ' \
                 f'(customer \'{event.customer or SYSTEM_CUSTOMER}\')'

        self._job_service.update(
            job=job,
            reason=reason,
            status=JobState.FAILED
        )
        TenantSettingJobLock(job.tenant_name).release(job.id)

        _LOG.info(f"Going to terminate job with id '{job.id}'")
        self._batch_client.terminate_job(
            job_id=job.batch_job_id,
            reason=reason
        )  # reason is just for AWS BatchClient here
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f'The job with id \'{job.id}\' will is terminated'
        )

    @validate_kwargs
    def post_k8s(self, event: K8sJobPostModel, _tap: TenantsAccessPayload):
        platform = self._platform_service.get_nullable(
            hash_key=event.platform_id)
        if not platform or (
                event.customer and platform.customer != event.customer):
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Active platform: {event.platform_id} not found'
            )
        tenant = self._obtain_tenant(platform.tenant_name, _tap,
                                     event.customer)
        if not self._environment_service.allow_simultaneous_jobs_for_one_tenant():
            lock = TenantSettingJobLock(tenant.name)
            if job_id := lock.locked_for({platform.platform_id}):
                return build_response(
                    code=HTTPStatus.FORBIDDEN,
                    content=f'Job {job_id} is already running '
                            f'for tenant {tenant.name}'
                )

        licenses = [
            l[1] for l in self._license_service.iter_tenant_licenses(tenant)
        ]
        if not licenses:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'There is no linked licenses for this tenant'
            ).exc()
        if all([lic.is_expired() for lic in licenses]):
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'All activated licenses have expired'
            ).exc()
        mapping = self.resolve_rulesets(
            licenses=licenses,
            tenant=tenant,
            domain=RuleDomain.KUBERNETES,
            names=event.target_rulesets
        )
        _LOG.debug(f'Resolved rulesets mapping: {mapping}')
        if not mapping:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'No appropriate licensed rulesets found for the requests scan'
            ).exc()
        # currently allow only jobs that exhaust one tenant license
        tenant_license_key, rulesets = next(iter(mapping.items()))
        self.ensure_job_is_allowed(tenant, tenant_license_key)
        affected_licenses = [tenant_license_key]
        licensed_rulesets = [f'0:{r.license_manager_id}' for r in rulesets]

        credentials_key = None  # TODO K8S validate whether long-lived token exists, validate whether it belongs to a cluster?
        if event.token:
            _LOG.debug('Temp token was provided. Saving to ssm')
            credentials_key = self._ssm_service.save_data(
                name=tenant.name, value=event.token
            )
        ttl_days = self._environment_service.jobs_time_to_live_days()
        ttl = None
        if ttl_days:
            ttl = timedelta(days=ttl_days)
        job = self._job_service.create(
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            regions=[],
            rulesets=[r.license_manager_id for r in rulesets],
            ttl=ttl,
            platform_id=platform.id
        )
        self._job_service.save(job)
        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            job_id=job.id,
            platform_id=platform.id,
            affected_licenses=affected_licenses,
            licensed_rulesets=licensed_rulesets,
            credentials_key=credentials_key
        )
        bid = self._submit_job_to_batch(tenant=tenant, job=job, envs=envs)
        self._job_service.update(job, bid)
        TenantSettingJobLock(tenant.name).acquire(job.id,
                                                  {platform.platform_id})
        return build_response(
            code=HTTPStatus.CREATED,
            content=self._job_service.dto(job)
        )

    @validate_kwargs
    def post_scheduled(self, event: ScheduledJobPostModel,
                       _tap: TenantsAccessPayload):
        tenant = self._obtain_tenant(event.tenant_name, _tap, event.customer)
        domain = RuleDomain.from_tenant_cloud(tenant.cloud)
        if not domain:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Cannot start job for tenant with cloud {tenant.cloud}'
            ).exc()
        # the same flow as for not scheduled jobs
        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=event.target_regions,
            tenant=tenant
        )
        licenses = [
            l[1] for l in self._license_service.iter_tenant_licenses(tenant)
        ]
        if not licenses:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'There is no linked licenses for this tenant'
            ).exc()
        if all([lic.is_expired() for lic in licenses]):
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'All activated licenses have expired'
            ).exc()
        mapping = self.resolve_rulesets(
            licenses=licenses,
            tenant=tenant,
            domain=domain,
            names=event.target_rulesets
        )
        if not mapping:
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'No appropriate licensed rulesets found for the requests scan'
            ).exc()
        # currently allow only jobs that exhaust one tenant license
        tenant_license_key, rulesets = next(iter(mapping.items()))
        self.ensure_job_is_allowed(tenant, tenant_license_key)
        affected_licenses = [tenant_license_key]
        licensed_rulesets = [f'0:{r.license_manager_id}' for r in rulesets]

        envs = self._assemble_service.build_job_envs(
            tenant=tenant,
            target_regions=list(regions_to_scan),
            affected_licenses=affected_licenses,
            licensed_rulesets=licensed_rulesets,
            job_type=BatchJobType.SCHEDULED
        )
        # MOVE TODO fix terminate for scheduled

        job = self._scheduler_service.register_job(
            tenant, event.schedule, envs, event.name
        )
        return build_response(
            code=HTTPStatus.CREATED,
            content=self._scheduler_service.dto(job)
        )

    @validate_kwargs
    def query_scheduled(self, event: ScheduledJobGetModel):
        tenants = set()
        if event.tenant_name:
            tenants.add(event.tenant_name)
        items = self._scheduler_service.list(
            customer=event.customer,
            tenants=tenants
        )
        return build_response(content=(
            self._scheduler_service.dto(item) for item in items
        ))

    @validate_kwargs
    def get_scheduled(self, event: BaseModel, name: str):
        item = next(self._scheduler_service.list(
            name=name, customer=event.customer
        ), None)
        if not item:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        return build_response(content=self._scheduler_service.dto(item))

    @validate_kwargs
    def delete_scheduled(self, event: BaseModel, name: str):

        item = self._scheduler_service.get(name, event.customer)
        if not item:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Scheduled job {name} not found'
            )
        self._scheduler_service.deregister_job(name)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def patch_scheduled(self, event: ScheduledJobPatchModel, name: str):
        is_enabled = event.enabled
        customer = event.customer
        schedule = event.schedule

        item = self._scheduler_service.get(name, customer)
        if not item:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Scheduled job {name} not found'
            )

        self._scheduler_service.update_job(
            item, is_enabled, schedule=schedule
        )
        return build_response(content=self._scheduler_service.dto(item))

    def _validate_cloud_identifier(self, cloud_identifier: int,
                                   credentials: dict, cloud: Cloud):
        identifier_validators_mapping = {
            Cloud.AWS: self._validate_aws_account_id,
            Cloud.AZURE: None,
            Cloud.GOOGLE: self._validate_gcp_project_id
        }
        validator = identifier_validators_mapping.get(cloud)
        if not validator:
            return
        if not validator(credentials, cloud_identifier):
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
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
                code=HTTPStatus.BAD_REQUEST,
                content=message
            )

    @staticmethod
    def _validate_gcp_project_id(credentials: dict, target_project_id: int):
        return credentials.get('project_id') == target_project_id
