from datetime import timedelta
from http import HTTPStatus
from itertools import chain
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import (
    GLOBAL_REGION,
    Cloud,
    Endpoint,
    HTTPMethod,
    JobState,
    RuleDomain,
    ScheduledJobType,
)
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.mixins import SubmitJobToBatchMixin
from helpers.system_customer import SystemCustomer
from models.ruleset import Ruleset
from services import SERVICE_PROVIDER, cache, modular_helpers
from services.abs_lambda import ProcessedEvent
from services.clients.batch import (
    BatchClient,
    CeleryJobClient,
)
from services.clients.ssm import AbstractSSMClient
from services.clients.sts import StsClient
from services.environment_service import EnvironmentService
from services.job_lock import TenantSettingJobLock
from services.job_service import JobService
from services.license_manager_service import LicenseManagerService
from services.license_service import License, LicenseService
from services.platform_service import PlatformService
from services.rbac_service import TenantsAccessPayload
from services.rule_meta_service import RuleNamesResolver, RuleService
from services.ruleset_service import RulesetName, RulesetService
from services.scheduled_job_service import ScheduledJobService
from validators.swagger_request_models import (
    BaseModel,
    JobGetModel,
    JobPostModel,
    K8sJobPostModel,
    ScheduledJobGetModel,
    ScheduledJobPatchModel,
    ScheduledJobPostModel,
)
from validators.utils import validate_kwargs


if TYPE_CHECKING:
    from modular_sdk.services.tenant_settings_service import (
        TenantSettingsService,
    )

_LOG = get_logger(__name__)


class JobHandler(AbstractHandler, SubmitJobToBatchMixin):
    def __init__(
        self,
        tenant_service: TenantService,
        environment_service: EnvironmentService,
        job_service: JobService,
        license_service: LicenseService,
        license_manager_service: LicenseManagerService,
        ruleset_service: RulesetService,
        batch_client: BatchClient | CeleryJobClient,
        sts_client: StsClient,
        ssm: AbstractSSMClient,
        scheduled_job_service: ScheduledJobService,
        rule_service: RuleService,
        platform_service: PlatformService,
        tenant_settings_service: 'TenantSettingsService',
    ):
        self._tenant_service = tenant_service
        self._environment_service = environment_service
        self._job_service = job_service
        self._license_service = license_service
        self._license_manager_service = license_manager_service
        self._ruleset_service = ruleset_service
        self._batch_client = batch_client
        self._sts_client = sts_client
        self._ssm = ssm
        self._scheduled_job_service = scheduled_job_service
        self._rule_service = rule_service
        self._platform_service = platform_service
        self._tss = tenant_settings_service

        self._tenant_licenses = cache.factory()

    @classmethod
    def build(cls) -> Self:
        return cls(
            tenant_service=SERVICE_PROVIDER.modular_client.tenant_service(),
            environment_service=SERVICE_PROVIDER.environment_service,
            job_service=SERVICE_PROVIDER.job_service,
            license_service=SERVICE_PROVIDER.license_service,
            license_manager_service=SERVICE_PROVIDER.license_manager_service,
            ruleset_service=SERVICE_PROVIDER.ruleset_service,
            batch_client=SERVICE_PROVIDER.batch,
            sts_client=SERVICE_PROVIDER.sts,
            ssm=SERVICE_PROVIDER.ssm,
            scheduled_job_service=SERVICE_PROVIDER.scheduled_job_service,
            rule_service=SERVICE_PROVIDER.rule_service,
            platform_service=SERVICE_PROVIDER.platform_service,
            tenant_settings_service=\
                SERVICE_PROVIDER.modular_client.tenant_settings_service(),
        )

    @property
    def mapping(self) -> Mapping:
        """
        These are licensed jobs endpoints. They use only licensed rule-sets.
        And this is the main business case
        :return:
        """
        return {
            Endpoint.JOBS: {
                HTTPMethod.POST: self.post,
                HTTPMethod.GET: self.query,
            },
            Endpoint.JOBS_K8S: {HTTPMethod.POST: self.post_k8s},
            Endpoint.JOBS_JOB: {
                HTTPMethod.GET: self.get,
                HTTPMethod.DELETE: self.delete,
            },
            Endpoint.SCHEDULED_JOB: {
                HTTPMethod.POST: self.post_scheduled,
                HTTPMethod.GET: self.query_scheduled,
            },
            Endpoint.SCHEDULED_JOB_NAME: {
                HTTPMethod.GET: self.get_scheduled,
                HTTPMethod.DELETE: self.delete_scheduled,
                HTTPMethod.PATCH: self.patch_scheduled,
            },
        }

    def _obtain_tenant(
        self,
        tenant_name: str,
        tap: TenantsAccessPayload,
        customer: str | None = None,
    ) -> Tenant:
        tenant = self._tenant_service.get(tenant_name)
        modular_helpers.assert_tenant_valid(tenant, customer)
        if not tap.is_allowed_for(tenant.name):
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(f"The request tenant '{tenant_name}' is not found")
                .exc()
            )
        return tenant

    def _get_tenant_licenses(self, tenant: Tenant) -> tuple[License, ...]:
        if tenant.name in self._tenant_licenses:
            _LOG.debug('Returning cached tenant licenses')
            return self._tenant_licenses[tenant.name]
        _LOG.debug('Querying tenant licenses')
        licenses = tuple(
            lic[1]
            for lic in self._license_service.iter_tenant_licenses(tenant)
        )
        self._tenant_licenses[tenant.name] = licenses
        return licenses

    def _resolve_all_from_licenses(
        self, tenant: Tenant, domain: RuleDomain, licenses: tuple[License, ...]
    ) -> tuple[License, list[RulesetName]]:
        mapping = {}  # license to acceptable rulesets
        for lic in licenses:
            _tlk = lic.tenant_license_key(tenant.customer_name)
            if not _tlk:
                continue
            for name in set(lic.ruleset_ids):
                item = self._ruleset_service.get_licensed(name)
                if not item or item.cloud != domain.value:
                    continue
                ruleset_name = RulesetName(name, None, lic.license_key)
                ruleset_name.rules = item.rules
                mapping.setdefault(lic, set()).add(ruleset_name)
        if not mapping:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'No appropriate rulesets can be resolved from license(s)'
                )
                .exc()
            )
        if len(mapping) > 1:
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message(
                    f'Ambiguous situation. Multiple licenses: '
                    f'{", ".join(l.license_key for l in mapping)} - '
                    f'can be used for this job but only one license per job '
                    f'is currently allowed. Specify the desired license key'
                )
                .exc()
            )
        lic, rulesets = next(iter(mapping.items()))
        return lic, list(rulesets)

    def _resolve_local(
        self,
        tenant: Tenant,
        domain: RuleDomain,
        ruleset_names: set[RulesetName],
    ) -> list[RulesetName]:
        local = []
        for name in ruleset_names:
            if name.version:
                item = self._ruleset_service.get_standard(
                    customer=tenant.customer_name,
                    name=name.name,
                    version=name.version.to_str(),
                )
            else:
                item = self._ruleset_service.get_latest(
                    customer=tenant.customer_name, name=name.name
                )
            if not item:
                if name.version:
                    raise (
                        ResponseFactory(HTTPStatus.NOT_FOUND)
                        .message(
                            f'Licensed or local ruleset {name.name} '
                            f'{name.version} not found'
                        )
                        .exc()
                    )
                else:
                    raise (
                        ResponseFactory(HTTPStatus.NOT_FOUND)
                        .message(
                            f'No versions of licensed or local '
                            f'ruleset {name.name} found'
                        )
                        .exc()
                    )
            if item.cloud != domain.value:
                raise (
                    ResponseFactory(HTTPStatus.BAD_REQUEST)
                    .message(
                        f'Local ruleset {item.name} is supposed to be used with '
                        f'{item.cloud}'
                    )
                    .exc()
                )
            ruleset_name = RulesetName(item.name, item.version or None, None)
            ruleset_name.rules = item.rules
            local.append(ruleset_name)
        return local

    def _match_licensed(
        self, lic: License, domain: RuleDomain, ruleset_name: RulesetName
    ) -> Ruleset | None:
        if ruleset_name.name not in lic.ruleset_ids:
            return
        item = self._ruleset_service.get_licensed(ruleset_name.name)
        if not item:
            return
        if item.cloud != domain.value:
            return
        if (
            ruleset_name.version
            and ruleset_name.version.to_str() not in item.versions
        ):
            return
        return item

    def _resolve_from_names_and_licenses(
        self,
        tenant: Tenant,
        domain: RuleDomain,
        ruleset_names: set[RulesetName],
        licenses: tuple[License, ...],
    ) -> tuple[list[RulesetName], License | None, list[RulesetName]]:
        utilized = set()
        mapping = {}
        for lic in licenses:
            _tlk = lic.tenant_license_key(tenant.customer_name)
            if not _tlk:
                continue
            for name in ruleset_names:
                item = self._match_licensed(lic, domain, name)
                if not item:
                    continue
                ruleset_name = RulesetName(
                    name.name, name.version, lic.license_key
                )
                ruleset_name.rules = item.rules
                mapping.setdefault(lic, set()).add(ruleset_name)
                utilized.add(name)
        if len(mapping) > 1:
            # either the save ruleset name can be used from different licenses
            # or different rulesets can be used from different licenses
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message(
                    f'Ambiguous situation. Multiple licenses: '
                    f'{", ".join(l.license_key for l in mapping)} - '
                    f'can be used for this job but only one license per job '
                    f'is currently allowed. Specify the desired license key'
                )
                .exc()
            )
        if len(mapping) == 1:
            lic, licensed = next(iter(mapping.items()))
        else:  # len(mapping) == 0:
            lic, licensed = None, set()
        local = self._resolve_local(
            tenant=tenant,
            domain=domain,
            ruleset_names=ruleset_names - utilized,
        )
        return local, lic, licensed

    def _resolve_rulesets(
        self,
        tenant: Tenant,
        domain: RuleDomain,
        ruleset_names: set[RulesetName],
        licenses: tuple[License, ...],
    ) -> tuple[list[RulesetName], License | None, list[RulesetName]]:
        if not ruleset_names and not licenses:
            # or use all local rulesets
            _LOG.warning(
                'No rulesets were provided and no licenses are activated'
            )
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'No licenses are activated for tenant {tenant.name} and '
                    f'not ruleset names provided. '
                    f'Specify ruleset names to use local rulesets or activate '
                    f'a license'
                )
                .exc()
            )
        if not ruleset_names:  # but licenses
            _LOG.info(
                'No rulesets were provided but some licenses are '
                'activated. Resolving all rulesets from license'
            )
            standard_rulesets = []
            lic, licensed_rulesets = self._resolve_all_from_licenses(
                tenant=tenant, domain=domain, licenses=licenses
            )
        elif not licenses:  # but ruleset names
            _LOG.info(
                'No licenses are activated but some rulesets were '
                'provided. Resolving local rulesets'
            )
            standard_rulesets = self._resolve_local(
                tenant=tenant, domain=domain, ruleset_names=ruleset_names
            )
            lic, licensed_rulesets = None, []
        else:  # both ruleset names and licenses
            _LOG.info(
                'Some licenses are activated and rulesets were provided.'
            )
            standard_rulesets, lic, licensed_rulesets = (
                self._resolve_from_names_and_licenses(
                    tenant=tenant,
                    domain=domain,
                    ruleset_names=ruleset_names,
                    licenses=licenses,
                )
            )
        return standard_rulesets, lic, licensed_rulesets

    def _get_rulesets_for_scan(
        self,
        tenant: Tenant,
        domain: RuleDomain,
        license_key: str | None,
        ruleset_names: set[RulesetName],
    ) -> tuple[list[RulesetName], License | None, list[RulesetName]]:
        if license_key:
            lic = self._license_service.get_nullable(license_key)
            if not lic:
                raise (
                    ResponseFactory(HTTPStatus.BAD_REQUEST)
                    .message(f'License {license_key} not found')
                    .exc()
                )
            if not self._license_service.is_subject_applicable(
                lic=lic, customer=tenant.customer_name, tenant_name=tenant.name
            ):
                raise (
                    ResponseFactory(HTTPStatus.FORBIDDEN)
                    .message(
                        f'License {license_key} is not applicable for '
                        f'tenant {tenant.name}'
                    )
                    .exc()
                )
            licenses = (lic,)
        else:
            licenses = self._get_tenant_licenses(tenant)

        if licenses and all(l.is_expired() for l in licenses):
            raise (
                ResponseFactory(HTTPStatus.FORBIDDEN)
                .message('All licenses have expired')
                .exc()
            )

        standard_rulesets, lic, licensed_rulesets = self._resolve_rulesets(
            tenant=tenant,
            domain=domain,
            ruleset_names=ruleset_names,
            licenses=licenses,
        )

        if not standard_rulesets and not licensed_rulesets:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message('No licensed and standard rulesets are found')
                .exc()
            )

        if lic:
            _LOG.debug('Making request to LM to ensure the job is allowed')
            self.ensure_job_is_allowed(
                tenant, lic.tenant_license_key(tenant.customer_name)
            )
        return standard_rulesets, lic, licensed_rulesets

    @staticmethod
    def _serialize_rulesets(
        standard: list[RulesetName],
        lic: License | None,
        licensed: list[RulesetName],
    ) -> list[str]:
        rulesets = [r.to_str() for r in standard]
        if lic:
            rulesets += [
                RulesetName(
                    r.name, r.version or None, lic.license_key
                ).to_str()
                for r in licensed
            ]
        return rulesets

    @validate_kwargs
    def post(self, event: JobPostModel, _tap: TenantsAccessPayload):
        """
        Post job for the given tenant
        :param event:
        :param _tap:
        :return:
        """
        _LOG.info('Job post event came')
        tenant = self._obtain_tenant(
            event.tenant_name, _tap, event.customer_id
        )
        domain = RuleDomain.from_tenant_cloud(tenant.cloud)
        if not domain:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Cannot start job for tenant with cloud {tenant.cloud}'
                )
                .exc()
            )

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=event.target_regions, tenant=tenant
        )

        if jid := TenantSettingJobLock(tenant.name).locked_for(
            regions_to_scan
        ):
            raise (
                ResponseFactory(HTTPStatus.FORBIDDEN)
                .message(
                    f'Some requested regions are already being '
                    f'scanned in another tenant`s job {jid}'
                )
                .exc()
            )

        credentials_key = None
        if event.credentials:
            _LOG.info('Credentials were provided. Saving to secrets manager')
            credentials = event.credentials.model_dump()
            if not self._environment_service.skip_cloud_identifier_validation():
                _LOG.info('Validating cloud identifier')
                self._validate_cloud_identifier(
                    credentials=credentials,
                    cloud_identifier=tenant.project,
                    cloud=tenant.cloud.upper(),
                )
            credentials_key = self._ssm.prepare_name(tenant.name)
            self._ssm.create_secret(
                secret_name=credentials_key,
                secret_value=credentials,
                ttl=1800,  # should be enough for on-prem
            )

        standard_rulesets, lic, licensed_rulesets = (
            self._get_rulesets_for_scan(
                tenant=tenant,
                domain=domain,
                license_key=event.license_key,
                ruleset_names=set(event.iter_rulesets()),
            )
        )

        rules_to_scan = event.rules_to_scan
        if rules_to_scan:
            _LOG.info('Rules to scan were provided. Resolving them')
            available = set(
                chain.from_iterable(
                    r.rules
                    for r in chain(
                        standard_rulesets, licensed_rulesets
                    )  # not a bug, rules attribute is injected
                )
            )
            resolver = RuleNamesResolver(
                resolve_from=list(available), allow_multiple=True
            )
            resolved, not_resolved = [], []
            for rule, is_resolved in resolver.resolve_multiple_names(
                event.rules_to_scan
            ):
                if is_resolved:
                    resolved.append(rule)
                else:
                    not_resolved.append(rule)
            if not_resolved:
                return build_response(
                    code=HTTPStatus.BAD_REQUEST,
                    content=f'These rules are not allowed by your '
                    f'{tenant.cloud} '
                    f'license: {", ".join(not_resolved)}',
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
            rulesets=self._serialize_rulesets(
                standard_rulesets, lic, licensed_rulesets
            ),
            rules_to_scan=list(rules_to_scan or []),
            ttl=ttl,
            affected_license=lic.license_key if lic else None,
            status=JobState.PENDING,
            credentials_key=credentials_key,
            application_id=event.application_id,
            dojo_structure=event.dojo_structure(),
        )
        self._job_service.save(job)

        resp = self._submit_job_to_batch(
            tenant_name=tenant.name,
            job=job,
            timeout=int(event.timeout_minutes * 60)
            if event.timeout_minutes
            else None,
        )
        self._job_service.update(
            job=job,
            batch_job_id=resp['jobId'],
            celery_task_id=resp.get('celeryTaskId'),
        )
        return build_response(
            code=HTTPStatus.CREATED, content=self._job_service.dto(job)
        )

    def ensure_job_is_allowed(self, tenant: Tenant, tlk: str):
        _LOG.info(
            f'Going to check for permission to exhaust{tlk} TenantLicense(s).'
        )
        if not self._license_manager_service.cl.check_permission(
            customer=tenant.customer_name,
            tenant=tenant.name,
            tenant_license_key=tlk,
        ):
            message = (
                f"Tenant '{tenant.name}' could not be granted to "
                f'start a licensed job with tenant license {tlk}'
            )
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(message).exc()
        _LOG.info(
            f"Tenant '{tenant.name}' has been granted "
            f'permission to submit a licensed job.'
        )

    def _resolve_regions_to_scan(
        self,
        target_regions: set[str],
        tenant: Tenant
    ) -> set[str]:
        cloud = modular_helpers.tenant_cloud(tenant)
        if cloud == Cloud.AZURE or cloud == Cloud.GOOGLE:
            return {GLOBAL_REGION}  # cannot scan individual regions
        tenant_region = modular_helpers.get_tenant_regions(tenant, self._tss)
        missing = target_regions - tenant_region
        if missing:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Regions: {", ".join(missing)} not active '
                    f'in tenant: {tenant.name}'
                )
                .exc()
            )
        if not target_regions:  # all available must be scanned
            target_regions = tenant_region
        return target_regions

    @validate_kwargs
    def query(self, event: JobGetModel):
        if event.tenant_name:
            cursor = self._job_service.get_by_tenant_name(
                tenant_name=event.tenant_name,
                status=event.status,
                limit=event.limit,
                start=event.start_iso,
                end=event.end_iso,
                last_evaluated_key=NextToken.deserialize(
                    event.next_token
                ).value,
            )
        else:
            cursor = self._job_service.get_by_customer_name(
                customer_name=event.customer,
                status=event.status,
                limit=event.limit,
                start=event.start_iso,
                end=event.end_iso,
                last_evaluated_key=NextToken.deserialize(
                    event.next_token
                ).value,
            )
        jobs = list(cursor)
        return (
            ResponseFactory()
            .items(
                it=map(self._job_service.dto, jobs),
                next_token=NextToken(cursor.last_evaluated_key),
            )
            .build()
        )

    @validate_kwargs
    def get(self, event: BaseModel, job_id: str):
        _LOG.info('Job id was given, querying using it')
        job = self._job_service.get_nullable(job_id)
        if not job or event.customer and job.customer_name != event.customer:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(self._job_service.not_found_message(job_id))
                .exc()
            )
        return build_response(content=self._job_service.dto(job))

    @validate_kwargs
    def delete(self, event: BaseModel, job_id: str, _pe: ProcessedEvent):
        job = self._job_service.get_nullable(job_id)
        if not job or event.customer and job.customer_name != event.customer:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=self._job_service.not_found_message(job_id),
            )
        if job.status in (JobState.SUCCEEDED, JobState.FAILED):
            message = f'Can not terminate job with status {job.status}'
            _LOG.warning(message)
            return build_response(content=message, code=HTTPStatus.BAD_REQUEST)

        user_id = _pe['cognito_username']
        reason = (
            f"Initiated by user '{user_id}' "
            f"(customer '{event.customer or SystemCustomer.get_name()}')"
        )
        _LOG.info(f"Going to terminate job with id '{job.id}'")
        self._batch_client.terminate_job(job=job, reason=reason)

        self._job_service.update(
            job=job, reason=reason, status=JobState.FAILED
        )
        TenantSettingJobLock(job.tenant_name).release(job.id)

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content=f"The job with id '{job.id}' will be terminated",
        )

    @validate_kwargs
    def post_k8s(self, event: K8sJobPostModel, _tap: TenantsAccessPayload):
        platform = self._platform_service.get_nullable(
            hash_key=event.platform_id
        )
        if not platform or (
            event.customer and platform.customer != event.customer
        ):
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Active platform: {event.platform_id} not found',
            )
        tenant = self._obtain_tenant(
            platform.tenant_name, _tap, event.customer
        )
        if jid := TenantSettingJobLock(tenant.name).locked_for(
            platform.platform_id
        ):
            raise (
                ResponseFactory(HTTPStatus.FORBIDDEN)
                .message(
                    f'Job {jid} is already running for platform {platform.name}'
                )
                .exc()
            )

        standard_rulesets, lic, licensed_rulesets = (
            self._get_rulesets_for_scan(
                tenant=tenant,
                domain=RuleDomain.KUBERNETES,
                license_key=event.license_key,
                ruleset_names=set(event.iter_rulesets()),
            )
        )

        credentials_key = None  # TODO K8S validate whether long-lived token exists, validate whether it belongs to a cluster?
        if event.token:
            _LOG.debug('Temp token was provided. Saving to ssm')
            credentials_key = self._ssm.prepare_name(tenant.name)
            self._ssm.create_secret(
                secret_name=credentials_key,
                secret_value=event.token,
                ttl=1800,  # should be enough for on-prem
            )

        ttl_days = self._environment_service.jobs_time_to_live_days()
        ttl = None
        if ttl_days:
            ttl = timedelta(days=ttl_days)

        job = self._job_service.create(
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            regions=[],
            rulesets=self._serialize_rulesets(
                standard_rulesets, lic, licensed_rulesets
            ),
            ttl=ttl,
            platform_id=platform.id,
            affected_license=lic.license_key if lic else None,
            status=JobState.PENDING,
            credentials_key=credentials_key,
            dojo_structure=event.dojo_structure(),
        )
        self._job_service.save(job)
        resp = self._submit_job_to_batch(
            tenant=tenant,
            job=job,
            timeout=int(event.timeout_minutes * 60)
            if event.timeout_minutes
            else None,
        )
        self._job_service.update(
            job=job,
            batch_job_id=resp['jobId'],
            celery_task_id=resp.get('celeryTaskId'),
        )
        return build_response(
            code=HTTPStatus.CREATED, content=self._job_service.dto(job)
        )

    @validate_kwargs
    def post_scheduled(
        self, event: ScheduledJobPostModel, _tap: TenantsAccessPayload
    ):
        tenant = self._obtain_tenant(event.tenant_name, _tap, event.customer)
        domain = RuleDomain.from_tenant_cloud(tenant.cloud)
        if not domain:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(
                    f'Cannot start job for tenant with cloud {tenant.cloud}'
                )
                .exc()
            )
        name = event.name or self._scheduled_job_service.generate_name(
            tenant.name
        )
        if self._scheduled_job_service.get_by_name(tenant.customer_name, name):
            raise (
                ResponseFactory(HTTPStatus.CONFLICT)
                .message(f'Scheduled job with name {name} already exists')
                .exc()
            )

        regions_to_scan = self._resolve_regions_to_scan(
            target_regions=event.target_regions, tenant=tenant
        )
        standard_rulesets, lic, licensed_rulesets = (
            self._get_rulesets_for_scan(
                tenant=tenant,
                domain=domain,
                license_key=event.license_key,
                ruleset_names=set(event.iter_rulesets()),
            )
        )

        item = self._scheduled_job_service.create(
            name=name,
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            typ=ScheduledJobType.STANDARD,
            description=event.description,
            meta={
                'rulesets': self._serialize_rulesets(
                    standard_rulesets, lic, licensed_rulesets
                ),
                'regions': sorted(regions_to_scan),
            },
        )
        self._scheduled_job_service.set_celery_task(
            item=item,
            task='onprem.tasks.run_scheduled_job',
            sch=event.celery_schedule(),
            args=(tenant.customer_name, tenant.name),
        )
        self._scheduled_job_service.save(item)

        return build_response(
            code=HTTPStatus.CREATED,
            content=self._scheduled_job_service.dto(item),
        )

    @validate_kwargs
    def query_scheduled(self, event: ScheduledJobGetModel):
        cursor = self._scheduled_job_service.get_by_customer(
            customer_name=event.customer_id,
            typ=ScheduledJobType.STANDARD,
            tenant_name=event.tenant_name,
            limit=event.limit,
            last_evaluated_key=NextToken.deserialize(event.next_token).value,
        )
        jobs = list(cursor)
        return (
            ResponseFactory()
            .items(
                it=map(self._scheduled_job_service.dto, jobs),
                next_token=NextToken(cursor.last_evaluated_key),
            )
            .build()
        )

    @validate_kwargs
    def get_scheduled(self, event: BaseModel, name: str):
        item = self._scheduled_job_service.get_by_name(
            customer_name=event.customer_id, name=name
        )
        if not item or event.customer and item.customer_name != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).default().exc()
        return build_response(content=self._scheduled_job_service.dto(item))

    @validate_kwargs
    def delete_scheduled(self, event: BaseModel, name: str):
        item = self._scheduled_job_service.get_by_name(
            customer_name=event.customer_id, name=name
        )
        if not item or event.customer and item.customer_name != event.customer:
            return build_response(code=HTTPStatus.NO_CONTENT)
        self._scheduled_job_service.delete(item)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def patch_scheduled(self, event: ScheduledJobPatchModel, name: str):
        item = self._scheduled_job_service.get_by_name(
            customer_name=event.customer_id, name=name
        )
        if not item:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Scheduled job {name} not found',
            )
        self._scheduled_job_service.update(
            item=item,
            enabled=event.enabled,
            description=event.description,
            sch=event.celery_schedule(),
        )
        return build_response(content=self._scheduled_job_service.dto(item))

    def _validate_cloud_identifier(
        self, cloud_identifier: str, credentials: dict, cloud: Cloud
    ):
        identifier_validators_mapping = {
            Cloud.AWS: self._validate_aws_account_id,
            Cloud.AZURE: None,
            Cloud.GOOGLE: self._validate_gcp_project_id,
        }
        validator = identifier_validators_mapping.get(cloud)
        if not validator:
            return
        if not validator(credentials, cloud_identifier):
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content="Target account identifier didn't match with"
                ' one provided in the credentials. Check your '
                'credentials and try again.',
            )

    def _validate_aws_account_id(
        self, credentials: dict, target_account_id: str
    ):
        credentials_lower = {
            k.lower(): v
            for k, v in credentials.items()
            if k != 'AWS_DEFAULT_REGION'
        }
        try:
            account_id = self._sts_client.get_caller_identity(
                credentials=credentials_lower
            )['Account']
            return str(account_id) == str(target_account_id)
        except ClientError:
            message = 'Invalid AWS credentials provided.'
            _LOG.warning(message)
            return build_response(code=HTTPStatus.BAD_REQUEST, content=message)

    @staticmethod
    def _validate_gcp_project_id(credentials: dict, target_project_id: str):
        return str(credentials.get('project_id')) == str(target_project_id)
