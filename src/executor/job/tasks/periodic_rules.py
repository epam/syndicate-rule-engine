"""Run periodic rules job Celery tasks."""

from datetime import timedelta
from typing import TYPE_CHECKING

from helpers.constants import JobState
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services import modular_helpers
from services.metadata import DEFAULT_VERSION
from services.job_lock import TenantSettingJobLock

from helpers.constants import Cloud, GLOBAL_REGION, JobType
from services.ruleset_service import RulesetName

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant
    from modular_sdk.services.tenant_settings_service import TenantSettingsService
    from services.license_service import License

_LOG = get_logger(__name__)


PERIODIC_RULES_MARKER = 'process-periodic-rules'
ACTIVE_SCHEDULED_STATES = (
    JobState.SUBMITTED,
    JobState.PENDING,
    JobState.RUNNABLE,
    JobState.STARTING,
    JobState.RUNNING,
)


def create_periodic_rules_jobs() -> list[str]:

    tenant_service = SP.modular_client.tenant_service()
    tss = SP.modular_client.tenant_settings_service()

    created_job_ids: list[str] = []
    for tenant in tenant_service.i_scan_tenants(only_active=True):
        lic = SP.license_service.get_tenant_license(tenant)

        if not _is_valid_license_for_tenant(tenant, lic):
            continue

        periodic_rules = SP.s3_periodic_mapping_provider.get_from_s3(
            license_key=lic.license_key,
            version=DEFAULT_VERSION,
            cloud=tenant.cloud,
        )
        if not periodic_rules:
            _LOG.debug(
                f'Skipping license {lic.license_key}: periodic_rules '
                'artifact not found'
            )
            continue

        regions = _regions_to_scan(tenant, tss)
        if not regions:
            continue
        if TenantSettingJobLock(tenant.name).locked_for(regions):
            _LOG.info(
                f'Skipping periodic rules for tenant {tenant.name}: '
                'region lock is held'
            )
            continue
        if _has_active_periodic_job(tenant.name):
            _LOG.info(
                f'Skipping periodic rules for tenant {tenant.name}: '
                'matching active process-periodic-rules job already exists'
            )
            continue

        ttl_days = SP.environment_service.jobs_time_to_live_days()
        ttl = timedelta(days=ttl_days) if ttl_days else None
        rulesets = [
            RulesetName(_id, None, lic.license_key).to_str()
            for _id in lic.ruleset_ids
        ]
        job = SP.job_service.create(
            customer_name=tenant.customer_name,
            tenant_name=tenant.name,
            regions=regions,
            rules_to_scan=periodic_rules,
            rulesets=rulesets,
            ttl=ttl,
            affected_license=lic.license_key,
            job_type=JobType.SCHEDULED,
            status=JobState.PENDING,
            scheduled_rule_name=PERIODIC_RULES_MARKER,
        )
        SP.job_service.save(job)
        created_job_ids.append(job.id)
        _LOG.info(
            f'Created process-periodic-rules job {job.id} for tenant '
            f'{tenant.name} with {len(periodic_rules)} rule(s)'
        )

    return created_job_ids


def _is_valid_license_for_tenant(tenant: Tenant, lic: License) -> bool:
    if not lic:
        _LOG.debug(f'Skipping tenant {tenant.name}: no license found')
        return False

    if lic.is_expired():
        _LOG.debug(
            f'Skipping tenant {tenant.name}: '
            f'license {lic.license_key} is expired'
        )
        return False

    if not SP.license_service.is_subject_applicable(
        lic=lic,
        customer=tenant.customer_name,
        tenant_name=tenant.name,
    ):
        _LOG.debug(
            f'Tenant {tenant.name} is not applicable for license '
            f'{lic.license_key} for customer {tenant.customer_name}'
        )
        return False

    if not lic.event_driven.get('active'):
        _LOG.debug(
            f'Skipping tenant {tenant.name}: '
            f'event-driven is not active for license {lic.license_key}'
        )
        return False

    return True


def _regions_to_scan(
    tenant: Tenant,
    tenant_settings_service: TenantSettingsService,
) -> list[str]:
    cloud = Cloud.parse(tenant.cloud)
    if cloud in (Cloud.AZURE, Cloud.GOOGLE):
        return [GLOBAL_REGION]

    regions = modular_helpers.get_tenant_regions(
        tenant, tenant_settings_service
    )
    return sorted(regions)


def _has_active_periodic_job(tenant_name: str) -> bool:

    query = Job.status.is_in(ACTIVE_SCHEDULED_STATES)
    query &= Job.scheduled_rule_name == PERIODIC_RULES_MARKER

    jobs = SP.job_service.get_by_tenant_name(
        tenant_name=tenant_name,
        job_type=JobType.SCHEDULED,
        filter_condition=query,
        limit=1,
    )

    return bool(list(jobs))
