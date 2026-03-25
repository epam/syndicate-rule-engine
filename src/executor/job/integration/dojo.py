"""DefectDojo integration for job execution."""

from typing import Iterable

from modular_sdk.commons.trace_helper import tracer_decorator
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud, ServiceOperationType
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.job import Job
from services import SP
from services.clients.dojo_client import DojoV2Client
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform
from services.report_convertors import ShardCollectionDojoConvertor
from services.sharding import ShardsCollection

_LOG = get_logger(__name__)


def import_to_dojo(
    job: Job,
    tenant: Tenant,
    cloud: Cloud,
    collection: ShardsCollection,
    metadata: Metadata,
    platform: Platform | None = None,
    send_after_job: bool | None = None,
) -> list:
    warnings = []

    for dojo, configuration in SP.integration_service.get_dojo_adapters(
        tenant=tenant,
        send_after_job=send_after_job,
    ):
        convertor = ShardCollectionDojoConvertor.from_scan_type(
            configuration.scan_type, cloud, metadata
        )
        configuration = configuration.substitute_fields(job, platform)
        client = DojoV2Client(
            url=dojo.url,
            api_key=SP.defect_dojo_service.get_api_key(dojo),
        )
        try:
            client.import_scan(
                scan_type=configuration.scan_type,
                scan_date=utc_datetime(),
                product_type_name=configuration.product_type,
                product_name=configuration.product,
                engagement_name=configuration.engagement,
                test_title=configuration.test,
                data=convertor.convert(collection),
                tags=SP.integration_service.job_tags_dojo(job),
            )
        except Exception as e:
            _LOG.exception(f'Unexpected error occurred pushing to dojo: {e}')
            warnings.append(f'could not upload data to DefectDojo {dojo.id}')

    return warnings


@tracer_decorator(
    is_job=True,
    component=ServiceOperationType.PUSH_DOJO.value,
)
def upload_to_dojo(job_ids: Iterable[str]):
    for job_id in job_ids:
        _LOG.info(f'Uploading job {job_id} to dojo')
        job = SP.job_service.get_nullable(job_id)
        if not job:
            _LOG.warning(
                f'Job {job_id} not found. Skipping upload to dojo'
            )
            continue
        tenant = SP.modular_client.tenant_service().get(job.tenant_name)
        if not tenant:
            _LOG.warning(
                f'Tenant {job.tenant_name} not found. Skipping upload to dojo'
            )
            continue

        platform = None
        if job.is_platform_job:
            platform = SP.platform_service.get_nullable(job.platform_id)
            if not platform:
                _LOG.warning('Job platform not found. Skipping upload to dojo')
                continue
            collection = SP.report_service.platform_job_collection(
                platform=platform,
                job=job,
            )
            collection.meta = SP.report_service.fetch_meta(platform)
            cloud = Cloud.KUBERNETES
        else:
            collection = SP.report_service.job_collection(tenant, job)
            collection.meta = SP.report_service.fetch_meta(tenant)
            cloud = tenant_cloud(tenant)

        collection.fetch_all()

        metadata = SP.license_service.get_customer_metadata(tenant.customer_name)

        import_to_dojo(
            job=job,
            tenant=tenant,
            cloud=cloud,
            platform=platform,
            collection=collection,
            metadata=metadata,
        )
