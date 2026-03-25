"""SIEM integration (DefectDojo + Chronicle) for job execution."""

from executor.job.execution.context import JobExecutionContext
from executor.job.integration.dojo import import_to_dojo
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from services import SP
from services.chronicle_service import ChronicleConverterType
from services.clients.chronicle import ChronicleV2Client
from services.sharding import ShardsCollection
from services.udm_generator import (
    ShardCollectionUDMEntitiesConvertor,
    ShardCollectionUDMEventsConvertor,
)

_LOG = get_logger(__name__)


def upload_to_siem(ctx: JobExecutionContext, collection: ShardsCollection):
    tenant = ctx.tenant
    job = ctx.job
    platform = ctx.platform
    warnings = []
    cloud = ctx.cloud()

    metadata = SP.license_service.get_customer_metadata(tenant.customer_name)

    dojo_warnings = import_to_dojo(
        job=job,
        tenant=tenant,
        cloud=cloud,
        platform=platform,
        collection=collection,
        metadata=metadata,
        send_after_job=True,
    )
    warnings.extend(dojo_warnings)

    mcs = SP.modular_client.maestro_credentials_service()
    for (
        chronicle,
        configuration,
    ) in SP.integration_service.get_chronicle_adapters(tenant, True):
        _LOG.debug('Going to push data to Chronicle')
        creds = mcs.get_by_application(
            chronicle.credentials_application_id, tenant
        )
        if not creds:
            continue
        client = ChronicleV2Client(
            url=chronicle.endpoint,
            credentials=creds.GOOGLE_APPLICATION_CREDENTIALS,
            customer_id=chronicle.instance_customer_id,
        )
        match configuration.converter_type:
            case ChronicleConverterType.EVENTS:
                _LOG.debug('Converting our collection to UDM events')
                convertor = ShardCollectionUDMEventsConvertor(
                    cloud, metadata, tenant=tenant
                )
                success = client.create_udm_events(
                    events=convertor.convert(collection)
                )
            case _:  # ENTITIES
                _LOG.debug('Converting our collection to UDM entities')
                convertor = ShardCollectionUDMEntitiesConvertor(
                    cloud, metadata, tenant=tenant
                )
                success = client.create_udm_entities(
                    entities=convertor.convert(collection),
                    log_type='AWS_API_GATEWAY',
                )
        if not success:
            warnings.append(
                f'could not upload data to Chronicle {chronicle.id}'
            )
    if warnings:
        ctx.add_warnings(*warnings)
