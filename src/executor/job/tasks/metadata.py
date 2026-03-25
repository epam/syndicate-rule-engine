"""Metadata update Celery task."""

import operator
from itertools import chain

from modular_sdk.commons.constants import ApplicationType
from modular_sdk.commons.trace_helper import tracer_decorator

from executor.helpers.constants import ExecutorError
from executor.job.types import ExecutorException
from helpers.constants import ServiceOperationType
from helpers.log_helper import get_logger
from services import SERVICE_PROVIDER

_LOG = get_logger(__name__)


@tracer_decorator(
    is_job=True,
    component=ServiceOperationType.UPDATE_METADATA.value,
)
def update_metadata():
    _LOG.info('Starting metadata update task for all customers')

    license_service = SERVICE_PROVIDER.license_service
    metadata_provider = SERVICE_PROVIDER.metadata_provider
    customer_service = SERVICE_PROVIDER.modular_client.customer_service()
    application_service = SERVICE_PROVIDER.modular_client.application_service()

    _LOG.info('Collecting licenses from all customers')
    customer_names = map(
        operator.attrgetter('name'),
        customer_service.i_get_customer(),
    )
    license_applications = chain.from_iterable(
        application_service.i_get_application_by_customer(
            customer_name,
            ApplicationType.CUSTODIAN_LICENSES.value,
            deleted=False,
        )
        for customer_name in customer_names
    )
    licenses = list(license_service.to_licenses(license_applications))

    total_licenses = len(licenses)
    _LOG.info(f'Found {total_licenses} license(s) to update')

    if not licenses:
        _LOG.warning('No licenses found - skipping metadata update')
        return

    successful_updates = 0
    failed_updates = 0

    for license_obj in licenses:
        license_key = license_obj.license_key
        try:
            _LOG.info(f'Updating metadata for license: {license_key}')
            metadata = metadata_provider.refresh(license_obj)
            if not metadata.rules and not metadata.domains:
                _LOG.warning(
                    f'Metadata update returned empty metadata for license: {license_key}'
                )
                failed_updates += 1
            else:
                _LOG.info(f'Successfully updated metadata for license: {license_key}')
                successful_updates += 1
        except Exception as e:
            _LOG.error(
                f'Failed to update metadata for license {license_key}: {e}',
                exc_info=True,
            )
            failed_updates += 1

    if failed_updates > 0:
        reason = (
            f'Failed to update metadata for {failed_updates}/{total_licenses} '
            'license(s)'
        )
        raise ExecutorException(
            ExecutorError.with_reason(
                value=ExecutorError.METADATA_UPDATE_FAILED,
                reason=reason,
            )
        )

    _LOG.info(
        f'Metadata for {successful_updates}/{total_licenses} '
        'licenses updated successfully'
    )
