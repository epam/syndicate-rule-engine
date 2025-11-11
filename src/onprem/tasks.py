from executor.job import task_scheduled_job, task_standard_job, upload_to_dojo
from helpers import RequestContext
from helpers.constants import Env
from helpers.log_helper import get_logger
from lambdas.license_updater.handler import LicenseUpdater
from lambdas.metrics_updater.handler import MetricsUpdater
from lambdas.rule_meta_updater.handler import RuleMetaUpdaterLambdaHandler
from lambdas.metrics_updater.processors.findings_processor import (
    FindingsUpdater,
)
from lambdas.metrics_updater.processors.expired_metrics_processor import (
    ExpiredMetricsCleaner,
)
from services.resources_collector import CustodianResourceCollector
from onprem.celery import app

_LOG = get_logger(__name__)


@app.task(
    bind=True,
    time_limit=3600 * 4,
    soft_time_limit=Env.BATCH_JOB_LIFETIME_MINUTES.as_float() * 60,
)
def run_standard_job(self, job_id: str):
    return task_standard_job(self, job_id)


@app.task(
    bind=True,
    time_limit=3600 * 4,
    soft_time_limit=Env.BATCH_JOB_LIFETIME_MINUTES.as_float() * 60,
)
def run_scheduled_job(self, customer_name: str, name: str):
    return task_scheduled_job(self, customer_name, name)


@app.task
def make_findings_snapshot():
    FindingsUpdater.build().__call__()


@app.task
def sync_license(license_keys: list[str] | str | None = None):
    if isinstance(license_keys, str):
        license_keys = [license_keys]
    event = {}
    if license_keys:
        event['license_keys'] = list(license_keys)
    LicenseUpdater.build().lambda_handler(
        event=event, context=RequestContext()
    )


@app.task
def sync_rulesource(rule_source_ids: list[str] | str):
    if isinstance(rule_source_ids, str):
        rule_source_ids = [rule_source_ids]
    RuleMetaUpdaterLambdaHandler.build().lambda_handler(
        event={'rule_source_ids': rule_source_ids}, context=RequestContext()
    )


@app.task
def collect_metrics():
    MetricsUpdater.build().lambda_handler(
        event={'data_type': 'metrics'}, context=RequestContext()
    )


@app.task
def update_metadata():
    from itertools import chain
    import operator
    from modular_sdk.commons.constants import ApplicationType
    from services import SERVICE_PROVIDER

    _LOG.info('Starting metadata update task for all customers')
    
    license_service = SERVICE_PROVIDER.license_service
    metadata_provider = SERVICE_PROVIDER.metadata_provider
    customer_service = SERVICE_PROVIDER.modular_client.customer_service()
    application_service = SERVICE_PROVIDER.modular_client.application_service()
    
    _LOG.info('Collecting licenses from all customers')
    customer_names = map(
        operator.attrgetter('name'), 
        customer_service.i_get_customer()
    )
    license_applications = chain.from_iterable(
        application_service.i_get_application_by_customer(
            customer_name, 
            ApplicationType.CUSTODIAN_LICENSES.value, 
            deleted=False
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
            metadata_provider.get_no_cache(license_obj)
            _LOG.info(f'Successfully updated metadata for license: {license_key}')
            successful_updates += 1
        except Exception as e:
            _LOG.error(
                f'Failed to update metadata for license {license_key}: {e}',
                exc_info=True
            )
            failed_updates += 1
    
    _LOG.info(
        f'Metadata update completed. '
        f'Total: {total_licenses}, '
        f'Successful: {successful_updates}, '
        f'Failed: {failed_updates}'
    )


@app.task
def delete_expired_metrics():
    ExpiredMetricsCleaner.build().__call__()


@app.task
def collect_resources():
    CustodianResourceCollector.build().collect_all_resources()


@app.task
def push_to_dojo(job_ids: list[str] | str):
    if isinstance(job_ids, str):
        job_ids = [job_ids]
    upload_to_dojo(job_ids)
