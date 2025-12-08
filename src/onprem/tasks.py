from executor.job import task_scheduled_job, task_standard_job, upload_to_dojo, \
    update_metadata
from helpers import RequestContext
from helpers.constants import Env
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
def collect_recommendations():
    # After findings are collected, findings trigger recommendations.
    # This feature works only in `MetricsUpdater` lambda.
    MetricsUpdater.build().lambda_handler(
        event={'data_type': 'findings'},
        context=RequestContext(),
    )


@app.task
def run_update_metadata():
    update_metadata()


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
