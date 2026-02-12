from executor.job import (
    task_scheduled_job,
    task_standard_job,
    update_metadata,
    upload_to_dojo,
)
from helpers import RequestContext
from helpers.constants import ACTION_PARAM, Env
from lambdas.license_updater.handler import LicenseUpdater
from lambdas.metrics_updater.handler import MetricsUpdater
from lambdas.metrics_updater.processors.expired_metrics_processor import (
    ExpiredMetricsCleaner,
)
from lambdas.rule_meta_updater.handler import RuleMetaUpdaterLambdaHandler
from onprem.celery import app


@app.task(
    bind=True,
    time_limit=3600 * 4,
    soft_time_limit=Env.BATCH_JOB_LIFETIME_MINUTES.as_float() * 60,
)
def run_standard_job(self, job_id: str | list[str]):
    if isinstance(job_id, str):
        job_id = [job_id]

    for jid in job_id:
        task_standard_job(self, jid)


@app.task(
    bind=True,
    time_limit=3600 * 4,
    soft_time_limit=Env.BATCH_JOB_LIFETIME_MINUTES.as_float() * 60,
)
def run_scheduled_job(self, customer_name: str, name: str):
    return task_scheduled_job(self, customer_name, name)


@app.task
def assemble_events() -> None:
    """
    OnPrem equivalent of event-handler Lambda (assemble-events action)
    Runs every 5 minutes to process accumulated events
    """
    from lambdas.event_handler.handler import (
        ASSEMBLE_EVENTS_ACTION,
        EventHandler,
    )
    handler = EventHandler.build()
    handler.handle_request(
        event={ACTION_PARAM: ASSEMBLE_EVENTS_ACTION},
        context=RequestContext(),
    )


@app.task
def clear_events() -> None:
    """
    OnPrem equivalent of event-handler Lambda (clear-events action)
    Runs daily to clean up old events
    """
    from lambdas.event_handler.handler import CLEAR_EVENTS_ACTION, EventHandler
    handler = EventHandler.build()
    handler.handle_request(
        event={ACTION_PARAM: CLEAR_EVENTS_ACTION},
        context=RequestContext(),
    )


@app.task
def make_findings_snapshot():
    # After findings are collected, findings trigger recommendations.
    # This feature works only in `MetricsUpdater` lambda.
    MetricsUpdater.build().lambda_handler(
        event={'data_type': 'findings'},
        context=RequestContext(),
    )


@app.task
def sync_license(
    license_keys: list[str] | str | None = None,
    overwrite_rulesets: bool = False,
):
    if isinstance(license_keys, str):
        license_keys = [license_keys]
    event = {'overwrite_rulesets': overwrite_rulesets}
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
def run_update_metadata():
    update_metadata()


@app.task
def delete_expired_metrics():
    ExpiredMetricsCleaner.build().__call__()


@app.task
def collect_resources() -> None:
    from executor.job import CustodianResourceCollector
    
    collector = CustodianResourceCollector.build()
    collector.collect_all_resources()


@app.task
def push_to_dojo(job_ids: list[str] | str):
    if isinstance(job_ids, str):
        job_ids = [job_ids]
    upload_to_dojo(job_ids)
