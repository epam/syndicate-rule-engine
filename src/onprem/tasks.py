"""
Main tasks for Celery Worker.

Any task can be wrapped with @safe_call decorator to catch and log any exceptions.
"""

from typing import Any

from executor.job import (
    task_scheduled_job,
    task_standard_job,
    update_metadata,
    upload_to_dojo,
    remove_shards,
)
from helpers import RequestContext
from helpers.constants import ACTION_PARAM, Env
from helpers.exceptions import safe_call
from helpers.lambda_response import log_lambda_response
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
@safe_call
def run_standard_job(self, job_id: str | list[str]) -> None:
    if isinstance(job_id, str):
        job_id = [job_id]

    for jid in job_id:
        task_standard_job(self, jid)


@app.task(
    bind=True,
    time_limit=3600 * 4,
    soft_time_limit=Env.BATCH_JOB_LIFETIME_MINUTES.as_float() * 60,
)
@safe_call
def run_scheduled_job(self, customer_name: str, name: str) -> None:
    task_scheduled_job(self, customer_name, name)


@app.task
@safe_call
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
    resp = handler.handle_request(
        event={ACTION_PARAM: ASSEMBLE_EVENTS_ACTION},
        context=RequestContext(),
    )
    log_lambda_response(resp, is_debug=True)


@app.task
@safe_call
def clear_events() -> None:
    """
    OnPrem equivalent of event-handler Lambda (clear-events action)
    Runs daily to clean up old events
    """
    from lambdas.event_handler.handler import CLEAR_EVENTS_ACTION, EventHandler

    handler = EventHandler.build()
    resp = handler.handle_request(
        event={ACTION_PARAM: CLEAR_EVENTS_ACTION},
        context=RequestContext(),
    )
    log_lambda_response(resp, is_debug=True)


@app.task
@safe_call
def make_findings_snapshot() -> None:
    # After findings are collected, findings trigger recommendations.
    # This feature works only in `MetricsUpdater` lambda.
    MetricsUpdater.build().lambda_handler(
        event={"data_type": "findings"},
        context=RequestContext(),
    )


@app.task
@safe_call
def sync_license(
    license_keys: list[str] | str | None = None,
    overwrite_rulesets: bool = False,
) -> None:
    if isinstance(license_keys, str):
        license_keys = [license_keys]
    event: dict[str, Any] = {"overwrite_rulesets": overwrite_rulesets}
    if license_keys:
        event["license_keys"] = list(license_keys)
    LicenseUpdater.build().lambda_handler(event=event, context=RequestContext())


@app.task
@safe_call
def sync_rulesource(rule_source_ids: list[str] | str) -> None:
    if isinstance(rule_source_ids, str):
        rule_source_ids = [rule_source_ids]
    RuleMetaUpdaterLambdaHandler.build().lambda_handler(
        event={"rule_source_ids": rule_source_ids}, context=RequestContext()
    )


@app.task
@safe_call
def collect_metrics() -> None:
    MetricsUpdater.build().lambda_handler(
        event={"data_type": "metrics"}, context=RequestContext()
    )


@app.task
@safe_call
def run_update_metadata() -> None:
    update_metadata()


@app.task
@safe_call
def delete_expired_metrics() -> None:
    ExpiredMetricsCleaner.build().__call__()


@app.task
@safe_call
def collect_resources() -> None:
    from executor.job import CustodianResourceCollector

    collector = CustodianResourceCollector.build()
    collector.collect_all_resources()


@app.task
@safe_call
def push_to_dojo(job_ids: list[str] | str) -> None:
    if isinstance(job_ids, str):
        job_ids = [job_ids]
    upload_to_dojo(job_ids)


@app.task
@safe_call
def generate_reactive_report(job_id: str) -> None:
    """
    Generate attacks report for a single REACTIVE job and send via RabbitMQ
    (immediate mode).
    """
    from services import SP

    SP.report_delivery_service.generate_and_send_report_immediate(job_id)


@app.task
@safe_call
def process_interval_reports() -> None:
    """
    Celery Beat task: for each tenant with report_delivery mode=interval,
    check if interval has elapsed, aggregate attacks from jobs in window,
    send if any.
    """
    from services import SP

    SP.report_delivery_service.process_interval_reports()


@app.task
@safe_call
def remove_old_shards(days) -> None:
    """
    Remove shard parts that were last updated N days ago
    """
    remove_shards(days)
