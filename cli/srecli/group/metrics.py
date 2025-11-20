import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    build_background_job_status_command
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import BackgroundJobName


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response()
def update(
    ctx: ContextObj,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Triggers a metrics update for Syndicate Rule Engine reports. Report data will
    contain data up to the time when the trigger was executed
    """
    return ctx['api_client'].trigger_metrics_update()


build_background_job_status_command(
    group=metrics,
    background_job_name=BackgroundJobName.METRICS,
    help_text='Execution status of the last metrics update',
)
