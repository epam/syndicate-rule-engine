import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    service_job_from_date_option, service_job_to_date_option,
    get_service_job_status,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import ServiceJobType


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response(
    hint="Use 'sre metrics update_status' to check execution status",
)
def update(
    ctx: ContextObj,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Triggers a metrics update for Syndicate Rule Engine reports. Report data will
    contain data up to the time when the trigger was executed
    """
    return ctx['api_client'].trigger_metrics_update()


@metrics.command(
    cls=ViewCommand, 
    name='update_status',
)
@service_job_from_date_option
@service_job_to_date_option
@cli_response()
def update_status(
    ctx: ContextObj,
    from_date: str | None,
    to_date: str | None,
    customer_id: str | None = None,
) -> SREResponse:
    """Execution status of the last metrics update"""
    return get_service_job_status(
        ctx=ctx,
        service_job_type=ServiceJobType.UPDATE_METRICS.value,
        from_date=from_date,
        to_date=to_date,
    )
