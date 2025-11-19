import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    DYNAMIC_DATE_ONLY_EXAMPLE, DYNAMIC_DATE_ONLY_PAST_EXAMPLE
)
from srecli.service.adapter_client import SREResponse


_GROUP_NAME = 'metrics'


@click.group(name=_GROUP_NAME)
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


@metrics.command(cls=ViewCommand, name='status')
@click.option(
    '--from_date', '-from', 
    type=str,
    help=(
        'Query metrics statuses from this date. Accepts date ISO '
        f'string. Example: {DYNAMIC_DATE_ONLY_PAST_EXAMPLE}'
    ),
)
@click.option(
    '--to_date', '-to', 
    type=str,
    help=(
        'Query metrics statuses till this date. Accepts date ISO '
        f'string. Example: {DYNAMIC_DATE_ONLY_EXAMPLE}'
    ),
)
@cli_response()
def status(
    ctx: ContextObj,
    from_date: str,
    to_date: str,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Execution status of the last metrics update
    """
    params = {
        'from': from_date,
        'to': to_date
    }
    return ctx['api_client'].background_job_status(
        background_job_name=_GROUP_NAME,
        **params
    )
