import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.service.adapter_client import SREResponse


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response(
    hint="Use 'sre service_operation status --operation metrics_update' to check execution status",
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
