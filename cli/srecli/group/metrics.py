import click

from srecli.group import (
    ContextObj, 
    ViewCommand, 
    cli_response, 
)
from srecli.service.constants import (
    ServiceOperationType, 
    OPERATION_STATUS_HINT,
)
from srecli.service.adapter_client import SREResponse


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response(
    hint=OPERATION_STATUS_HINT.format(
        operation_type=ServiceOperationType.UPDATE_METRICS.value[0],
    ),
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
