import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    get_service_operation_status,
    DYNAMIC_DATE_ONLY_PAST_EXAMPLE, DYNAMIC_DATE_NOW_WITH_TIME_EXAMPLE,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import ServiceOperationType


_CLI_TO_API = {member.value[0]: member.value[1] for member in ServiceOperationType}


@click.group(name='service_operation')
def service_operation():
    """Manages service operations status tracking"""


@service_operation.command(
    cls=ViewCommand,
    name='status',
)
@click.option(
    '--operation', '-op',
    type=click.Choice(tuple(_CLI_TO_API.keys())),
    required=True,
    help='Service operation type to check status for',
)
@click.option(
    '--from_date', '-from',
    type=str,
    default=None,
    help=f'Query statuses from this date. Example: {DYNAMIC_DATE_ONLY_PAST_EXAMPLE}',
)
@click.option(
    '--to_date', '-to',
    type=str,
    default=None,
    help=f'Query statuses till this date. Example: {DYNAMIC_DATE_NOW_WITH_TIME_EXAMPLE}',
)
@cli_response()
def status(
    ctx: ContextObj,
    operation: str,
    from_date: str | None,
    to_date: str | None,
    customer_id: str | None = None,
) -> SREResponse:
    """Execution status of the specified service operation"""
    return get_service_operation_status(
        ctx=ctx,
        service_operation_type=_CLI_TO_API[operation],
        from_date=from_date,
        to_date=to_date,
    )

