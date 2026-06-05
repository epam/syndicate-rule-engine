import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.service.adapter_client import SREResponse
from srecli.service.event_files import (
    events_file_option,
    load_events_from_file,
)


@click.group(name='maestro')
def maestro():
    """Send maestro audit events from a JSON file"""


@maestro.command(cls=ViewCommand, name='aws')
@events_file_option(
    help='JSON file with an array of maestro events (passed as-is to the API)',
)
@cli_response()
def maestro_aws(
    ctx: ContextObj,
    events_file: str,
    customer_id: str,
) -> SREResponse:
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx,
        customer_id=customer_id,
        events_file=events_file,
    )


@maestro.command(cls=ViewCommand, name='azure')
@events_file_option(
    help='JSON file with an array of maestro events (passed as-is to the API)',
)
@cli_response()
def maestro_azure(
    ctx: ContextObj,
    events_file: str,
    customer_id: str,
) -> SREResponse:
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx,
        customer_id=customer_id,
        events_file=events_file,
    )


@maestro.command(cls=ViewCommand, name='google')
@events_file_option(
    help='JSON file with an array of maestro events (passed as-is to the API)',
)
@cli_response()
def maestro_google(
    ctx: ContextObj,
    events_file: str,
    customer_id: str,
) -> SREResponse:
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx,
        customer_id=customer_id,
        events_file=events_file,
    )


def _run_maestro_events(
    ctx: ContextObj,
    *,
    customer_id: str,
    events_file: str,
) -> SREResponse:
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='MAESTRO',
        events=load_events_from_file(events_file),
        customer_id=customer_id,
    )
