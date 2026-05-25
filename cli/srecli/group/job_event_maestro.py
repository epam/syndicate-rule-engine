import json
import click
from pathlib import Path
from typing import Any

from srecli.group import ContextObj, ViewCommand, cli_response


def _events_file_option(f):
    return click.option(
        '--events-file',
        '-f',
        required=True,
        type=click.Path(exists=True, dir_okay=False, readable=True),
        help='JSON file with an array of maestro events (passed as-is to the API)',
    )(f)


@click.group(name='maestro')
def maestro():
    """Send maestro audit events from a JSON file"""


@maestro.command(cls=ViewCommand, name='aws')
@_events_file_option
@cli_response()
def maestro_aws(ctx: ContextObj, events_file: str, customer_id: str):
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx, customer_id=customer_id, events_file=events_file
    )


@maestro.command(cls=ViewCommand, name='azure')
@_events_file_option
@cli_response()
def maestro_azure(ctx: ContextObj, events_file: str, customer_id: str):
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx, customer_id=customer_id, events_file=events_file
    )


@maestro.command(cls=ViewCommand, name='google')
@_events_file_option
@cli_response()
def maestro_google(ctx: ContextObj, events_file: str, customer_id: str):
    """Send maestro events from a JSON file (vendor MAESTRO)."""
    return _run_maestro_events(
        ctx, customer_id=customer_id, events_file=events_file
    )


def _load_events_from_file(path: str) -> list[dict[str, Any]]:
    file_path = Path(path)
    try:
        data = json.loads(file_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as e:
        raise click.ClickException(
            f'Invalid JSON in {file_path}: {e.msg}'
        ) from e
    except OSError as e:
        raise click.ClickException(f'Cannot read {file_path}: {e}') from e
    if not isinstance(data, list):
        raise click.ClickException(
            f'{file_path}: expected a JSON array of events'
        )
    if not data:
        raise click.ClickException(f'{file_path}: events array is empty')
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise click.ClickException(
                f'Event at index {i} must be a JSON object'
            )
    return data


def _run_maestro_events(
    ctx: ContextObj,
    *,
    customer_id: str,
    events_file: str,
) -> Any:
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='MAESTRO',
        events=_load_events_from_file(events_file),
        customer_id=customer_id,
    )
