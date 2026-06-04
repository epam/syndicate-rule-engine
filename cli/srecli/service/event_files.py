import json
from pathlib import Path
from typing import Any

import click


def events_file_option(*, help: str):
    def decorator(f):
        return click.option(
            '--events-file',
            '-f',
            required=True,
            type=click.Path(exists=True, dir_okay=False, readable=True),
            help=help,
        )(f)

    return decorator


def load_events_from_file(path: str) -> list[dict[str, Any]]:
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
