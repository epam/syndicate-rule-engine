"""
Click decorator: presigned report ``--download`` and post-request fetch/decompress.
Declare ``--href`` and ``--format`` on each command as needed; ``--download`` implies
``--href`` when the command accepts ``href``.
"""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, Callable

import click

from srecli.service.adapter_client import SREResponse

from .download import save_presigned_payloads


def report_presigned_download_pack(func: Callable) -> Callable:
    """`--download` + save after SREResponse; forces `href=True` when `download` is set."""

    accepts_href = 'href' in inspect.signature(func).parameters

    @wraps(func)
    def wrapper(*args, **kwargs):
        if kwargs.get('download') and accepts_href:
            kwargs = {**kwargs, 'href': True}
        return _after_response_save(func(*args, **kwargs))

    wrapper = click.option(
        '--download',
        type=click.Path(file_okay=True, dir_okay=True, writable=True),
        default=None,
        help=(
            'Download presigned report from URL, decompress gzip, and save to '
            'this file or directory. Implies presigned URL mode when supported.'
        ),
    )(wrapper)
    return wrapper


def download_unsupported_pack(func: Callable) -> Callable:
    """Adds ``--download`` to help; rejects use with a clear message until supported."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    wrapper = click.option(
        '--download',
        type=click.Path(file_okay=True, dir_okay=True, writable=True),
        default=None,
        expose_value=False,
        is_eager=True,
        callback=_download_unavailable_callback,
        help=(
            'Not available for this command yet; use --href for presigned URLs.'
        ),
    )(wrapper)
    return wrapper


def _ctx_download_path() -> str | None:
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return None
    d = ctx.params.get('download')
    return str(d) if d else None


def _after_response_save(resp: SREResponse) -> SREResponse:
    path = _ctx_download_path()
    if path:
        save_presigned_payloads(resp, path)
    return resp


def _download_unavailable_callback(
    ctx: click.Context,
    param: click.Parameter,
    value: Any,
) -> None:
    raise click.UsageError(
        "'--download' is not available for this command yet. "
        'Use --href to get presigned URLs.'
    )
