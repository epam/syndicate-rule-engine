from typing import Optional

import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    cli_response,
    SREResponse,
)
from srecli.service.constants import PolicyErrorType
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='errors')
def errors():
    """Describes error reports"""


@errors.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@click.option('--error_type', '-et', type=click.Choice(tuple(PolicyErrorType.iter())))
@click.option(
    '--format',
    '-ft',
    type=click.Choice(('json', 'xlsx')),
    default='json',
    show_default=True,
    help='Format of the file behind the presigned reference',
)
@click.option(
    '--href',
    '-hf',
    is_flag=True,
    help='Return presigned URL instead of inline payload',
)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    error_type: Optional[str],
    href: bool,
    format: str,
    customer_id: str | None,
) -> SREResponse:
    """
    Describes errors report of a specific job
    """
    return ctx['api_client'].report_errors_job(
        job_id=job_id,
        href=href,
        format=format,
        error_type=error_type,
        customer_id=customer_id
    )
