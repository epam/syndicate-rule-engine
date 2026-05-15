from typing import Optional

import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    build_job_type_option,
    cli_response,
    SREResponse,
)
from srecli.service.presigned_reports import report_presigned_download_pack
from srecli.service.constants import PolicyErrorType


@click.group(name='errors')
def errors():
    """Describes error reports"""


@errors.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@build_job_type_option()
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
    help='Return presigned URL instead of inline payload (set automatically with --download)',
)
@cli_response()
@report_presigned_download_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    job_type: str,
    error_type: Optional[str],
    href: bool,
    format: str,
    download: str | None,
    customer_id,
) -> SREResponse:
    """
    Describes errors report of a specific job
    """
    return ctx['api_client'].report_errors_job(
        job_id=job_id,
        job_type=job_type,
        href=href,
        format=format,
        error_type=error_type,
        customer_id=customer_id
    )
