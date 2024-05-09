from typing import Optional

import click

from c7ncli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    build_job_type_option,
    cli_response,
)
from c7ncli.service.constants import PolicyErrorType


@click.group(name='errors')
def errors():
    """Describes error reports"""


@errors.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@build_job_type_option()
@click.option('--href', '-hf', is_flag=True,
              help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(('json', 'xlsx')),
              default='json', show_default=True,
              help='Format of the file within the hypertext reference')
@click.option('--error_type', '-et', type=click.Choice(tuple(PolicyErrorType.iter())))
@cli_response()
def jobs(ctx: ContextObj, job_id: str, job_type: str, href: bool, format: str,
         error_type: Optional[str], customer_id):
    """
    Errors of specific job
    """
    return ctx['api_client'].report_errors_job(
        job_id=job_id,
        job_type=job_type,
        href=href,
        format=format,
        error_type=error_type,
        customer_id=customer_id
    )
