import click

from c7ncli.group import cli_response, ViewCommand, ContextObj
from c7ncli.group import build_job_id_option, build_job_type_option
from c7ncli.service.constants import MANUAL_JOB_TYPE

required_job_id_option = build_job_id_option(required=True)
default_job_type_option = build_job_type_option(default=MANUAL_JOB_TYPE, show_default=True)

AVAILABLE_ERROR_FORMATS = ['json', 'xlsx']


@click.group(name='jobs')
def jobs():
    """Describes error reports of jobs"""


@jobs.command(cls=ViewCommand, name='total')
@required_job_id_option
@default_job_type_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def total(ctx: ContextObj, job_id: str, job_type: str, href: bool, format: str):
    """
    Describes all job error reports
    """
    return ctx['api_client'].report_errors_get(
        job_id=job_id, job_type=job_type, href=href, frmt=format
    )


@jobs.command(cls=ViewCommand, name='access')
@required_job_id_option
@default_job_type_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def access(
    ctx: ContextObj, job_id: str,
    job_type: str, href: bool, format: str
):
    """
    Describes access-related job error reports
    """

    return ctx['api_client'].report_errors_get(
        job_id=job_id, job_type=job_type, href=href, frmt=format,
        subtype='access'
    )


@jobs.command(cls=ViewCommand, name='core')
@required_job_id_option
@default_job_type_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def core(
    ctx: ContextObj, job_id: str, job_type: str,
    href: bool, format: str
):
    """
    Describes core-related job error reports
    """

    return ctx['api_client'].report_errors_get(
        job_id=job_id, job_type=job_type, href=href, frmt=format,
        subtype='core'
    )
