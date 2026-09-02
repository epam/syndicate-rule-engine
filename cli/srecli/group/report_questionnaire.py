import click

from srecli.group import (
    ContextObj,
    SREResponse,
    ViewCommand,
    build_job_id_option,
    build_tenant_option,
    cli_response,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='questionnaire')
def questionnaire():
    """Describes standard questionnaire reports"""


@questionnaire.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@click.option(
    '--standard',
    '-s',
    type=str,
    required=True,
    help='Full name of the standard, i.e. "CIS Controls v7"',
)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    standard: str,
    customer_id: str | None,
) -> SREResponse:
    """
    Returns a presigned url to a job standard questionnaire xlsx file
    """
    return ctx['api_client'].report_questionnaire_jobs(
        job_id=job_id,
        standard=standard,
        customer_id=customer_id,
    )


@questionnaire.command(cls=ViewCommand, name='accumulated')
@build_tenant_option(required=True)
@click.option(
    '--standard',
    '-s',
    type=str,
    required=True,
    help='Full name of the standard, i.e. "CIS Controls v7"',
)
@cli_response()
@presigned_url_hints_pack
def accumulated(
    ctx: ContextObj,
    tenant_name: str,
    standard: str,
    customer_id: str | None,
) -> SREResponse:
    """
    Returns a presigned url to a tenant standard questionnaire xlsx file
    """
    return ctx['api_client'].report_questionnaire_tenants(
        tenant_name=tenant_name,
        standard=standard,
        customer_id=customer_id,
    )

