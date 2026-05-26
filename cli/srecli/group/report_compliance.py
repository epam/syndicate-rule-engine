import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    build_tenant_option,
    cli_response,
    optional_job_type_option,
    SREResponse,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack

@click.group(name='compliance')
def compliance():
    """Describes compliance reports"""


@compliance.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@optional_job_type_option
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
    job_type: str,
    href: bool,
    format: str,
    customer_id,
) -> SREResponse:
    """
    Describes job compliance reports
    """
    return ctx['api_client'].report_compliance_jobs(
        job_id=job_id,
        job_type=job_type,
        href=href,
        format=format,
        customer_id=customer_id
    )


@compliance.command(cls=ViewCommand, name='accumulated')
@build_tenant_option(required=True)
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
def accumulated(
    ctx: ContextObj,
    tenant_name: str,
    href: bool,
    format: str,
    customer_id,
) -> SREResponse:
    """
    Describes tenant-specific compliance report
    """

    return ctx['api_client'].report_compliance_tenants(
        tenant_name=tenant_name,
        href=href,
        format=format,
        customer_id=customer_id
    )
