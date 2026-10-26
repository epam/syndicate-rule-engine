import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    build_tenant_option,
    cli_response,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='dashboard')
def dashboard():
    """Describes dashboard reports"""


@dashboard.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=False)
@build_tenant_option(required=False)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str | None,
    tenant_name: str | None,
    customer_id: str | None,
) -> SREResponse:
    """Generate an Excel dashboard from a scan job or tenant."""
    fields = (job_id, tenant_name)
    if not any(fields) or all(fields):
        raise click.ClickException(
            'Specify exactly one of --job_id or --tenant_name'
        )

    if job_id:
        return ctx['api_client'].report_dashboard_job(
            job_id=job_id, customer_id=customer_id
        )
    return ctx['api_client'].report_dashboard_tenant(
        tenant_name=tenant_name, customer_id=customer_id
    )
