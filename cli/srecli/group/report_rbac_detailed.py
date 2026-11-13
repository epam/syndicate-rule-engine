import click

from srecli.group import (
    ContextObj,
    SREResponse,
    ViewCommand,
    build_job_id_option,
    cli_response,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='detailed')
def detailed():
    """Generate detailed Kubernetes RBAC reports"""


@detailed.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    customer_id,
) -> SREResponse:
    """
    Generate the "Who can do what" detailed RBAC report for a scan job as xlsx
    """
    return ctx['api_client'].report_rbac_job(
        job_id=job_id,
        customer_id=customer_id,
    )


@detailed.command(cls=ViewCommand, name='latest')
@click.option(
    '--platform_id', '-pid', type=str, required=True, help='Platform id'
)
@cli_response()
@presigned_url_hints_pack
def latest(
    ctx: ContextObj,
    platform_id: str,
    customer_id,
) -> SREResponse:
    """
    Generate the "Who can do what" detailed RBAC report for the latest scan
    job as xlsx
    """
    return ctx['api_client'].platform_report_rbac_latest(
        platform_id=platform_id,
        customer_id=customer_id,
    )
