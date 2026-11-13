import click

from srecli.group import (
    ContextObj,
    SREResponse,
    ViewCommand,
    build_job_id_option,
    cli_response,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='compact')
def compact():
    """Generate compact Kubernetes RBAC Single Pane of Glass reports"""


@compact.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    customer_id,
) -> SREResponse:
    """
    Generate a Kubernetes RBAC single-pane-of-glass report for a scan job
    as xlsx
    """
    return ctx['api_client'].report_rbac_single_pane_job(
        job_id=job_id,
        customer_id=customer_id,
    )


@compact.command(cls=ViewCommand, name='latest')
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
    Generate a Kubernetes RBAC single-pane-of-glass report for the latest
    scan job as xlsx
    """
    return ctx['api_client'].platform_report_rbac_single_pane_latest(
        platform_id=platform_id,
        customer_id=customer_id,
    )
