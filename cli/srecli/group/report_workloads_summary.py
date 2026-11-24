import click

from srecli.group import (
    ContextObj,
    SREResponse,
    ViewCommand,
    build_job_id_option,
    cli_response,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='summary')
def summary():
    """Generate summary Kubernetes workloads reports: unique workloads"""


@summary.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@click.option(
    '--rule',
    '-r',
    type=str,
    help='Pod rule name or its short id (e.g. ecc-k8s-066)',
)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    rule: str | None,
    customer_id,
) -> SREResponse:
    """
    Generate the summary Kubernetes workloads report for a scan job as xlsx
    """
    return ctx['api_client'].report_workloads_summary_job(
        job_id=job_id,
        rule=rule,
        customer_id=customer_id,
    )


@summary.command(cls=ViewCommand, name='latest')
@click.option(
    '--platform_id', '-pid', type=str, required=True, help='Platform id'
)
@click.option(
    '--rule',
    '-r',
    type=str,
    help='Pod rule name or its short id (e.g. ecc-k8s-066)',
)
@cli_response()
@presigned_url_hints_pack
def latest(
    ctx: ContextObj,
    platform_id: str,
    rule: str | None,
    customer_id,
) -> SREResponse:
    """
    Generate the summary Kubernetes workloads report for the latest scan
    job as xlsx
    """
    return ctx['api_client'].platform_report_workloads_summary_latest(
        platform_id=platform_id,
        rule=rule,
        customer_id=customer_id,
    )
