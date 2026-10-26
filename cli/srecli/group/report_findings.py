from datetime import datetime
from typing import Optional

import click

from srecli.group import (
    ContextObj,
    SREResponse,
    ViewCommand,
    build_job_id_option,
    cli_response,
    from_date_report_option,
    optional_job_type_option,
    tenant_option,
    to_date_report_option,
)
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='findings')
def findings():
    """Describes findings reports"""


@findings.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=False)
@optional_job_type_option
@tenant_option
@from_date_report_option
@to_date_report_option
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option(
    '--href',
    '-hf',
    is_flag=True,
    help='Return presigned URL instead of inline payload.',
)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: Optional[str],
    tenant_name: Optional[str],
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    job_types: tuple[str, ...],
    href: bool,
    customer_id: str | None,
    obfuscated: bool,
) -> SREResponse:
    """
    Describes detailed reports of jobs
    """
    if sum(map(bool, (job_id, tenant_name))) != 1:
        raise click.ClickException(
            'Either --job_id or --tenant_name must be given'
        )

    from_date = from_date.isoformat() if from_date else None
    to_date = to_date.isoformat() if to_date else None

    if job_id:
        return ctx['api_client'].report_findings_jobs(
            job_id=job_id,
            href=href,
            customer_id=customer_id,
            obfuscated=obfuscated
        )
    return ctx['api_client'].report_findings_tenants(
        tenant_name=tenant_name,
        job_types=job_types,
        href=href,
        start_iso=from_date,
        end_iso=to_date,
        customer_id=customer_id,
        obfuscated=obfuscated
    )


@findings.command(cls=ViewCommand, name='k8s_control')
@build_job_id_option(required=True)
@click.option('--format', '-ft', 'report_format',
              type=click.Choice(('md', 'json')), default='md',
              show_default=True, help='Format of the report')
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return '
                   'also a dictionary')
@click.option('--href', '-hf', is_flag=True,
              help='Return presigned URL instead of inline JSON. '
                   'Markdown always returns a URL')
@cli_response()
@presigned_url_hints_pack
def k8s_control(
    ctx: ContextObj,
    job_id: str,
    report_format: str,
    obfuscated: bool,
    href: bool,
    customer_id: str | None,
) -> SREResponse:
    """Describes detailed reports of Kubernetes Control Plane"""
    return ctx['api_client'].report_findings_control_plane_job(
        job_id=job_id,
        format=report_format,
        href=href,
        obfuscated=obfuscated,
        customer_id=customer_id,
    )
