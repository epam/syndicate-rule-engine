from datetime import datetime
from typing import Optional

import click

from srecli.group import (
    build_job_id_option,
    from_date_report_option,
    optional_job_type_option,
    to_date_report_option,
    SREResponse,
)
from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group import tenant_option
from srecli.service.presigned_hints_pack import presigned_url_hints_pack

@click.group(name='details')
def details():
    """Describes detailed undigested reports"""


@details.command(cls=ViewCommand, name='jobs')
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
    job_type: tuple[str, ...],
    href: bool,
    obfuscated,
    customer_id,
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
        return ctx['api_client'].report_details_jobs(
            job_id=job_id,
            href=href,
            customer_id=customer_id,
            obfuscated=obfuscated
        )
    return ctx['api_client'].report_details_tenants(
        tenant_name=tenant_name,
        job_types=job_type,
        href=href,
        start_iso=from_date,
        end_iso=to_date,
        customer_id=customer_id,
        obfuscated=obfuscated
    )
