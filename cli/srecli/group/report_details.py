from datetime import datetime
from typing import Optional

import click

from srecli.group import (
    build_job_id_option,
    from_date_report_option,
    optional_job_type_option,
    response,
    to_date_report_option,
)
from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group import tenant_option


@click.group(name='details')
def details():
    """Describes detailed undigested reports"""


@details.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=False)
@optional_job_type_option
@tenant_option
@from_date_report_option
@to_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@cli_response()
def jobs(ctx: ContextObj, job_id: Optional[str], tenant_name: Optional[str],
         from_date: Optional[datetime], to_date: Optional[datetime],
         job_type: str, href: bool, obfuscated, customer_id):
    """
    Describes detailed reports of jobs
    """
    if sum(map(bool, (job_id, tenant_name))) != 1:
        return response('Either --job_id or --tenant_name must be given')
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    if job_id:
        return ctx['api_client'].report_details_jobs(
            job_id=job_id,
            job_type=job_type,
            href=href,
            customer_id=customer_id,
            obfuscated=obfuscated
        )
    return ctx['api_client'].report_details_tenants(
        tenant_name=tenant_name,
        job_type=job_type,
        href=href,
        start_iso=from_date,
        end_iso=to_date,
        customer_id=customer_id,
        obfuscated=obfuscated
    )
