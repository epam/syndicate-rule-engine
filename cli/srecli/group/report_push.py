from datetime import datetime
from typing import Optional

import click

from srecli.group import build_job_id_option, optional_job_type_option
from srecli.group import (
    ContextObj,
    ViewCommand,
    cli_response,
    from_date_report_option,
    tenant_option,
    to_date_report_option,
)

optional_job_id_option = build_job_id_option(
    required=False,
    help='Unique job identifier. Required if neither `--to_date` or '
         '`--from_date` are set.'
)


@click.group(name='push')
def push():
    """Pushes job reports to SIEMs"""


@push.command(cls=ViewCommand, name='dojo')
@optional_job_id_option
@optional_job_type_option
@from_date_report_option
@to_date_report_option
@tenant_option
@cli_response()
def dojo(ctx: ContextObj, job_id: Optional[str], job_type: Optional[str],
         from_date: Optional[datetime], to_date: Optional[datetime],
         customer_id: Optional[str], tenant_name: Optional[str]):
    """
    Pushes job detailed report(s) to the Dojo SIEM
    """
    if job_id:
        return ctx['api_client'].push_dojo_by_job_id(job_id=job_id,
                                                     customer_id=customer_id)
    if not tenant_name:
        raise click.UsageError("Missing option '--tenant_name' / '-tn'.")
    return ctx['api_client'].push_dojo_multiple(
        start_date=from_date.isoformat() if from_date else None,
        end_date=to_date.isoformat() if to_date else None,
        customer_id=customer_id,
        tenant_name=tenant_name,
        job_type=job_type
    )


@push.command(cls=ViewCommand, name='chronicle')
@build_job_id_option(required=True, help='Job id to push')
@optional_job_type_option
@cli_response()
def chronicle(ctx: ContextObj, job_id: Optional[str], job_type: Optional[str],
              customer_id: Optional[str]):
    """
    Pushes job detailed report(s) to the Google Chronicle
    """
    return ctx['api_client'].push_chronicle_by_job_id(
        job_id=job_id,
        customer_id=customer_id,
        job_type=job_type
    )
