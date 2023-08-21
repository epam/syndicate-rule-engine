from datetime import datetime
from typing import Optional

import click

from c7ncli.group import build_job_id_option, optional_job_type_option
from c7ncli.group import cli_response, ViewCommand, \
    tenant_option, customer_option, ContextObj, \
    from_date_report_option, to_date_report_option

optional_job_id_option = build_job_id_option(
    required=False,
    help='Unique job identifier. Required if neither `--to_date` or '
         '`--from_date` are set.'
)


@click.group(name='push')
def push():
    """Pushes Custodian Service job reports to SIEMs"""


@push.command(cls=ViewCommand, name='dojo')
@optional_job_id_option
@optional_job_type_option
@from_date_report_option
@to_date_report_option
@customer_option
@tenant_option
@cli_response()
def dojo(
        ctx: ContextObj, job_id: Optional[str], job_type: Optional[str],
        from_date: Optional[datetime], to_date: Optional[datetime],
        customer_id: Optional[str], tenant_name: Optional[str]
):
    """
    Pushes job detailed report(s) to the Dojo SIEM
    """
    if job_id:
        return ctx['api_client'].push_dojo_by_job_id(job_id=job_id)
    if not tenant_name:
        raise click.UsageError("Missing option '--tenant_name' / '-tn'.")
    return ctx['api_client'].push_dojo_multiple(
        start_date=from_date.isoformat() if from_date else None,
        end_date=to_date.isoformat() if to_date else None,
        customer=customer_id,
        tenant_name=tenant_name,
        job_type=job_type
    )


@push.command(cls=ViewCommand, name='security_hub')
@optional_job_id_option
@optional_job_type_option
@from_date_report_option
@to_date_report_option
@customer_option
@tenant_option
@click.option('--aws_access_key', '-ak', type=str,
              help='AWS Account access key')
@click.option('--aws_secret_access_key', '-sk', type=str,
              help='AWS Account secret access key')
@click.option('--aws_session_token', '-st', type=str,
              help='AWS Account session token')
@click.option('--aws_default_region', '-df', type=str, default='eu-central-1',
              show_default=True,
              help='AWS Account default region to init a client ')
@cli_response(attributes_order=[])
def security_hub(
        ctx: ContextObj, job_id: Optional[str], job_type: Optional[str],
        from_date: Optional[datetime], to_date: Optional[datetime],
        customer_id: Optional[str], tenant_name: Optional[str],
        aws_access_key, aws_secret_access_key, aws_session_token,
        aws_default_region
):
    """
    Pushes job detailed report(s) to the AWS Security Hub SIEM
    """
    if job_id:
        return ctx['api_client'].push_security_hub_by_job_id(job_id)
    if not tenant_name:
        raise click.UsageError("Missing option '--tenant_name' / '-tn'.")
    return ctx['api_client'].push_security_hub_multiple(
        start_date=from_date.isoformat() if from_date else None,
        end_date=to_date.isoformat() if to_date else None,
        customer=customer_id,
        job_type=job_type,
        tenant_name=tenant_name,
    )
