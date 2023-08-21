from datetime import datetime
from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, ContextObj
from c7ncli.group import tenant_option, customer_option
from c7ncli.group import from_date_report_option, \
    to_date_report_option, optional_job_type_option

AVAILABLE_ERROR_FORMATS = ['json', 'xlsx']


@click.group(name='accumulated')
def accumulated():
    """Describes tenant-specific error reports, based on relevant jobs"""


@accumulated.command(cls=ViewCommand, name='total')
@optional_job_type_option
@tenant_option
@customer_option
@to_date_report_option
@from_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def total(ctx: ContextObj, tenant_name: Optional[str], customer_id: Optional[str],
          from_date: Optional[datetime], to_date: Optional[datetime], job_type: str,
          href: bool, format: str):
    """
    Describes job error reports
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    return ctx['api_client'].report_errors_query(
        end_date=to_date, start_date=from_date,
        tenant_name=tenant_name,
        job_type=job_type, href=href, frmt=format, subtype=None,
        customer=customer_id
    )


@accumulated.command(cls=ViewCommand, name='access')
@optional_job_type_option
@tenant_option
@customer_option
@to_date_report_option
@from_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def access(ctx: ContextObj, tenant_name: Optional[str], customer_id: Optional[str],
           from_date: Optional[datetime], to_date: Optional[datetime],
           job_type: str, href: bool, format: str):
    """
    Describes access-related job error reports
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    return ctx['api_client'].report_errors_query(
        end_date=to_date, start_date=from_date,
        tenant_name=tenant_name,
        job_type=job_type, href=href, frmt=format, subtype='access',
        customer=customer_id
    )


@accumulated.command(cls=ViewCommand, name='core')
@optional_job_type_option
@tenant_option
@customer_option
@to_date_report_option
@from_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def core(ctx: ContextObj, tenant_name: Optional[str], customer_id: Optional[str],
         from_date: Optional[datetime], to_date: Optional[datetime], job_type: str,
         href: bool, format: str):
    """
    Describes core-related error reports
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    return ctx['api_client'].report_errors_query(
        end_date=to_date, start_date=from_date,
        tenant_name=tenant_name, job_type=job_type, href=href, frmt=format,
        subtype='core', customer=customer_id
    )
