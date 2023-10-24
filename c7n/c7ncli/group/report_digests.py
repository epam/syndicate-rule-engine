from datetime import datetime
from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, ContextObj
from c7ncli.group import tenant_option, customer_option
from c7ncli.group import build_job_id_option, \
    from_date_report_option, to_date_report_option,\
    optional_job_type_option


@click.group(name='digests')
def digests():
    """Describes summaries of job reports"""


@digests.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=False)
@optional_job_type_option
@tenant_option
@customer_option
@from_date_report_option
@to_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@cli_response(attributes_order=[])
def jobs(ctx: ContextObj,
         job_id: Optional[str],
         tenant_name: Optional[str], customer_id: Optional[str],
         from_date: Optional[datetime], to_date: Optional[datetime],
         job_type: str, href: bool):
    """
    Describes summary reports of jobs
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    kwargs = dict(
        start_date=from_date, end_date=to_date,
        job_type=job_type, href=href, jobs=True
    )
    if tenant_name or job_id:
        return ctx['api_client'].report_digests_get(
            job_id=job_id, tenant_name=tenant_name, **kwargs
        )
    else:
        return ctx['api_client'].report_digests_query(
            customer=customer_id, **kwargs
        )


@digests.command(cls=ViewCommand, name='accumulated')
@tenant_option
@optional_job_type_option
@customer_option
@from_date_report_option
@to_date_report_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@cli_response(attributes_order=[])
def accumulated(ctx: ContextObj, tenant_name: Optional[str],
                customer_id: Optional[str], from_date: Optional[datetime],
                to_date: Optional[datetime], job_type: str, href: bool):
    """
    Describes tenant-specific summary reports, based on relevant jobs
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)

    kwargs = dict(
        start_date=from_date, end_date=to_date,
        job_type=job_type, href=href, jobs=False
    )
    if tenant_name:
        return ctx['api_client'].report_digests_get(
            tenant_name=tenant_name, **kwargs
        )
    else:
        return ctx['api_client'].report_digests_query(
            customer=customer_id, **kwargs
        )
