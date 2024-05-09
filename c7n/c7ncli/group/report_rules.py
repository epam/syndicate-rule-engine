from datetime import datetime
from typing import Optional

import click

from c7ncli.group import (
    build_job_id_option,
    build_job_type_option,
    optional_job_type_option,
)
from c7ncli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
    from_date_report_option,
    to_date_report_option,
)
from c7ncli.service.constants import JobType


@click.group(name='rules')
def rules():
    """Describes rule reports of jobs """


@rules.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@build_job_type_option(default=JobType.MANUAL.value, show_default=True)
@click.option('--href', '-hf', is_flag=True,
              help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(('json', 'xlsx')),
              default='json', show_default=True,
              help='Format of the file within the hypertext reference')
@cli_response()
def jobs(ctx: ContextObj, job_id: str, job_type: str,
         href: bool, format: str, customer_id):
    """
    Describes job-specific rule statistic reports
    """

    return ctx['api_client'].report_rules_get(
        job_id=job_id,
        job_type=job_type,
        href=href,
        format=format,
        customer_id=customer_id
    )


@rules.command(cls=ViewCommand, name='accumulated')
@build_tenant_option(required=True)
@from_date_report_option
@to_date_report_option
@optional_job_type_option
@cli_response()
def accumulated(ctx: ContextObj, tenant_name: str,
                from_date: Optional[datetime],
                to_date: Optional[datetime],
                job_type: Optional[str], customer_id):
    """
    Describes tenant-specific rule statistic reports, based on relevant jobs
    """

    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)
    return ctx['api_client'].report_rules_query(
        start_iso=from_date,
        end_iso=to_date,
        tenant_name=tenant_name,
        job_type=job_type,
        customer_id=customer_id
    )
