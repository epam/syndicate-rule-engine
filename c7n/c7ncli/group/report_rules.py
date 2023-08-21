from datetime import datetime
from typing import Optional

import click

from c7ncli.group import build_job_id_option, build_job_type_option, optional_job_type_option
from c7ncli.group import cli_response, ViewCommand, \
    tenant_option, customer_option, ContextObj, \
    from_date_report_option, to_date_report_option
from c7ncli.service.constants import MANUAL_JOB_TYPE

required_job_id_option = build_job_id_option(required=True)
default_job_type_option = build_job_type_option(default=MANUAL_JOB_TYPE, show_default=True)
AVAILABLE_ERROR_FORMATS = ['json', 'xlsx']


@click.group(name='rules')
def rules():
    """Describes rule reports of jobs """


@rules.command(cls=ViewCommand, name='jobs')
@required_job_id_option
@default_job_type_option
@click.option('--rule', '-rl', type=str, help='Denotes rule to target')
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def jobs(ctx: ContextObj, job_id: str, job_type: str,
         rule: str, href: bool, format: str):
    """
    Describes job-specific rule statistic reports
    """

    return ctx['api_client'].report_rules_get(
        job_id=job_id, job_type=job_type, href=href,
        frmt=format, jobs=True, target_rule=rule
    )


@rules.command(cls=ViewCommand, name='accumulated')
@tenant_option
@customer_option
@from_date_report_option
@to_date_report_option
@optional_job_type_option
@click.option('--rule', '-rl', type=str, help='Denotes rule to target')
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@click.option('--format', '-ft', type=click.Choice(AVAILABLE_ERROR_FORMATS),
              help='Format of the file within the hypertext reference')
@cli_response(attributes_order=[])
def accumulated(ctx: ContextObj, tenant_name: Optional[str],
                customer_id: Optional[str], from_date: Optional[datetime],
                to_date: datetime, job_type: str,
                rule: str, href: bool, format: str):
    """
    Describes tenant-specific rule statistic reports, based on relevant jobs
    """

    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)
    kwargs = dict(
        start_date=from_date, end_date=to_date,
        job_type=job_type, href=href, jobs=False,
        target_rule=rule, frmt=format
    )

    if tenant_name:
        return ctx['api_client'].report_rules_get(
            tenant_name=tenant_name, **kwargs
        )
    else:
        return ctx['api_client'].report_rules_query(
            customer=customer_id, **kwargs
        )
