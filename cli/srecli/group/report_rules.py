from datetime import datetime
from typing import Optional

import click

from srecli.group import (
    build_job_id_option,
    optional_job_type_option,
    SREResponse,
)
from srecli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
    from_date_report_option,
    to_date_report_option,
)
from srecli.service.constants import JobType
from srecli.service.presigned_hints_pack import presigned_url_hints_pack


@click.group(name='rules')
def rules():
    """Describes rule reports of jobs """


@rules.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@click.option(
    '--format',
    '-ft',
    type=click.Choice(('json', 'xlsx')),
    default='json',
    show_default=True,
    help='Format of the file behind the presigned reference',
)
@click.option(
    '--href',
    '-hf',
    is_flag=True,
    help='Return presigned URL instead of inline payload',
)
@cli_response()
@presigned_url_hints_pack
def jobs(
    ctx: ContextObj,
    job_id: str,
    href: bool,
    format: str,
    customer_id,
) -> SREResponse:
    """
    Describes job-specific rule statistic reports
    """

    return ctx['api_client'].report_rules_get(
        job_id=job_id,
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
def accumulated(
    ctx: ContextObj,
    tenant_name: str,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    job_type: tuple[str, ...],
    customer_id: str | None,
):
    """
    Describes tenant-specific rule statistic reports, based on relevant jobs
    """

    from_date = from_date.isoformat() if from_date else None
    to_date = to_date.isoformat() if to_date else None
    return ctx['api_client'].report_rules_query(
        start_iso=from_date,
        end_iso=to_date,
        tenant_name=tenant_name,
        job_types=job_type,
        customer_id=customer_id
    )
