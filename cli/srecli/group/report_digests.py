from datetime import datetime
from typing import Optional

import click

from srecli.group import (
    build_job_id_option,
    from_date_report_option,
    optional_job_type_option,
    to_date_report_option,
)
from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group import tenant_option


@click.group(name='digests')
def digests():
    """Describes summaries of job reports"""


@digests.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=False)
@optional_job_type_option
@tenant_option
@from_date_report_option
@to_date_report_option
@cli_response()
def jobs(
    ctx: ContextObj,
    job_id: Optional[str],
    tenant_name: Optional[str],
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    job_type: tuple[str, ...],
    customer_id: str | None,
):
    """
    Describes summary reports of jobs
    """
    if sum(map(bool, (job_id, tenant_name))) != 1:
        raise click.ClickException(
            'Either --job_id or --tenant_name must be given'
        )
    from_date = from_date.isoformat() if from_date else None
    to_date = to_date.isoformat() if to_date else None

    if job_id:
        return ctx['api_client'].report_digest_jobs(
            job_id=job_id,
            customer_id=customer_id
        )
    return ctx['api_client'].report_digest_tenants(
        tenant_name=tenant_name,
        job_types=job_type,
        start_iso=from_date,
        end_iso=to_date,
        customer_id=customer_id
    )
