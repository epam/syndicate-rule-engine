from datetime import datetime
from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, \
    build_tenant_option, from_date_report_option, to_date_report_option, \
    optional_job_type_option, build_job_id_option


@click.group(name='resource')
def resource():
    """Describes compliance reports"""


@resource.command(cls=ViewCommand, name='latest')
@build_tenant_option(required=True)
@click.option('--resource_id', '-rid', required=True, type=str,
              help='Resource identifier (arn or name or id)')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by', '-sb', type=str, multiple=True,
              help='Attributes to search the identifier by '
                   '(arn, name, id, etc..). If not specified, report '
                   'fields from rule meta will be used')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by')
@cli_response()
def latest(ctx: ContextObj, tenant_name: str, resource_id: str,
           exact_match: bool,
           search_by: tuple, search_by_all: bool,
           resource_type: Optional[str], region: Optional[str]):
    """
    Describes job compliance reports
    """
    return ctx['api_client'].report_resource_latest(
        tenant_name=tenant_name,
        identifier=resource_id,
        exact_match=exact_match,
        search_by=','.join(search_by) if search_by else None,
        search_by_all=search_by_all,
        resource_type=resource_type,
        region=region,
    )


@resource.command(cls=ViewCommand, name='jobs')
@build_tenant_option(required=True)
@optional_job_type_option
@from_date_report_option
@to_date_report_option
@click.option('--resource_id', '-rid', required=True, type=str,
              help='Resource identifier (arn or name or id)')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by', '-sb', type=str, multiple=True,
              help='Attributes to search the identifier by '
                   '(arn, name, id, etc..). If not specified, report '
                   'fields from rule meta will be used')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by')
@cli_response()
def jobs(ctx: ContextObj, tenant_name: str, job_type: str,
         from_date: datetime, to_date: datetime, resource_id: str,
         exact_match: bool,
         search_by: tuple, search_by_all: bool,
         resource_type: Optional[str], region: Optional[str], ):
    """
    Describes job compliance reports
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)
    return ctx['api_client'].report_resource_jobs(
        tenant_name=tenant_name,
        type=job_type,
        start_iso=from_date,
        end_iso=to_date,
        identifier=resource_id,
        exact_match=exact_match,
        search_by=','.join(search_by) if search_by else None,
        search_by_all=search_by_all,
        resource_type=resource_type,
        region=region,
    )


@resource.command(cls=ViewCommand, name='job')
@build_job_id_option(required=True)
@optional_job_type_option
@click.option('--resource_id', '-rid', required=True, type=str,
              help='Resource identifier (arn or name or id)')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by', '-sb', type=str, multiple=True,
              help='Attributes to search the identifier by '
                   '(arn, name, id, etc..). If not specified, report '
                   'fields from rule meta will be used')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by')
@cli_response()
def job(ctx: ContextObj, job_id: str, job_type: str, resource_id: str,
        exact_match: bool, search_by: tuple, search_by_all: bool,
        resource_type: Optional[str], region: Optional[str], ):
    """
    Describes job compliance reports
    """
    return ctx['api_client'].report_resource_job(
        id=job_id,
        type=job_type,
        identifier=resource_id,
        exact_match=exact_match,
        search_by=','.join(search_by) if search_by else None,
        search_by_all=search_by_all,
        resource_type=resource_type,
        region=region,
    )
