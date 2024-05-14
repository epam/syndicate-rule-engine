from datetime import datetime
from typing import Optional

import click

from c7ncli.group import (
    ContextObj,
    ViewCommand,
    build_job_id_option,
    build_tenant_option,
    cli_response,
    from_date_report_option,
    optional_job_type_option,
    to_date_report_option,
)


@click.group(name='resource')
def resource():
    """Generate resource reports"""


@resource.command(cls=ViewCommand, name='latest')
@build_tenant_option(required=True)
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by. Specify "global" '
                   'for region-independent resources')
@click.option('--full', '-f', is_flag=True,
              help='Whether to return full resource data')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--format', '-ft', type=click.Choice(('json', 'xlsx')),
              help='Report format')
@click.option('--href', '-hf', is_flag=True,
              help='Whether to return raw data of url to the data')
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option('--id', required=False, type=str,
              help='Resource identifier')
@click.option('--name', required=False, type=str,
              help='Resource name')
@click.option('--arn', required=False, type=str,
              help='Resource arn, only for aws')
@cli_response()
def latest(ctx: ContextObj, tenant_name: str, resource_type: Optional[str],
           region: Optional[str], full: bool, exact_match: bool,
           search_by_all: bool, format: str, href: bool, id: Optional[str],
           name: Optional[str], arn: Optional[str], customer_id, obfuscated):
    """
    Resource report for tenant
    """
    return ctx['api_client'].report_resource_latest(
        tenant_name=tenant_name,
        resource_type=resource_type,
        region=region,
        full=full,
        exact_match=exact_match,
        search_by_all=search_by_all,
        format=format,
        href=href,
        obfuscated=obfuscated,
        id=id,
        name=name,
        arn=arn,
        customer_id=customer_id
    )


@resource.command(cls=ViewCommand, name='platform_latest')
@click.option('--platform_id', '-pid', type=str, required=True,
              help='Platform id')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--full', '-f', is_flag=True,
              help='Whether to return full resource data')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--format', '-ft', type=click.Choice(('json', 'xlsx')),
              help='Report format')
@click.option('--href', '-hf', is_flag=True,
              help='Whether to return raw data of url to the data')
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option('--id', required=False, type=str,
              help='Resource identifier')
@click.option('--name', required=False, type=str,
              help='Resource name')
@cli_response()
def platform_latest(ctx: ContextObj, platform_id: str,
                    resource_type: Optional[str], full: bool,
                    exact_match: bool, search_by_all: bool,
                    format: str, href: bool, id: Optional[str],
                    name: Optional[str], customer_id, obfuscated):
    """
    Resource report for platform
    """
    return ctx['api_client'].platform_report_resource_latest(
        platform_id=platform_id,
        resource_type=resource_type,
        full=full,
        exact_match=exact_match,
        search_by_all=search_by_all,
        format=format,
        href=href,
        id=id,
        name=name,
        customer_id=customer_id,
        obfuscated=obfuscated
    )


@resource.command(cls=ViewCommand, name='jobs')
@build_tenant_option(required=True)
@optional_job_type_option
@from_date_report_option
@to_date_report_option
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by. Specify "global" '
                   'for region-independent resources')
@click.option('--full', '-f', is_flag=True,
              help='Whether to return full resource data')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--id', required=False, type=str,
              help='Resource identifier')
@click.option('--name', required=False, type=str,
              help='Resource name')
@click.option('--arn', required=False, type=str,
              help='Resource arn, only for aws')
@cli_response()
def jobs(ctx: ContextObj, tenant_name: str, job_type: str,
         from_date: datetime, to_date: datetime, resource_type: Optional[str],
         region: Optional[str], full: bool, exact_match: bool,
         search_by_all: bool, id: Optional[str], name: Optional[str],
         arn: Optional[str], customer_id):
    """
    Resource report for tenant jobs
    """
    dates = from_date, to_date
    i_iso = map(lambda d: d.isoformat() if d else None, dates)
    from_date, to_date = tuple(i_iso)
    return ctx['api_client'].report_resource_jobs(
        tenant_name=tenant_name,
        job_type=job_type,
        start_iso=from_date,
        end_iso=to_date,
        resource_type=resource_type,
        region=region,
        full=full,
        exact_match=exact_match,
        search_by_all=search_by_all,
        id=id,
        name=name,
        arn=arn,
        customer_id=customer_id
    )


@resource.command(cls=ViewCommand, name='job')
@build_job_id_option(required=True)
@optional_job_type_option
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the result (lambda, s3, ...)')
@click.option('--region', '-r', type=str,
              help='Region to filter the result by. Specify "global" '
                   'for region-independent resources')
@click.option('--full', '-f', is_flag=True,
              help='Whether to return full resource data')
@click.option('--exact_match', '-em', type=bool, default=True,
              show_default=True,
              help='Whether to match the identifier exactly or allow '
                   'partial match')
@click.option('--search_by_all', '-sba', is_flag=True,
              help='If specified, all the fields will be checked')
@click.option('--href', '-hf', is_flag=True,
              help='Whether to return raw data of url to the data')
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option('--id', required=False, type=str,
              help='Resource identifier')
@click.option('--name', required=False, type=str,
              help='Resource name')
@click.option('--arn', required=False, type=str,
              help='Resource arn, only for aws')
@cli_response()
def job(ctx: ContextObj, job_id: str, job_type: str,
        resource_type: Optional[str], region: Optional[str], full: bool,
        exact_match: bool, search_by_all: bool, id: Optional[str],
        name: Optional[str], arn: Optional[str], href: bool, customer_id,
        obfuscated):
    """
    Resource report for concrete job
    """
    return ctx['api_client'].report_resource_job(
        job_id=job_id,
        type=job_type,
        resource_type=resource_type,
        region=region,
        full=full,
        exact_match=exact_match,
        search_by_all=search_by_all,
        id=id,
        name=name,
        arn=arn,
        customer_id=customer_id,
        href=href,
        obfuscated=obfuscated
    )
