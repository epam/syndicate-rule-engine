from typing import Optional

import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    account_option,
    build_limit_option,
    build_tenant_option,
    cli_response,
    limit_option,
    next_option,
    tenant_option,
)
from srecli.service.constants import ModularCloud
from srecli.group.tenant_credentials import credentials


attributes_order = 'name', 'account_id', 'id_active', 'regions'


@click.group(name='tenant')
def tenant():
    """Manages Tenant Entity"""


@tenant.command(cls=ViewCommand, name='describe')
@tenant_option
@account_option
@click.option('--active', '-act', type=bool, required=False,
              help='Type of tenants to return')
@click.option('--cloud', '-c', type=click.Choice(tuple(ModularCloud.iter())),
              required=False, help='Specific cloud to describe tenants')
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, tenant_name, account_number, active, cloud,
             limit, next_token, customer_id):
    """
    Describes tenants within your customer
    """
    if tenant_name and account_number:
        raise click.ClickException(
            'Either --tenant_name or --account_number can be provided.'
        )
    if tenant_name or account_number:
        return ctx['api_client'].tenant_get(tenant_name or account_number,
                                            customer_id=customer_id)
    return ctx['api_client'].tenant_query(
        active=active,
        cloud=cloud,
        limit=limit,
        next_token=next_token,
        customer_id=customer_id
    )


@tenant.command(cls=ViewCommand, name='active_licenses')
@build_tenant_option(required=True)
@build_limit_option(default=1, show_default=True)
@cli_response()
def active_licenses(ctx: ContextObj, tenant_name: str, limit: Optional[int],
                    customer_id):
    """
    Get tenant active licenses
    """
    return ctx['api_client'].tenant_get_active_licenses(
        tenant_name,
        limit=limit,
        customer_id=customer_id
    )


@tenant.command(cls=ViewCommand, name='set_excluded_rules')
@build_tenant_option(required=True)
@click.option('--rules', '-r', type=str, multiple=True,
              help='Rules that you want to exclude for a tenant')
@click.option('--empty', is_flag=True, help='Whether to reset the '
                                            'list of excluded rules')
@cli_response()
def set_excluded_rules(ctx: ContextObj, tenant_name: str,
                       rules: tuple[str, ...], empty: bool,
                       customer_id):
    """
    Excludes rules for a tenant
    """
    if not rules and not empty:
        raise click.ClickException('Specify either --rules '' or --empty')
    if empty:
        rules = ()
    return ctx['api_client'].tenant_set_excluded_rules(
        tenant_name=tenant_name,
        rules=rules,
        customer_id=customer_id
    )


@tenant.command(cls=ViewCommand, name='get_excluded_rules')
@build_tenant_option(required=True)
@cli_response()
def get_excluded_rules(ctx: ContextObj, tenant_name: str, customer_id):
    """
    Returns excluded rules for a tenant
    """
    return ctx['api_client'].tenant_get_excluded_rules(
        tenant_name=tenant_name,
        customer_id=customer_id
    )


tenant.add_command(credentials)
