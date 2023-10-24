import click
from typing import Optional

from c7ncli.group import cli_response, ViewCommand, limit_option, \
    next_option, ContextObj, tenant_option, build_tenant_option, \
    account_option, build_account_option, customer_option
from c7ncli.group.tenant_findings import findings
from c7ncli.group.tenant_region import region_group
from c7ncli.group.tenant_credentials import credentials
from c7ncli.service.constants import PARAM_CUSTOMER_DISPLAY_NAME, \
    PARAM_ACTIVATION_DATE, PARAM_INHERIT, PARAM_NAME, AWS, GOOGLE, AZURE


@click.group(name='tenant')
def tenant():
    """Manages Tenant Entity"""


@tenant.command(cls=ViewCommand, name='describe')
@tenant_option
@account_option
@customer_option
@click.option('--full', '-f', is_flag=True,
              help='Show full command output.')
@limit_option
@next_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER_DISPLAY_NAME,
                                PARAM_ACTIVATION_DATE, PARAM_INHERIT])
def describe(ctx: ContextObj, tenant_name, customer_id, account_number,
             full, limit, next_token):
    """
    Describes your user's tenant(s)
    """
    return ctx['api_client'].tenant_get(
        tenant_name=tenant_name,
        customer_name=customer_id,
        cloud_identifier=account_number,
        complete=full,
        limit=limit,
        next_token=next_token
    )


@tenant.command(cls=ViewCommand, name='create')
@build_tenant_option(required=True, help='Name of the tenant')
@click.option('--display_name', '-dn', type=str,
              help='Tenant display name. If not specified, the '
                   'value from --name is used')
@click.option('--cloud', '-c', type=click.Choice([AWS, AZURE, GOOGLE]),
              required=True, help='Cloud of the tenant')
@build_account_option(required=True)
@click.option('--primary_contacts', '-pc', type=str, multiple=True,
              help='Primary emails')
@click.option('--secondary_contacts', '-sc', type=str, multiple=True,
              help='Secondary emails')
@click.option('--tenant_manager_contacts', '-tmc', type=str, multiple=True,
              help='Tenant manager emails')
@click.option('--default_owner', '-do', type=str,
              help='Owner email')
@cli_response()
def create(ctx: ContextObj, tenant_name: str, display_name: Optional[str], cloud: str,
           account_number: str, primary_contacts: tuple,
           secondary_contacts: tuple, tenant_manager_contacts: tuple,
           default_owner: Optional[str]):
    """
    Activates a tenant, if the environment does not restrict it.
    """
    return ctx['api_client'].tenant_post(
        name=tenant_name,
        display_name=display_name,
        cloud=cloud,
        cloud_identifier=account_number,
        primary_contacts=list(primary_contacts),
        secondary_contacts=list(secondary_contacts),
        tenant_manager_contacts=list(tenant_manager_contacts),
        default_owner=default_owner
    )


@tenant.command(cls=ViewCommand, name='update')
@tenant_option
@click.option('--rules_to_exclude', '-rte', type=str, multiple=True,
              help='Rules to exclude for tenant')
@click.option('--rules_to_include', '-rti', type=str, multiple=True,
              help='Rules to include for tenant')
# @click.option('--send_scan_result', '-ssr', type=bool,
#               help='Specify whether to send scan results. Obsolete')
@cli_response()
def update(ctx: ContextObj, tenant_name: Optional[str],
           rules_to_exclude: tuple,
           rules_to_include: tuple):
    """
    Updates settings of your user's tenant
    """
    return ctx['api_client'].tenant_patch(
        tenant_name=tenant_name,
        rules_to_exclude=list(rules_to_exclude),
        rules_to_include=list(rules_to_include),
    )


tenant.add_command(findings)
tenant.add_command(region_group)
tenant.add_command(credentials)
