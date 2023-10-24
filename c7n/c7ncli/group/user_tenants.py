import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj
from c7ncli.service.helpers import cast_to_list


@click.group(name='tenants')
def tenants():
    """Manages User-Tenant relations"""


@tenants.command(cls=ViewCommand, name='assign')
@click.option('--username', '-u', required=True, type=str,
              help='User to update')
@click.option('--tenant', '-t', type=str, multiple=True, required=True,
              help='Tenant name to assign to user. '
                   'Multiple names can be specified')
@cli_response()
def assign(ctx: ContextObj, username, tenant):
    """
    Assigns tenants to a user
    """
    return ctx['api_client'].user_assign_tenants(username=username,
                                                 tenants=tenant)


@tenants.command(cls=ViewCommand, name='unassign')
@click.option('--username', '-u', required=True, type=str,
              help='User to update')
@click.option('--tenant', '-t', type=str, multiple=True,
              help='Tenant names to unassign from user. '
                   'Multiple names can be specified')
@click.option('--all_tenants', '-ALL', is_flag=True, required=False,
              help='Remove all tenants from user. This will allow the user to '
                   'interact with all tenants within the customer.')
@cli_response()
def unassign(ctx: ContextObj, username, tenant, all_tenants=False):
    """
    Detaches tenants from a user
    """
    if not (bool(all_tenants) ^ bool(tenant)):
        return response(
            'You must specify either \'--tenant\' or '
            '\'--all_tenants\' parameter')

    return ctx['api_client'].user_unassign_tenants(
        username=username, tenants=tenant, all_tenants=all_tenants)


@tenants.command(cls=ViewCommand, name='describe')
@click.option('--username', '-u', required=True, type=str,
              help='The name of the user for whom the available '
                   'tenants information is to be provided')
@cli_response()
def describe(ctx: ContextObj, username):
    """
    Describes user-accessible tenants
    """
    return ctx['api_client'].user_describe_tenants(username=username)
