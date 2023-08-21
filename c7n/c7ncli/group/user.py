import os

import click

from c7ncli.group import ViewCommand, cli_response, ContextObj, \
    build_customer_option
from c7ncli.group.user_tenants import tenants
from c7ncli.service.constants import C7NCLI_DEVELOPER_MODE_ENV_NAME
from c7ncli.service.helpers import gen_password
from c7ncli.service.logger import get_logger

_LOG = get_logger(__name__)


@click.group(name='user')
def user():
    """
    Manages User Entity
    """


@click.option('--username', '-u', required=False, type=str,
              help='User`s name to delete. Only for SYSTEM. ALl other users '
                   'can delete only themselves')
@cli_response()
def delete(ctx: ContextObj, username):
    """
    Delete the current user. You do not want to use the action!
    """
    return ctx['api_client'].user_delete(username=username)

# TODO add command for password-reset


@user.command(cls=ViewCommand, name='signup')
@click.option('--username', '-u', required=True, type=str,
              help='User`s username. Must be unique')
@click.option('--password', '-p', required=False, type=str,
              help='User`s password', show_default=True)
@build_customer_option(required=True)
@click.option('--role', '-r', required=True, type=str,
              help='User`s role')
@click.option('--tenants', '-t', 'tenants_', required=False, multiple=True,
              type=str,
              help='Tenants within the customer the user need access to')
@cli_response(secured_params=['password'])
def signup(ctx: ContextObj, username, password, customer_id, role, tenants_):
    """
    Signs up a new Cognito user
    """
    if not password:
        _LOG.debug('Password was not given, generating')
        password = gen_password()
        click.echo(f'Password: {password}')
    return ctx['api_client'].signup(
        username=username,
        password=password,
        customer=customer_id,
        role=role,
        tenants=list(tenants_)
    )


if str(os.getenv(C7NCLI_DEVELOPER_MODE_ENV_NAME)).lower() == 'true':
    user.command(cls=ViewCommand, name='delete')(delete)


user.add_command(tenants)
