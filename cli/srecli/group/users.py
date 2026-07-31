import click

from srecli.group import ContextObj, ViewCommand, cli_response, limit_option, \
    next_option


@click.group(name='users')
def users():
    """Manages Rule Engine users. Only for system customer"""


@users.command(cls=ViewCommand, name='describe')
@click.option('--username', required=False, type=str,
              help='Username to describe a specific user')
@limit_option
@next_option
@cli_response()
def describe(ctx: ContextObj, username, limit, next_token, customer_id):
    """
    Describes users
    """
    if username:
        return ctx['api_client'].get_user(username)
    return ctx['api_client'].query_user(
        customer_id=customer_id,
        limit=limit,
        next_token=next_token
    )


@users.command(cls=ViewCommand, name='create')
@click.option('--username', required=True, type=str,
              help='Username to create user')
@click.option('--password', '-p', type=str,
              required=True, hide_input=True, prompt=True,
              help='New user password')
@click.option('--role_name', '-rn', type=str, required=True,
              help='Role to assign to this user. It should exist inside the '
                   'customer')
@click.option('--service_account', '-sa', type=click.BOOL, default=False,
              help='Whether the user is a service account allowed to use '
                   'MCP user-related headers. Allowed values: true, false. '
                   'Default: false')
@cli_response()
def create(ctx: ContextObj, username, password, role_name, customer_id,
           service_account):
    """
    Creates a new user
    """
    return ctx['api_client'].create_user(
        username=username,
        password=password,
        customer_id=customer_id,
        role_name=role_name,
        is_service_account=service_account,
    )


@users.command(cls=ViewCommand, name='update')
@click.option('--username', required=True, type=str,
              help='Username to create user')
@click.option('--password', '-p', type=str, help='New user password')
@click.option('--role_name', '-rn', type=str,
              help='Role to assign to this user. '
                   'It should exist inside the customer')
@click.option('--service_account', '-sa', type=click.BOOL, default=None,
              help='Whether the user is a service account allowed to use '
                   'MCP user-related headers. Allowed values: true, false')
@cli_response()
def update(
    ctx: ContextObj,
    username: str,
    customer_id: str,
    password: str | None,
    role_name: str | None,
    service_account: bool | None,
):
    """
    Updates some user's attributes
    """
    return ctx['api_client'].update_user(
        username=username,
        customer_id=customer_id,
        password=password,
        role_name=role_name,
        is_service_account=service_account,
    )


@users.command(cls=ViewCommand, name='delete')
@click.option('--username', required=True, type=str,
              help='Username to create user')
@cli_response()
def delete(ctx: ContextObj, username, customer_id):
    """
    Removes an existing user
    """
    return ctx['api_client'].delete_user(
        username=username,
        customer_id=customer_id
    )


@users.command(cls=ViewCommand, name='change_password')
@click.option('--password', '-p', type=str,
              required=True, hide_input=True, prompt=True,
              help='New password for your user')
@cli_response()
def change_password(ctx: ContextObj, password, customer_id):
    """
    Changes password for your user
    """
    return ctx['api_client'].reset_password(
        new_password=password,
        customer_id=customer_id
    )
