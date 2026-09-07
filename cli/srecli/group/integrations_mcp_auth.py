import click

from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name='auth')
def auth():
    """
    Manages MCP JWT auth configuration
    """


@auth.command(cls=ViewCommand, name='describe')
@cli_response()
def describe(ctx: ContextObj, customer_id):
    """
    Describes current MCP JWT auth configuration
    """
    return ctx['api_client'].mcp_auth_setting_get(customer_id=customer_id)


@auth.command(cls=ViewCommand, name='add')
@click.option(
    '--jwt',
    '-j',
    type=str,
    required=True,
    hide_input=True,
    prompt=True,
    help='JWT verification key. If not provided, will be prompted for '
         'securely',
)
@click.option(
    '--algorithm',
    '-alg',
    type=str,
    default='RS256',
    show_default=True,
    help='JWT signing algorithm',
)
@cli_response()
def add(ctx: ContextObj, jwt: str, algorithm: str, customer_id):
    """
    Adds MCP JWT auth configuration. Only one configuration is allowed.
    """
    return ctx['api_client'].mcp_auth_setting_post(
        jwt=jwt,
        algorithm=algorithm,
        customer_id=customer_id,
    )


@auth.command(cls=ViewCommand, name='update')
@click.option(
    '--jwt',
    '-j',
    type=str,
    required=False,
    help='JWT verification key. Use --prompt_jwt instead of this option '
         'to enter the key securely without exposing it in the command',
)
@click.option(
    '--prompt_jwt',
    '-pj',
    is_flag=True,
    default=False,
    help='Prompt for the JWT verification key securely (input hidden) '
         'instead of passing it via --jwt',
)
@click.option(
    '--algorithm',
    '-alg',
    type=str,
    required=False,
    help='JWT signing algorithm',
)
@cli_response()
def update(
    ctx: ContextObj,
    jwt: str | None,
    prompt_jwt: bool,
    algorithm: str | None,
    customer_id,
):
    """
    Updates existing MCP JWT auth configuration
    """
    if jwt and prompt_jwt:
        raise click.ClickException(
            'Provide either --jwt or --prompt_jwt, not both'
        )
    if prompt_jwt:
        jwt = click.prompt('JWT verification key', hide_input=True)
    if jwt is None and algorithm is None:
        raise click.ClickException(
            'Provide at least one of --jwt or --algorithm'
        )
    return ctx['api_client'].mcp_auth_setting_patch(
        jwt=jwt,
        algorithm=algorithm,
        customer_id=customer_id,
    )


@auth.command(cls=ViewCommand, name='delete')
@cli_response()
def delete(ctx: ContextObj, customer_id):
    """
    Removes current MCP JWT auth configuration
    """
    return ctx['api_client'].mcp_auth_setting_delete(customer_id=customer_id)


auth.add_command(describe)
auth.add_command(add)
auth.add_command(update)
auth.add_command(delete)
