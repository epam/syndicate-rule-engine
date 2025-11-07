from datetime import datetime

import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    DYNAMIC_DATE_EXAMPLE,
)

attributes_order = 'name', 'expiration', 'policies'


@click.group(name='role')
def role():
    """Manages Role Entity"""


@role.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, help='Role name to describe.')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id, name):
    """
    Describes roles for the given customer.
    """
    if name:
        return ctx['api_client'].role_get(
            name=name,
            customer_id=customer_id,
        )
    return ctx['api_client'].role_query(
        customer_id=customer_id,
    )


@role.command(cls=ViewCommand, name='add')
@click.option('--name', '-n', type=str, required=True, help='Role name')
@click.option('--policies', '-p', multiple=True,
              required=True,
              help='List of policies to attach to the role')
@click.option(
    '--expiration', '-e',
    type=str,
    help=f'Expiration date in ISO 8601 format (e.g. {DYNAMIC_DATE_EXAMPLE}). '
         'If no timezone (tz) is specified, it will be interpreted as UTC.')
@click.option('--description', '-d', type=str, required=True,
              help='Description for the created role')
@cli_response(attributes_order=attributes_order)
def add(ctx: ContextObj, customer_id, name, policies, description, expiration):
    """
    Creates the Role entity with the given name from Customer with the given id
    """
    if expiration:
        try:
            datetime.fromisoformat(expiration)
        except ValueError:
            raise click.ClickException(
                f'Invalid value for the \'expiration\' parameter: {expiration}'
            )
    return ctx['api_client'].role_post(
        customer_id=customer_id,
        name=name,
        policies=policies,
        expiration=expiration,
        description=description
    )


@role.command(cls=ViewCommand, name='update')
@click.option('--name', '-n', type=str,
              help='Role name to modify', required=True)
@click.option('--attach_policy', '-a', multiple=True,
              help='List of policies to attach to the role')
@click.option('--detach_policy', '-dp', multiple=True,
              help='List of policies to detach from role')
@click.option(
    '--expiration', '-e', type=str, required=False,
    help=f'Expiration date in ISO 8601 format (e.g. {DYNAMIC_DATE_EXAMPLE}). '
         'If no timezone (tz) is specified, it will be interpreted as UTC.')
@click.option('--description', '-d', type=str,
              help='Description for the created role')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, customer_id, name, attach_policy, detach_policy, expiration, description):
    """
    Updates role configuration.
    """
    req_param_names = ('--attach_policy', '--detach_policy', '--expiration')
    required = (attach_policy, detach_policy, expiration)
    if not any(required):
        raise click.ClickException(
            f'At least one of: {", ".join(req_param_names)} must be specified'
        )

    if expiration:
        try:
            datetime.fromisoformat(expiration)
        except ValueError:
            raise click.ClickException(
                f'Invalid value for the \'expiration\' parameter: {expiration}'
            )
    return ctx['api_client'].role_patch(
        name=name,
        policies_to_attach=attach_policy,
        policies_to_detach=detach_policy,
        expiration=expiration,
        customer_id=customer_id,
        description=description
    )


@role.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True,
              help='Role name to delete')
@cli_response()
def delete(ctx: ContextObj, customer_id, name):
    """
    Deletes customers role.
    """
    return ctx['api_client'].role_delete(
        name=name,
        customer_id=customer_id,
    )
