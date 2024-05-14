from datetime import datetime

import click

from c7ncli.group import ContextObj, ViewCommand, cli_response, response

attributes_order = 'name', 'expiration', 'policies'


@click.group(name='role')
def role():
    """Manages Role Entity"""


@role.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, help='Role name to describe.')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id, name):
    """
    Describes a Custodian Service roles for the given customer.
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
@click.option('--expiration', '-e', type=str,
              help='Expiration date, ISO 8601. Example: 2021-08-01T15:30:00')
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
            return response(f'Invalid value for the \'expiration\' '
                            f'parameter: {expiration}')
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
@click.option('--detach_policy', '-d', multiple=True,
              help='List of policies to detach from role')
@click.option('--expiration', '-e', type=str, required=False,
              help='Expiration date, ISO 8601. Example: 2021-08-01T15:30:00')
@click.option('--description', '-d', type=str,
              help='Description for the created role')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, customer_id, name, attach_policy, detach_policy, expiration, description):
    """
    Updates role configuration.
    """
    required = (attach_policy, detach_policy, expiration)
    if not any(required):
        return response(f'At least one of: {", ".join(required)} '
                        f'must be specified')

    if expiration:
        try:
            datetime.fromisoformat(expiration)
        except ValueError:
            return response(f'Invalid value for the \'expiration\' '
                            f'parameter: {expiration}')
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
