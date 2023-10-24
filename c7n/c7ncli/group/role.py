from datetime import datetime

import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj, \
    customer_option
from c7ncli.service.constants import PARAM_NAME, PARAM_CUSTOMER, \
    PARAM_POLICIES, PARAM_EXPIRATION


@click.group(name='role')
def role():
    """Manages Role Entity"""


@role.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, help='Role name to describe.')
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER, PARAM_POLICIES,
                                PARAM_EXPIRATION])
def describe(ctx: ContextObj, customer_id, name):
    """
    Describes a Custodian Service roles for the given customer.
    """
    return ctx['api_client'].role_get(
        customer=customer_id,
        name=name
    )


@role.command(cls=ViewCommand, name='add')
@click.option('--name', '-n', type=str, required=True, help='Role name')
@click.option('--policies', '-p', multiple=True,
              required=True,
              help='List of policies to attach to the role')
@click.option('--expiration', '-e', type=str,
              help='Expiration date, ISO 8601. Example: 2021-08-01T15:30:00')
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER, PARAM_POLICIES,
                                PARAM_EXPIRATION])
def add(ctx: ContextObj, customer_id, name, policies, expiration):
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
        customer=customer_id,
        name=name,
        policies=policies,
        expiration=expiration
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
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER, PARAM_POLICIES,
                                PARAM_EXPIRATION])
def update(ctx: ContextObj, customer_id, name, attach_policy, detach_policy, expiration):
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
        customer=customer_id
    )


@role.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True,
              help='Role name to delete')
@customer_option
@cli_response()
def delete(ctx: ContextObj, customer_id, name):
    """
    Deletes customers role.
    """
    return ctx['api_client'].role_delete(
        customer=customer_id,
        name=name
    )


@role.command(cls=ViewCommand, name='clean_cache')
@click.option('--name', '-n', type=str,
              help='Role name to clean from cache')
@customer_option
@cli_response()
def clean_cache(ctx: ContextObj, customer_id, name):
    """
    Cleans cached role from lambda.
    """
    return ctx['api_client'].role_clean_cache(
        customer=customer_id,
        name=name
    )
