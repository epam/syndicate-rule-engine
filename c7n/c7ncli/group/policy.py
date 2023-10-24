import json
from pathlib import Path

import click

from c7ncli.group import cli_response, ViewCommand, response, \
    ContextObj, customer_option
from c7ncli.service.constants import PARAM_CUSTOMER, PARAM_NAME, \
    PARAM_PERMISSIONS


@click.group(name='policy')
def policy():
    """Manages Custodian Service Policy Entities"""


@policy.command(cls=ViewCommand, name='describe')
@click.option('--policy_name', '-name', type=str,
              help='Policy name to describe.')
@customer_option
@cli_response(attributes_order=[PARAM_CUSTOMER, PARAM_NAME, PARAM_PERMISSIONS])
def describe(ctx: ContextObj, customer_id, policy_name=None):
    """
    Describes Custodian Service policies of a customer
    """
    return ctx['api_client'].policy_get(
        customer_display_name=customer_id,
        policy_name=policy_name
    )


@policy.command(cls=ViewCommand, name='add')
@click.option('--policy_name', '-name', type=str, required=True,
              help='Policy name to create')
@click.option('--permission', '-p', multiple=True,
              help='List of permissions to attach to the policy')
@click.option('--path_to_permissions', '-path', required=False,
              help='Local path to .json file that contains list of '
                   'permissions to attach to the policy')
@customer_option
@cli_response(attributes_order=[PARAM_CUSTOMER, PARAM_NAME, PARAM_PERMISSIONS])
def add(ctx: ContextObj, customer_id, policy_name, permission,
        path_to_permissions):
    """
    Creates a Custodian Service policy for a customer
    """
    if not permission and not path_to_permissions:
        return response('--permission or --path_to_permissions '
                        'must be provided')
    permissions = list(permission)
    if path_to_permissions:
        path = Path(path_to_permissions)
        if not path.exists() or not path.is_file():
            return response(f'File {path_to_permissions} does not exist')
        with open(path, 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError as e:
                data = []
        permissions.extend(data)

    return ctx['api_client'].policy_post(
        name=policy_name,
        permissions=permissions,
        customer=customer_id,
    )


@policy.command(cls=ViewCommand, name='update')
@click.option('--policy_name', '-name', type=str, required=True)
@click.option('--attach_permission', '-ap', multiple=True,
              required=False,
              help='Names of permissions to attach to the policy')
@click.option('--detach_permission', '-dp', multiple=True,
              required=False,
              help='Names of permissions to detach from the policy')
@customer_option
@cli_response(attributes_order=[PARAM_CUSTOMER, PARAM_NAME, PARAM_PERMISSIONS])
def update(ctx: ContextObj, customer_id, policy_name, attach_permission,
           detach_permission):
    """
    Updates permission-list within a Custodian Service policy
    """

    if not attach_permission and not detach_permission:
        return response('At least one of the following arguments must be '
                        'provided: attach_permission, detach_permission')

    return ctx['api_client'].policy_patch(
        customer=customer_id,
        name=policy_name,
        permissions_to_attach=attach_permission,
        permissions_to_detach=detach_permission
    )


@policy.command(cls=ViewCommand, name='delete')
@click.option('--policy_name', '-name', type=str, required=True,
              help='Policy name to delete')
@customer_option
@cli_response()
def delete(ctx: ContextObj, customer_id, policy_name):
    """
    Deletes a Custodian Service policy of a customer
    """
    if policy_name:
        policy_name = policy_name.lower()
    return ctx['api_client'].policy_delete(
        customer_display_name=customer_id,
        policy_name=policy_name.lower())


@policy.command(cls=ViewCommand, name='clean_cache')
@click.option('--policy_name', '-name', type=str,
              help='Policy name to clean from cache. If not specified, '
                   'all policies cache within the customer is cleaned')
@customer_option
@cli_response()
def clean_cache(ctx: ContextObj, policy_name, customer_id):
    """
    Clears out cached Custodian Service policies within Lambda
    """
    return ctx['api_client'].policy_clean_cache(
        customer=customer_id,
        name=policy_name
    )
