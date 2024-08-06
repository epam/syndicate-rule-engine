import json
from pathlib import Path

import click

from srecli.group import ContextObj, ViewCommand, cli_response, response

attributes_order = 'name', 'permissions', 'customer'


@click.group(name='policy')
def policy():
    """Manages Custodian Service Policy Entities"""


@policy.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str,
              help='Policy name to describe.')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id, name):
    """
    Describes Custodian Service policies of a customer
    """
    if name:
        return ctx['api_client'].policy_get(
            name=name,
            customer_id=customer_id,
        )
    return ctx['api_client'].policy_query(
        customer_id=customer_id,
    )


@policy.command(cls=ViewCommand, name='add')
@click.option('--name', '-n', type=str, required=True,
              help='Policy name to create')
@click.option('--permission', '-p', multiple=True,
              help='List of permissions to attach to the policy')
@click.option('--path_to_permissions', '-path', required=False,
              help='Local path to .json file that contains list of '
                   'permissions to attach to the policy')
@click.option('--permissions_admin', '-admin', is_flag=True,
              help='Whether to add all permissions to this policy')
@click.option('--effect', '-ef', type=click.Choice(('allow', 'deny')),
              required=True, help='That this policy will do')
@click.option('--tenant', '-t', type=str, multiple=True,
              help='Permission will be allowed or denied for these tenants. '
                   'Specify tenant name')
@click.option('--description', '-d', type=str, required=True,
              help='Description for this policy')
@cli_response(attributes_order=attributes_order)
def add(ctx: ContextObj, customer_id, name, permission,
        path_to_permissions, permissions_admin, effect, tenant, description):
    """
    Creates a Custodian Service policy for a customer
    """
    if not permission and not path_to_permissions and not permissions_admin:
        return response(
            '--permission or --path_to_permissions or --permissions_admin '
            'must be provided'
        )
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
        name=name,
        permissions=permissions,
        customer_id=customer_id,
        permissions_admin=permissions_admin,
        effect=effect,
        tenants=tenant,
        description=description
    )


@policy.command(cls=ViewCommand, name='update')
@click.option('--name', '-n', type=str, required=True)
@click.option('--attach_permission', '-ap', multiple=True,
              required=False,
              help='Names of permissions to attach to the policy')
@click.option('--detach_permission', '-dp', multiple=True,
              required=False,
              help='Names of permissions to detach from the policy')
@click.option('--effect', '-ef', type=click.Choice(('allow', 'deny')),
              help='That this policy will do')
@click.option('--add_tenant', '-at', type=str, multiple=True,
              help='Add these tenants. Specify tenant name')
@click.option('--remove_tenant', '-rt', type=str, multiple=True,
              help='Remove these tenants. Specify tenant name')
@click.option('--description', '-d', type=str,
              help='Description for this policy')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, customer_id, name, attach_permission,
           detach_permission, effect, add_tenant, remove_tenant, description):
    """
    Updates permission-list within a Custodian Service policy
    """

    if not attach_permission and not detach_permission and not effect and not add_tenant and not remove_tenant and not description:
        return response('At least one parameter to update must be provided')

    return ctx['api_client'].policy_patch(
        name=name,
        customer_id=customer_id,
        permissions_to_attach=attach_permission,
        permissions_to_detach=detach_permission,
        effect=effect,
        tenants_to_add=add_tenant,
        tenants_to_remove=remove_tenant,
        description=description
    )


@policy.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True,
              help='Policy name to delete')
@cli_response()
def delete(ctx: ContextObj, customer_id, name):
    """
    Deletes a Custodian Service policy of a customer
    """
    return ctx['api_client'].policy_delete(
        name=name,
        customer_id=customer_id,
    )
