from typing import Optional, Tuple

import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, \
    customer_option, tenant_option
from c7ncli.service.constants import AWS, AZURE, GOOGLE, ParentScope

parent_type_option = click.option(
    '--type', '-t',
    type=click.Choice(['CUSTODIAN', 'CUSTODIAN_LICENSES', 'SIEM_DEFECT_DOJO',
                       'CUSTODIAN_ACCESS']),
    required=True, help='Parent type'
)

ORDER = ['parent_id', 'application_id', 'customer_id']


@click.group(name='parent')
def parent():
    """Manages Custodian Service Parent Entities"""


@parent.command(cls=ViewCommand, name='add')
@click.option('--application_id', '-aid', type=str, required=False,
              help='Id of an application to connect to the parent')
@parent_type_option
@click.option('--description', '-d', type=str, required=False,
              help='Description for the parent')
@click.option('--cloud', '-c',
              type=click.Choice([AWS, AZURE, GOOGLE]),
              help='Cloud to connect the parent to')
@click.option('--scope', '-sc',
              type=click.Choice(ParentScope.iter()),
              help='Tenants scope for the parent')
@click.option('--rules_to_exclude', '-rte', multiple=True,
              help='Rules to exclude for the scope of tenants')
@tenant_option
@customer_option
@cli_response(ORDER)
def add(ctx: ContextObj, customer_id: Optional[str],
        cloud: Optional[str], **kwargs):
    """
    Creates parent within the customer
    """
    kwargs.update(customer=customer_id)
    kwargs.update(cloud=cloud)
    return ctx['api_client'].parent_post(**kwargs)


@parent.command(cls=ViewCommand, name='update')
@click.option('--parent_id', '-pid', type=str, required=True,
              help='Parent id to update')
@click.option('--application_id', '-aid', type=str,
              help='Id of an application to connect to the parent')
@click.option('--description', '-d', type=str,
              help='Description for the parent')
@click.option('--rules_to_exclude', '-rte', type=str, multiple=True,
              help='Rules to exclude for tenant')
@click.option('--rules_to_include', '-rti', type=str, multiple=True,
              help='Rules to include for tenant')
@customer_option
@cli_response(ORDER)
def update(ctx: ContextObj, customer_id: Optional[str], **kwargs):
    """
    Updates parent within the customer
    """
    kwargs.update(customer=customer_id)
    return ctx['api_client'].parent_patch(**kwargs)


@parent.command(cls=ViewCommand, name='describe')
@click.option('--parent_id', '-pid', type=str,
              help='Parent id to describe a concrete parent')
@customer_option
@cli_response(ORDER)
def describe(ctx: ContextObj, parent_id, customer_id):
    """
    Describes customer's parents
    """
    if parent_id:
        return ctx['api_client'].parent_get(parent_id)
    return ctx['api_client'].parent_list(
        customer=customer_id
    )


@parent.command(cls=ViewCommand, name='delete')
@click.option('--parent_id', '-pid', type=str, required=True,
              help='Parent id to describe a concrete parent')
@cli_response()
def delete(ctx: ContextObj, parent_id):
    """
    Deletes customer's parent by id
    """
    return ctx['api_client'].parent_delete(parent_id)
