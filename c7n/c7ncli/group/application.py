from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj, \
    customer_option
from c7ncli.group.application_access import access
from c7ncli.group.application_dojo import dojo
from c7ncli.service.constants import AWS, AZURE, GOOGLE


@click.group(name='application')
def application():
    """Manages Applications Entity"""


@application.command(cls=ViewCommand, name='add')
@customer_option
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--cloud', '-c', required=False,
              type=click.Choice([AWS, AZURE, GOOGLE]),
              help='Cloud to activate the application for')
@click.option('--cloud_application_id', '-caid', required=False, type=str,
              help='Application id containing creds to access the cloud')
@click.option('--tenant_license_key', '-tlk', required=False,
              type=str,
              help='Tenant license key with license for the specified cloud')
@cli_response()
def add(ctx: ContextObj, customer_id: Optional[str], **kwargs):
    """
    Creates Custodian Application with specified license
    """

    _cloud_modifier = (bool(kwargs['cloud_application_id']) or
                       bool(kwargs['tenant_license_key']))
    _cloud = bool(kwargs['cloud'])
    kwargs.update(customer=customer_id)

    if _cloud ^ _cloud_modifier:
        return response('Both --cloud and --cloud_application_id '
                        'or --tenant_license_key must be specified or omitted')
    kwargs['access_application_id'] = kwargs.pop('cloud_application_id')
    return ctx['api_client'].application_post(**kwargs)


@application.command(cls=ViewCommand, name='update')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--cloud', '-c',
              type=click.Choice([AWS, AZURE, GOOGLE]),
              help='Cloud to activate the application for')
@click.option('--cloud_application_id', '-caid',
              type=str, help='Application id containing creds to '
                             'access the cloud')
@click.option('--tenant_license_key', '-tlk',
              type=str,
              help='Tenant license key with license for the specified cloud')
@customer_option
@cli_response()
def update(ctx: ContextObj, application_id, **kwargs):
    """
    Update Custodian Application
    """

    _cloud_modifier = (bool(kwargs['cloud_application_id']) or
                       bool(kwargs['tenant_license_key']))
    _cloud = bool(kwargs['cloud'])

    if _cloud ^ _cloud_modifier:
        return response('Both --cloud and --cloud_application_id '
                        'or --tenant_license_key must be specified or omitted')
    kwargs['access_application_id'] = kwargs.pop('cloud_application_id')
    kwargs['customer'] = kwargs.pop('customer_id', None)
    return ctx['api_client'].application_patch(application_id, **kwargs)


@application.command(cls=ViewCommand, name='describe')
@click.option('--application_id', '-aid', required=False, type=str,
              help='Id of the application')
@customer_option
@cli_response()
def describe(ctx: ContextObj, application_id: str,
             customer_id: Optional[str]):
    """
    Describe Custodian Application
    """
    if application_id:
        return ctx['api_client'].application_get(application_id)
    return ctx['api_client'].application_list(
        customer=customer_id
    )


@application.command(cls=ViewCommand, name='delete')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@customer_option
@cli_response()
def delete(ctx: ContextObj, application_id: str, customer_id):
    """
    Delete Custodian Application
    """
    return ctx['api_client'].application_delete(application_id, customer_id)


application.add_command(access)
application.add_command(dojo)