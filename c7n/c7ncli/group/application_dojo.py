from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj, \
    customer_option


@click.group(name='dojo')
def dojo():
    """Manages Applications Entity"""


@dojo.command(cls=ViewCommand, name='add')
@customer_option
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--url', '-U', type=str, required=True,
              help='Url to Custodian installation')
@click.option('--api_key', '-key', type=str, required=True,
              help='Defect Dojo api key')
@cli_response()
def add(ctx: ContextObj, customer_id: Optional[str], **kwargs):
    """
    Creates an application which holds access to Defect Dojo
    """
    kwargs.update(customer=customer_id)
    return ctx['api_client'].dojo_application_post(**kwargs)


@dojo.command(cls=ViewCommand, name='update')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--url', '-U', type=str,
              help='Url to Custodian installation')
@click.option('--api_key', '-key', type=str,
              help='Defect Dojo api key')
@customer_option
@cli_response()
def update(ctx: ContextObj, application_id, **kwargs):
    """
    Update Custodian Application
    """
    kwargs['customer'] = kwargs.pop('customer_id', None)
    return ctx['api_client'].dojo_application_patch(application_id, **kwargs)


@dojo.command(cls=ViewCommand, name='describe')
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
        return ctx['api_client'].dojo_application_get(application_id)
    return ctx['api_client'].dojo_application_list(
        customer=customer_id
    )


@dojo.command(cls=ViewCommand, name='delete')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@customer_option
@cli_response()
def delete(ctx: ContextObj, application_id: str, customer_id):
    """
    Delete Custodian Application
    """
    return ctx['api_client'].dojo_application_delete(
        application_id, customer_id
    )
