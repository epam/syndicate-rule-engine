from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj, \
    customer_option


@click.group(name='access')
def access():
    """Manages Applications Entity"""


@access.command(cls=ViewCommand, name='add')
@customer_option
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--username', '-u', type=str,
              help='Username to set to the application')
@click.option('--password', '-p', type=str,
              help='Password to set to application')
@click.option('--url', '-U', type=str,
              help='Url to Custodian installation')
@click.option('--auto_resolve_access', '-ara', is_flag=True,
              help='If specified, Custodian will try to '
                   'resolve access automatically. '
                   'Otherwise you must specify url')
@click.option('--results_storage', '-rs', type=str,
              help='S3 bucket name were to store EC2 recommendations')
@cli_response()
def add(ctx: ContextObj, customer_id: Optional[str], **kwargs):
    """
    Creates Custodian Application with specified access data. Only one
    Custodian application with access data within a customer can be created
    """
    kwargs.update(customer=customer_id)
    if bool(kwargs['username']) ^ bool(kwargs['password']):
        return response('Both --username and --password must be given')
    if not kwargs['auto_resolve_access'] and not kwargs['url']:
        return response('--url must be given in case '
                        '--auto_resolve_access is not specified')
    return ctx['api_client'].access_application_post(**kwargs)


@access.command(cls=ViewCommand, name='update')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@click.option('--description', '-d', type=str,
              help='Application description')
@click.option('--username', '-u', type=str,
              help='Username to set to the application')
@click.option('--password', '-p', type=str,
              help='Password to set to application')
@click.option('--url', '-U', type=str,
              help='Url to Custodian installation')
@click.option('--auto_resolve_access', '-ara', is_flag=True,
              help='If specified, Custodian will try to '
                   'resolve access automatically. '
                   'Otherwise you must specify url')
@click.option('--results_storage', '-rs', type=str,
              help='S3 bucket name were to store EC2 recommendations')
@customer_option
@cli_response()
def update(ctx: ContextObj, application_id, **kwargs):
    """
    Update Custodian Application
    """
    if bool(kwargs['username']) ^ bool(kwargs['password']):
        return response('Both --username and --password must be given')
    kwargs['customer'] = kwargs.pop('customer_id', None)
    return ctx['api_client'].access_application_patch(application_id, **kwargs)


@access.command(cls=ViewCommand, name='describe')
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
        return ctx['api_client'].access_application_get(application_id)
    return ctx['api_client'].access_application_list(
        customer=customer_id
    )


@access.command(cls=ViewCommand, name='delete')
@click.option('--application_id', '-aid', required=True, type=str,
              help='Id of the application')
@customer_option
@cli_response()
def delete(ctx: ContextObj, application_id: str, customer_id):
    """
    Delete Custodian Application
    """
    return ctx['api_client'].access_application_delete(
        application_id, customer_id
    )
