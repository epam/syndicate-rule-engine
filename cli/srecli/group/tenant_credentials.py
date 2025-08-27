import click

from srecli.group import ViewCommand, cli_response, ContextObj, limit_option, \
    next_option, build_tenant_option
from srecli.service.constants import AWS, AZURE, GOOGLE
from srecli.service.adapter_client import SREResponse


@click.group(name='credentials')
def credentials():
    """
    Allows to bind existing credentials applications to tenants
    :return:
    """


@credentials.command(cls=ViewCommand, name='describe')
@click.option('--cloud', '-cl', type=click.Choice((AWS, AZURE, GOOGLE)),
              help='Cloud to describe credentials by')
@click.option('--application_id', '-aid', type=str,
              help='Application id to describe a concrete credentials item')
@limit_option
@next_option
@cli_response()
def describe(ctx: ContextObj, application_id, limit, next_token, cloud,
             customer_id):
    """
    Lists all available applications with credentials
    """
    if not any((cloud, application_id)):
        return SREResponse.build(
            'Provide either --application_id or --cloud'
        )
    if application_id:
        return ctx['api_client'].get_credentials(application_id)
    return ctx['api_client'].query_credentials(
        cloud=cloud,
        limit=limit,
        next_token=next_token,
        customer_id=customer_id,
    )


@credentials.command(cls=ViewCommand, name='link')
@click.option('--application_id', '-aid', type=str, required=True,
              help='Application id to describe a concrete credentials item')
@build_tenant_option(multiple=True)
@click.option('--all_tenants', is_flag=True,
              help='Whether to activate integration for all tenants')
@click.option('--exclude_tenant', '-et', type=str, multiple=True,
              help='Tenants to exclude for this integration. '
                   'Can be specified together with --all_tenants flag')
@cli_response()
def link(ctx: ContextObj, application_id: str, tenant_name: tuple[str, ...],
         all_tenants: bool, exclude_tenant: tuple[str, ...],
         customer_id: str | None):
    """
    Links credentials to a specific set of tenants.
    Each activation overrides the existing one
    """
    if tenant_name and any((all_tenants, exclude_tenant)):
        return SREResponse.build(
            'Do not provide --all_tenants or '
            '--exclude_tenants if --tenant_name given'
        )
    if not all_tenants and not tenant_name:
        return SREResponse.build(
            'Either --all_tenants or --tenant_name must be given'
        )
    if exclude_tenant and not all_tenants:
        return SREResponse.build(
            'set --all_tenants if you provide --clouds or --exclude_tenants'
        )
    return ctx['api_client'].credentials_bind(
        application_id=application_id,
        tenant_names=tenant_name,
        all_tenants=all_tenants,
        exclude_tenants=exclude_tenant,
        customer_id=customer_id
    )


@credentials.command(cls=ViewCommand, name='unlink')
@click.option('--application_id', '-aid', type=str, required=True,
              help='Application id to describe a concrete credentials item')
@cli_response()
def unlink(ctx: ContextObj, application_id: str, customer_id):
    """
    Unlinks credentials from tenants
    """
    return ctx['api_client'].credentials_unbind(
        application_id=application_id,
        customer_id=customer_id
    )


@credentials.command(cls=ViewCommand, name='get_linked_tenants')
@click.option('--application_id', '-aid', type=str, required=True,
              help='Application id to describe a concrete credentials item')
@cli_response()
def get_linked_tenants(ctx: ContextObj, application_id: str, customer_id):
    """
    Returns all the tenants the credentials is linked to
    """
    return ctx['api_client'].credentials_get_binding(
        application_id=application_id,
        customer_id=customer_id
    )
