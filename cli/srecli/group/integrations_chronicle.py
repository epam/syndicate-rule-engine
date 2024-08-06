import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
)
from srecli.service.constants import AWS, AZURE, GOOGLE, KUBERNETES


@click.group(name='chronicle')
def chronicle():
    """
    Manages chronicle integrations
    :return:
    """


@chronicle.command(cls=ViewCommand, name='add')
@click.option('--url', '-u', type=str, required=True,
              help='Base url for Google Chronicle integration '
                   '(https://region.googleapis.com')
@click.option('--description', '-d', type=str, required=True,
              help='Human-readable description of this installation')
@click.option('--credentials_application_id', '-cai', required=True,
              type=str, help='Id of application with google credentials')
@click.option('--instance_customer_id', '-ici', type=str, required=True,
              help='Customer id of Chronicle instance')
@cli_response()
def add(ctx: ContextObj, url: str, credentials_application_id: str,
        instance_customer_id: str, description: str, customer_id: str | None):
    """
    Adds Chronicle integration
    """
    return ctx['api_client'].chronicle_add(
        endpoint=url,
        description=description,
        credentials_application_id=credentials_application_id,
        instance_customer_id=instance_customer_id,
        customer_id=customer_id
    )


@chronicle.command(cls=ViewCommand, name='describe')
@click.option('--integration_id', '-id', type=str, required=False,
              help='Id of Chronicle integration')
@cli_response()
def describe(ctx: ContextObj, integration_id: str | None,
             customer_id: str | None):
    """
    Describes Chronicle integration
    """
    if integration_id:
        return ctx['api_client'].chronicle_get(integration_id, customer_id=customer_id)
    return ctx['api_client'].chronicle_query(customer_id=customer_id)


@chronicle.command(cls=ViewCommand, name='delete')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of Chronicle integration')
@cli_response()
def delete(ctx: ContextObj, integration_id: str, customer_id):
    """
    Deletes Chronicle integration
    """
    return ctx['api_client'].chronicle_delete(
        integration_id,
        customer_id=customer_id
    )


@chronicle.command(cls=ViewCommand, name='activate')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of Chronicle integration')
@build_tenant_option(multiple=True)
@click.option('--all_tenants', is_flag=True,
              help='Whether to activate integration for all tenants')
@click.option('--clouds', '-cl',
              type=click.Choice((AWS, AZURE, GOOGLE, KUBERNETES)),
              multiple=True,
              help='Tenant clouds to activate this dojo for. '
                   'Can be specific together with --all_tenants flag')
@click.option('--exclude_tenant', '-et', type=str, multiple=True,
              help='Tenants to exclude for this integration. '
                   'Can be specified together with --all_tenants flag')
@click.option('--send_after_job', '-saj', is_flag=True,
              help='Specify this flag to send results to dojo after each job '
                   'automatically')
@click.option('--convert_to', '-ct', type=click.Choice(('EVENTS', 'ENTITIES')),
              default='EVENTS',
              help='The way Rule engine findings will be converted '
                   'before sending to Chronicle')
@cli_response()
def activate(ctx: ContextObj, integration_id: str,
             tenant_name: tuple[str, ...],
             all_tenants: bool, clouds: tuple[str],
             exclude_tenant: tuple[str, ...],
             send_after_job: bool, convert_to: str, customer_id):
    """
    Activates a concrete Chronicle integration for a specific set of tenants.
    Each activation overrides the existing one
    """
    if tenant_name and any((all_tenants, clouds, exclude_tenant)):
        raise click.ClickException(
            'Do not provide --all_tenants, --clouds or --exclude_'
            'tenants if --tenant_name given'
        )
    if not all_tenants and not tenant_name:
        raise click.ClickException(
            'Either --all_tenants or --tenant_name must be given'
        )
    if (clouds or exclude_tenant) and not all_tenants:
        raise click.ClickException(
            'set --all_tenants if you provide --clouds or --exclude_tenants'
        )
    return ctx['api_client'].chronicle_activate(
        id=integration_id,
        tenant_names=tenant_name,
        all_tenants=all_tenants,
        clouds=clouds,
        exclude_tenants=exclude_tenant,
        send_after_job=send_after_job,
        convert_to=convert_to,
        customer_id=customer_id,
    )


@chronicle.command(cls=ViewCommand, name='deactivate')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of Chronicle integration')
@cli_response()
def deactivate(ctx: ContextObj, integration_id: str, customer_id):
    """
    Deactivates a concrete Chronicle integration for a specific set of tenants
    """
    return ctx['api_client'].chronicle_deactivate(
        id=integration_id,
        customer_id=customer_id
    )


@chronicle.command(cls=ViewCommand, name='get_activation')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of Chronicle integration')
@cli_response()
def get_activation(ctx: ContextObj, integration_id: str, customer_id):
    """
    Returns a Chronicle activation
    """
    return ctx['api_client'].chronicle_get_activation(
        id=integration_id,
        customer_id=customer_id
    )
