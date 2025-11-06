import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
)
from srecli.service.constants import AWS, AZURE, GOOGLE, KUBERNETES


@click.group(name='dojo')
def dojo():
    """
    Manages defect dojo integrations
    :return:
    """


@dojo.command(cls=ViewCommand, name='add')
@click.option('--url', '-u', type=str, required=True,
              help='Url to defect dojo installation. With API prefix '
                   '(http://127.0.0.1:8080/api/v2')
@click.option('--api_key', '-ak', type=str, required=True,
              help='Defect dojo api key')
@click.option('--description', '-d', type=str, required=True,
              help='Human-readable description of this installation')
@cli_response()
def add(ctx: ContextObj, url: str, api_key: str, description: str,
        customer_id: str | None):
    """
    Adds dojo integration
    """
    return ctx['api_client'].dojo_add(
        api_key=api_key,
        url=url,
        description=description,
        customer_id=customer_id
    )


@dojo.command(cls=ViewCommand, name='describe')
@click.option('--integration_id', '-id', type=str, required=False,
              help='Id of dojo integration')
@cli_response()
def describe(ctx: ContextObj, integration_id: str | None,
             customer_id: str | None):
    """
    Describes Dojo integration
    """
    if integration_id:
        return ctx['api_client'].dojo_get(integration_id, customer_id=customer_id)
    return ctx['api_client'].dojo_query(customer_id=customer_id)


@dojo.command(cls=ViewCommand, name='delete')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of dojo integration')
@cli_response()
def delete(ctx: ContextObj, integration_id: str, customer_id):
    """
    Deletes Dojo integration
    """
    return ctx['api_client'].dojo_delete(integration_id,
                                         customer_id=customer_id)


@dojo.command(cls=ViewCommand, name='activate')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of dojo integration')
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
@click.option('--scan_type', '-st', default='Generic Findings Import',
              show_default=True,
              type=click.Choice(
                  ('Generic Findings Import', 'Cloud Custodian Scan')),
              help='Defect Dojo scan type. Generic Findings Import can be '
                   'used with open source DefectDojo whereas Cloud '
                   'Custodian Scan - with EPAM`s fork')
@click.option('--send_after_job', '-saj', is_flag=True,
              help='Specify this flag to send results to dojo after each job '
                   'automatically')
@click.option('--product_type', type=str, default='Rule Engine',
              help='Product type to create in Dojo, '
                   '"Rule Engine" is used by default. "tenant_name", '
                   '"customer_name" and "job_id" can be used as generic '
                   'placeholders inside curly brackets')
@click.option('--product', type=str, default='{tenant_name}',
              help='Product name to create in Dojo, '
                   '"{tenant_name}" is used by default')
@click.option('--engagement', type=str, default='Rule-Engine Main',
              help='Engagement name to create in Dojo. "Rule-Engine Main" '
                   'is used by default')
@click.option('--test', type=str, default='{job_id}',
              help='Test name to create in Dojo, '
                   '"{job_id}" is used by default')
@click.option('--attachment', type=click.Choice(('json', 'xlsx', 'csv')),
              required=False,
              help='What type of file with resources to attach to each '
                   'finding. If not provided, no files will be attached, '
                   'resources will be displayed in description')
@cli_response()
def activate(ctx: ContextObj, integration_id: str,
             tenant_name: tuple[str, ...],
             all_tenants: bool, clouds: tuple[str],
             exclude_tenant: tuple[str, ...], scan_type: str,
             send_after_job: bool, product_type: str | None,
             product: str | None, engagement: str | None, test: str | None,
             attachment: str | None, customer_id):
    """
    Activates a concrete dojo integration for a specific set of tenants.
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
    return ctx['api_client'].dojo_activate(
        id=integration_id,
        tenant_names=tenant_name,
        all_tenants=all_tenants,
        clouds=clouds,
        exclude_tenants=exclude_tenant,
        scan_type=scan_type,
        send_after_job=send_after_job,
        product_type=product_type,
        product=product,
        engagement=engagement,
        test=test,
        attachment=attachment,
        customer_id=customer_id
    )


@dojo.command(cls=ViewCommand, name='deactivate')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of dojo integration')
@cli_response()
def deactivate(ctx: ContextObj, integration_id: str, customer_id):
    """
    Deactivates a concrete dojo integration for a specific set of tenants
    """
    return ctx['api_client'].dojo_deactivate(id=integration_id,
                                             customer_id=customer_id)


@dojo.command(cls=ViewCommand, name='get_activation')
@click.option('--integration_id', '-id', type=str, required=True,
              help='Id of dojo integration')
@cli_response()
def get_activation(ctx: ContextObj, integration_id: str, customer_id):
    """
    Returns a dojo activation
    """
    return ctx['api_client'].dojo_get_activation(
        id=integration_id,
        customer_id=customer_id
    )
