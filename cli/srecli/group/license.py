import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
)
from srecli.service.constants import (
    AWS, 
    AZURE, 
    GOOGLE, 
    KUBERNETES, 
    OPERATION_STATUS_HINT,
    ServiceOperationType,
)

attributes_order = 'license_key', 'ruleset_ids', 'expiration', 'latest_sync'


@click.group(name='license')
def license():
    """Manages License Entities"""


@license.command(cls=ViewCommand, name='describe')
@click.option('--license_key', '-lk', type=str, required=False,
              help='License key to describe')
@cli_response()
def describe(ctx: ContextObj, license_key, customer_id):
    """
    Describes licenses
    """
    if license_key:
        return ctx['api_client'].license_get(license_key,
                                             customer_id=customer_id)
    return ctx['api_client'].license_query(customer_id=customer_id)


@license.command(cls=ViewCommand, name='add')
@click.option('--tenant_license_key', '-tlk',
              type=str, required=True, help='License key to create')
@cli_response()
def add(ctx: ContextObj, tenant_license_key: str,
        customer_id: str | None):
    """
    Adds a license from License Manager to the system. After performing this
    action licensed rulesets will appear and will be ready to use
    """
    return ctx['api_client'].license_post(
        tenant_license_key=tenant_license_key,
        customer_id=customer_id,
    )


@license.command(cls=ViewCommand, name='delete')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key to delete')
@cli_response()
def delete(ctx: ContextObj, license_key, customer_id):
    """
    Deletes Licenses
    """
    return ctx['api_client'].license_delete(license_key=license_key,
                                            customer_id=customer_id)


@license.command(
    cls=ViewCommand, 
    name='sync',
)
@click.option(
    '--license_key', '-lk',
    type=str,
    required=True,
    help='License key to synchronize',
)
@cli_response(
    attributes_order=attributes_order,
    hint=OPERATION_STATUS_HINT.format(
        operation_type=ServiceOperationType.LICENSE_SYNC.value[0],
    ),
)
def sync(ctx: ContextObj, license_key, customer_id):
    """
    Synchronizes Licenses
    """
    return ctx['api_client'].license_sync(license_key, customer_id=customer_id)


@license.command(cls=ViewCommand, name='activate')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key')
@build_tenant_option(multiple=True)
@click.option('--all_tenants', is_flag=True,
              help='Whether to activate integration for all tenants')
@click.option('--clouds', '-cl',
              type=click.Choice((AWS, AZURE, GOOGLE, KUBERNETES)),
              multiple=True,
              help='Tenant clouds to activate this license for. '
                   'Can be specific together with --all_tenants flag')
@click.option('--exclude_tenant', '-et', type=str, multiple=True,
              help='Tenants to exclude for this integration. '
                   'Can be specified together with --all_tenants flag')
@cli_response()
def activate(ctx: ContextObj, license_key: str, tenant_name: tuple[str, ...],
             all_tenants: bool, clouds: tuple[str],
             exclude_tenant: tuple[str, ...], customer_id: str | None):
    """
    Activates a concrete license for a specific set of tenants.
    Each activation overrides the existing one
    """
    if tenant_name and any((all_tenants, clouds, exclude_tenant)):
        raise click.ClickException(
            'Do not provide --all_tenants, --clouds or '
            '--exclude_tenants if --tenant_name given'
        )
    if not all_tenants and not tenant_name:
        raise click.ClickException(
            'Either --all_tenants or --tenant_name must be given'
        )
    if (clouds or exclude_tenant) and not all_tenants:
        raise click.ClickException(
            'set --all_tenants if you provide --clouds or --exclude_tenants'
        )
    return ctx['api_client'].license_activate(
        license_key=license_key,
        tenant_names=tenant_name,
        all_tenants=all_tenants,
        clouds=clouds,
        exclude_tenants=exclude_tenant,
        customer_id=customer_id
    )


@license.command(cls=ViewCommand, name='deactivate')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key')
@cli_response()
def deactivate(ctx: ContextObj, license_key: str, customer_id):
    """
    Deactivates a concrete license for a specific set of tenants
    """
    return ctx['api_client'].license_deactivate(license_key=license_key,
                                                customer_id=customer_id)


@license.command(cls=ViewCommand, name='get_activation')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key')
@cli_response()
def get_activation(ctx: ContextObj, license_key: str, customer_id):
    """
    Returns license activation
    """
    return ctx['api_client'].license_get_activation(license_key=license_key,
                                                    customer_id=customer_id)


@license.command(cls=ViewCommand, name='update_activation')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key')
@click.option('--add_tenant', '-at', type=str, multiple=True,
              help='Tenants to activate')
@click.option('--exclude_tenant', '-et', type=str, multiple=True,
              help='Tenants to deactivate')
@cli_response()
def update_activation(ctx: ContextObj, license_key, customer_id, add_tenant,
                      exclude_tenant):
    """
    Allows to add and remove specific tenants from the activation
    """
    return ctx['api_client'].license_update_activation(
        license_key=license_key,
        add_tenants=add_tenant,
        remove_tenants=exclude_tenant,
        customer_id=customer_id
    )
