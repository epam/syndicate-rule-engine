import click
from c7ncli.group import cli_response, ViewCommand, ContextObj, customer_option
from c7ncli.service.constants import PARAM_CUSTOMERS, PARAM_LICENSE_HASH_KEY, \
    PARAM_RULESET_IDS, PARAM_EXPIRATION, PARAM_LATEST_SYNC


@click.group(name='license')
def license():
    """Manages Custodian Service License Entities"""


@license.command(cls=ViewCommand, name='describe')
@click.option('--license_key', '-lk', type=str, required=False,
              help='License key to describe')
@customer_option
@cli_response()
def describe(ctx: ContextObj, license_key, customer_id):
    """
    Describes a Custodian Service Licenses
    """
    return ctx['api_client'].license_get(
        license_key=license_key, customer=customer_id
    )


# currently obsolete. Don't try to uncomment, the endpoint is disabled
# @license.command(cls=ViewCommand, name='add')
@click.option('--tenant_license_key', '-tlk',
              type=str, required=True, help='License key to create')
@click.option('--tenant_name', '-tn', type=str,
              help='Tenant name to attach the license to', required=True)
@cli_response()
def add(ctx: ContextObj, tenant_license_key, tenant_name):
    """
    Adds the licensed rulesets to a specific tenant if allowed.
    """
    return ctx['api_client'].license_post(tenant_name, tenant_license_key)


@license.command(cls=ViewCommand, name='delete')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key to delete')
@customer_option
@cli_response()
def delete(ctx: ContextObj, license_key, customer_id):
    """
    Deletes Custodian Service Licenses
    """
    return ctx['api_client'].license_delete(
        customer_name=customer_id,
        license_key=license_key)


@license.command(cls=ViewCommand, name='sync')
@click.option('--license_key', '-lk', type=str, required=True,
              help='License key to synchronize')
@cli_response(attributes_order=[
    PARAM_LICENSE_HASH_KEY, PARAM_CUSTOMERS, PARAM_RULESET_IDS,
    PARAM_EXPIRATION, PARAM_LATEST_SYNC])
def sync(ctx: ContextObj, license_key=None):
    """
    Synchronizes Custodian Service Licenses
    """
    return ctx['api_client'].license_sync(license_key=license_key)
