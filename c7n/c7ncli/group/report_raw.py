import click

from c7ncli.group import ContextObj, ViewCommand, cli_response
from c7ncli.group import build_tenant_option


@click.group(name='raw')
def raw():
    """Fetches raw report"""


@raw.command(cls=ViewCommand, name='latest')
@build_tenant_option(required=True)
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option('--meta', is_flag=True,
              help='Whether to return rules meta as well')
@cli_response()
def latest(ctx: ContextObj, tenant_name, obfuscated, meta, customer_id):
    """
    Returns latest raw report
    """
    return ctx['api_client'].report_raw_tenant(
        tenant_name=tenant_name,
        obfuscated=obfuscated,
        meta=meta,
        customer_id=customer_id
    )
