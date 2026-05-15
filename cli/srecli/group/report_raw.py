import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group import build_tenant_option
from srecli.service.presigned_reports import report_presigned_download_pack
from srecli.group import SREResponse


@click.group(name='raw')
def raw():
    """Fetches raw report"""


@raw.command(cls=ViewCommand, name='latest')
@build_tenant_option(required=True)
@click.option('--obfuscated', is_flag=True,
              help='Whether to obfuscate the data and return also a dictionary')
@click.option('--meta', is_flag=True,
              help='Whether to return rules meta as well')
@click.option(
    '--href',
    '-hf',
    is_flag=True,
    help='Return presigned URL instead of inline payload (set automatically with --download)',
)
@cli_response()
@report_presigned_download_pack
def latest(
    ctx: ContextObj,
    tenant_name,
    obfuscated,
    meta,
    href: bool,
    customer_id,
    download: str | None,
) -> SREResponse:
    """
    Returns latest raw report
    """
    return ctx['api_client'].report_raw_tenant(
        tenant_name=tenant_name,
        obfuscated=obfuscated,
        meta=meta,
        href=href,
        customer_id=customer_id
    )
