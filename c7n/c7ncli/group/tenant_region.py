from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, ContextObj
from c7ncli.group import tenant_option


@click.group(name='region')
def region_group():
    """Manages tenant regions"""


@region_group.command(cls=ViewCommand, name='activate')
@tenant_option
@click.option('--region', '-r', type=str,
              required=True,
              help='Region native name to activate')
@cli_response()
def activate(ctx: ContextObj, tenant_name: Optional[str], region: str):
    """
    Activates region in tenant
    """
    return ctx['api_client'].tenant_region_post(
        tenant_name=tenant_name,
        region=region
    )
