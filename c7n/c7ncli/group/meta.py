import click

from c7ncli.group import ContextObj, ViewCommand, cli_response


@click.group(name='meta')
def meta():
    """Manages Scan and Tenant Metrics"""


@meta.command(cls=ViewCommand, name='update_standards')
@cli_response()
def update_standards(ctx: ContextObj, customer_id):
    """
    Triggers a metrics update for Custodian Service reports. Report data will
    contain data up to the time when the trigger was executed
    """
    return ctx['api_client'].update_standards()


@meta.command(cls=ViewCommand, name='update_mappings')
@cli_response()
def update_mappings(ctx: ContextObj, customer_id):
    """
    Execution status of the last metrics update
    """
    return ctx['api_client'].update_mappings()


@meta.command(cls=ViewCommand, name='update_meta')
@cli_response()
def update_meta(ctx: ContextObj, customer_id):
    """
    Execution status of the last metrics update
    """
    return ctx['api_client'].update_meta()
