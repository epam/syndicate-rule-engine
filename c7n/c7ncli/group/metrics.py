import click
from c7ncli.group import cli_response, ViewCommand, ContextObj


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(cls=ViewCommand, name='update')
@cli_response()
def update(ctx: ContextObj):
    """
    Triggers a metrics update for Custodian Service reports. Report data will
    contain data up to the time when the trigger was executed
    """
    return ctx['api_client'].trigger_metrics_update()


@metrics.command(cls=ViewCommand, name='status')
@cli_response()
def status(ctx: ContextObj):
    """
    Execution status of the last metrics update
    """
    return ctx['api_client'].metrics_status()
