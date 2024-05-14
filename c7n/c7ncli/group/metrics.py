import click

from c7ncli.group import ContextObj, ViewCommand, cli_response


@click.group(name='metrics')
def metrics():
    """Manages Scan and Tenant Metrics"""


@metrics.command(cls=ViewCommand, name='update')
@cli_response()
def update(ctx: ContextObj, customer_id):
    """
    Triggers a metrics update for Custodian Service reports. Report data will
    contain data up to the time when the trigger was executed
    """
    return ctx['api_client'].trigger_metrics_update()


@metrics.command(cls=ViewCommand, name='status')
@click.option('--from_date', '-from', type=str,
              help='Query metrics statuses from this date. Accepts date ISO '
                   'string. Example: 2023-10-20')
@click.option('--to_date', '-to', type=str,
              help='Query metrics statuses till this date. Accepts date ISO '
                   'string. Example: 2023-12-29')
@cli_response()
def status(ctx: ContextObj, from_date: str, to_date: str, customer_id):
    """
    Execution status of the last metrics update
    """
    params = {
        'from': from_date,
        'to': to_date
    }
    return ctx['api_client'].metrics_status(**params)
