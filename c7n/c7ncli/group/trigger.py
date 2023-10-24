import click
from c7ncli.group import cli_response, ViewCommand, ContextObj


@click.group(name='trigger')
def trigger():
    """Manages Lambda triggering"""


@trigger.command(cls=ViewCommand, name='configuration_backup')
@cli_response()
def configuration_backup(ctx: ContextObj):
    """
    Creates backup of Custodian Service
    """
    return ctx['api_client'].trigger_backup()
