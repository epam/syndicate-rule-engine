import click

from c7ncli.group import ContextObj, ViewCommand, cli_response


@click.group(name='report')
def report():
    """Manages Custodian Service Mail configuration"""


@report.command(cls=ViewCommand, name='enable_sending')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def enable_sending(ctx: ContextObj, confirm: bool, customer_id):
    """
    Enables Custodian Service report sending mechanism
    """
    if not confirm:
        raise click.UsageError('Please, specify `--confirm` flag')
    return ctx['api_client'].reports_sending_setting_enable()


@report.command(cls=ViewCommand, name='disable_sending')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def disable_sending(ctx: ContextObj, confirm: bool, customer_id):
    """
    Disables Custodian Service report sending mechanism
    """
    if not confirm:
        raise click.UsageError('Please, specify `--confirm` flag')
    return ctx['api_client'].reports_sending_setting_disable()
