import click

from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name='report')
def report():
    """Manages Syndicate Rule Engine configuration"""


@report.command(cls=ViewCommand, name='enable_sending')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def enable_sending(ctx: ContextObj, confirm: bool, customer_id):
    """
    Enables Syndicate Rule Engine report sending mechanism
    """
    if not confirm:
        raise click.ClickException('Please, specify `--confirm` flag')
    return ctx['api_client'].reports_sending_setting_enable()


@report.command(cls=ViewCommand, name='disable_sending')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def disable_sending(ctx: ContextObj, confirm: bool, customer_id):
    """
    Disables Syndicate Rule Engine report sending mechanism
    """
    if not confirm:
        raise click.ClickException('Please, specify `--confirm` flag')
    return ctx['api_client'].reports_sending_setting_disable()
