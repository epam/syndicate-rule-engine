import click

from c7ncli.group import ContextObj, ViewCommand, cli_response


@click.group(name='config')
def config():
    """Manages License Manager Config data"""


@config.command(cls=ViewCommand, name='describe')
@cli_response()
def describe(ctx: ContextObj, customer_id):
    """
    Describes current License Manager access configuration data
    """
    # Could allow to ping - to ensure network access.
    return ctx['api_client'].lm_config_setting_get()


@config.command(cls=ViewCommand, name='add')
@click.option('--host', '-h',
              type=str, required=True,
              help='License Manager host. You can specify the full url here')
@click.option('--port', '-p', type=int,
              help='License Manager port.', required=False)
@click.option('--protocol', '-pr', type=click.Choice(('HTTP', 'HTTPS')),
              help='License manager protocol')
@click.option('--stage', '-st', type=str,
              help='Path prefix')
@cli_response()
def add(ctx: ContextObj, **kwargs):
    """
    Adds License Manager access configuration data
    """
    return ctx['api_client'].lm_config_setting_post(**kwargs)


@config.command(cls=ViewCommand, name='delete')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def delete(ctx: ContextObj, confirm: bool, customer_id):
    """
    Removes current License Manager access configuration data
    """
    if not confirm:
        raise click.UsageError('Please, specify `--confirm` flag')
    # Could allow to ping - to ensure network access.
    return ctx['api_client'].lm_config_setting_delete()
