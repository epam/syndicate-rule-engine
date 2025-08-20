import click

from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name='mail')
def mail():
    """Manages Mail configuration"""


@mail.command(cls=ViewCommand, name='describe')
@click.option('--display_password', '-dp', is_flag=True, default=False,
              help='Specify to whether display a configured password.')
@cli_response()
def describe(ctx: ContextObj, display_password: bool, customer_id):
    """
    Describes Mail configuration
    """
    return ctx['api_client'].mail_setting_get(disclose=display_password)


@mail.command(cls=ViewCommand, name='add')
@click.option('--username', '-u', type=str, required=True,
              help='Username of mail account.')
@click.option('--password', '-pd', type=str, required=True,
              help='Password of mail account.')
@click.option('--password_label', '-pl', type=str, required=True,
              help='Name of the parameter to store password under.')
@click.option('--host', '-h', type=str, required=True,
              help='Host of a mail server.')
@click.option('--port', '-pt', type=int, required=True,
              help='Port of a mail server.')
@click.option('--use_tls', '-tls', is_flag=True, default=False,
              help='Specify to whether utilize TLS.')
@click.option('--sender_name', '-s', type=str, required=False,
              help='Name to specify as the sender of email(s). '
                   'Defaults to \'--username\'.')
@click.option('--emails_per_session', '-eps', type=int, default=1,
              required=False, help='Amount of emails to send per session.')
@cli_response()
def add(ctx: ContextObj,
        username, password, password_label, host, port,
        use_tls, sender_name, emails_per_session, customer_id):
    """
    Creates Mail configuration
    """
    if emails_per_session < 1:
        raise click.ClickException(
            '\'--emails_per_session\' must be a positive integer.'
        )

    return ctx['api_client'].mail_setting_post(
        username=username,
        password=password,
        password_alias=password_label,
        host=host,
        port=port,
        use_tls=use_tls,
        default_sender=sender_name or username,
        max_emails=emails_per_session
    )


@mail.command(cls=ViewCommand, name='delete')
@click.option('--confirm', is_flag=True, help='Confirms the action.')
@cli_response()
def delete(ctx: ContextObj, confirm: bool, customer_id):
    """
    Deletes Mail configuration
    """
    if not confirm:
        raise click.ClickException('Please, specify `--confirm` flag')
    return ctx['api_client'].mail_setting_delete()
