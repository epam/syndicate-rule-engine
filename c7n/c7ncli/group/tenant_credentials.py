import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, \
    account_option, build_account_option
from c7ncli.service.constants import PARAM_CLOUD, PARAM_CLOUD_IDENTIFIER, \
    PARAM_ENABLED, PARAM_TRUSTED_ROLE_ARN


@click.group(name='credentials')
def credentials():
    """Manages Custodian Service Credentials Manager configs"""


@credentials.command(cls=ViewCommand, name='describe')
@click.option('--cloud', '-c', type=str,
              help='The cloud to which the credentials configuration belongs.')
@account_option
@cli_response(attributes_order=[PARAM_CLOUD_IDENTIFIER,
                                PARAM_CLOUD,
                                PARAM_ENABLED,
                                PARAM_TRUSTED_ROLE_ARN])
def describe(ctx: ContextObj, cloud, account_number):
    """
    Describes Custodian Service Credentials Manager configurations.
    """
    return ctx['api_client'].credentials_manager_get(
        cloud=cloud,
        cloud_identifier=account_number
    )


@credentials.command(cls=ViewCommand, name='add')
@click.option('--cloud', '-c', type=click.Choice(['AWS', 'AZURE', 'GCP']),
              required=True,
              help='The cloud to which the credentials configuration belongs.')
@build_account_option(required=True)
@click.option('--trusted_role_arn', '-tra', type=str, required=False,
              help='Account role to assume')
@click.option('--enabled', '-e', type=bool, required=False,
              default=False, show_default=True,
              help="Enable or disable credentials, if not specified: disabled")
@cli_response(attributes_order=[PARAM_CLOUD_IDENTIFIER,
                                PARAM_CLOUD,
                                PARAM_ENABLED,
                                PARAM_TRUSTED_ROLE_ARN])
def add(ctx: ContextObj, cloud, account_number, trusted_role_arn, enabled):
    """
    Creates Custodian Service Credentials Manager configuration.
    """

    return ctx['api_client'].credentials_manager_post(
        cloud=cloud,
        cloud_identifier=account_number,
        trusted_role_arn=trusted_role_arn,
        enabled=enabled
    )


@credentials.command(cls=ViewCommand, name='update')
@click.option('--cloud', '-c', type=str, required=True,
              help='The cloud to which the credentials configuration belongs.')
@build_account_option(required=True)
@click.option('--trusted_role_arn', '-tra', type=str, required=False,
              help=f'Account role to assume')
@click.option('--enabled', '-e', type=bool, default=False,
              help='Enable or disable credentials actuality')
@cli_response(attributes_order=[PARAM_CLOUD_IDENTIFIER,
                                PARAM_CLOUD,
                                PARAM_ENABLED,
                                PARAM_TRUSTED_ROLE_ARN])
def update(ctx: ContextObj, cloud, account_number,
           trusted_role_arn, enabled):
    """
    Updates Custodian Service Credentials Manager configuration.
    """
    return ctx['api_client'].credentials_manager_patch(
        cloud=cloud.lower(),
        cloud_identifier=account_number,
        trusted_role_arn=trusted_role_arn,
        enabled=enabled)


@credentials.command(cls=ViewCommand, name='delete')
@click.option('--cloud', '-c', type=str, required=True,
              help='The cloud to which the credentials configuration belongs.')
@build_account_option(required=True)
@cli_response()
def delete(ctx: ContextObj, cloud, account_number):
    """
    Deletes Custodian Service Credentials Manager configuration.
    """
    return ctx['api_client'].credentials_manager_delete(
        cloud=cloud,
        cloud_identifier=account_number
    )
