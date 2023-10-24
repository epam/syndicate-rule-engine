import click
from c7ncli.group import ViewCommand, cli_response, tenant_option, ContextObj
from c7ncli.service.constants import PARAM_DOJO, SECURITY_HUB_COMMAND_NAME


@click.group(name='delete')
def delete():
    """Manages SIEM configuration delete action"""


@delete.command(cls=ViewCommand, name='dojo')
@tenant_option
@cli_response()
def dojo(ctx: ContextObj, tenant_name):
    """
    Deletes DefectDojo SIEM configuration.
    """
    return ctx['api_client'].siem_delete(tenant_name=tenant_name,
                                   configuration_type=PARAM_DOJO)


@delete.command(cls=ViewCommand, name='security_hub')
@tenant_option
@cli_response()
def security_hub(ctx: ContextObj, tenant_name):
    """
    Deletes Security Hub SIEM configuration.
    """
    return ctx['api_client'].siem_delete(
        tenant_name=tenant_name,
        configuration_type=SECURITY_HUB_COMMAND_NAME
    )
