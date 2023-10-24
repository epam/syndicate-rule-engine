import click

from c7ncli.group import ViewCommand, cli_response, tenant_option, ContextObj
from c7ncli.service.constants import PARAM_DOJO, SECURITY_HUB_COMMAND_NAME


@click.group(name='describe')
def describe():
    """Manages SIEM configuration describe action"""


@describe.command(cls=ViewCommand, name='dojo')
@tenant_option
@cli_response()
def dojo(ctx: ContextObj, tenant_name):
    """
    Describes DefectDojo SIEM configuration of a customer.
    """
    return ctx['api_client'].siem_get(tenant_name=tenant_name,
                                      configuration_type=PARAM_DOJO)


@describe.command(cls=ViewCommand, name='security_hub')
@tenant_option
@cli_response()
def security_hub(ctx: ContextObj, tenant_name):
    """
    Describes Security Hub SIEM configuration of a customer.
    """
    return ctx['api_client'].siem_get(
        tenant_name=tenant_name,
        configuration_type=SECURITY_HUB_COMMAND_NAME)
