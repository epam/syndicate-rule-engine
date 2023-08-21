import click

from c7ncli.service.helpers import validate_api_link
from c7ncli.group import cli_response, ViewCommand, response, ContextObj
from c7ncli.group.application import application
from c7ncli.group.customer import customer
from c7ncli.group.job import job
from c7ncli.group.license import license
from c7ncli.group.metrics import metrics
from c7ncli.group.parent import parent
from c7ncli.group.policy import policy
from c7ncli.group.report import report
from c7ncli.group.results import results
from c7ncli.group.role import role
from c7ncli.group.rule import rule
from c7ncli.group.ruleset import ruleset
from c7ncli.group.rulesource import rulesource
from c7ncli.group.setting import setting
from c7ncli.group.tenant import tenant
from c7ncli.group.trigger import trigger
from c7ncli.group.user import user
from c7ncli.service.logger import get_logger
from c7ncli.version import __version__

SYSTEM_LOG = get_logger(__name__)


@click.group()
@click.version_option(__version__)
def c7n():
    """The main click's group to accumulate all the CLI commands"""


@c7n.command(cls=ViewCommand, name='configure')
@click.option('--api_link', '-api', type=str,
              help='Link to the Custodian as a Service host.')
@click.option('--items_per_column', '-ipc', type=click.IntRange(min=0),
              help='Specify how many items per table. '
                   'Set `0` to disable the limitation')
@cli_response(check_api_link=False, check_access_token=False, )
def configure(ctx: ContextObj, api_link, items_per_column):
    """
    Configures c7n tool to work with Custodian as a Service.
    """
    _is_given = lambda x: x is not None
    if not any(_is_given(param) for param in (api_link, items_per_column)):
        return response('At least one parameter must be provided')
    if api_link:
        message = validate_api_link(api_link)
        if message:
            SYSTEM_LOG.error(message)
            return response(message)
        ctx['config'].api_link = api_link
    if isinstance(items_per_column, int):
        if items_per_column == 0:
            ctx['config'].items_per_column = None
        else:
            ctx['config'].items_per_column = items_per_column
    return response('Great! The c7n tool api_link has been configured.')


@c7n.command(cls=ViewCommand, name='login')
@click.option('--username', '-u', type=str,
              required=True,
              help='Custodian Service username.')
@click.option('--password', '-p', type=str,
              required=True, hide_input=True, prompt=True,
              help='Custodian Service user password.')
@cli_response(check_access_token=False, secured_params=['password'])
def login(ctx: ContextObj, username: str, password: str):
    """
    Authenticates user to work with Custodian as a Service.
    """
    adapter = ctx['api_client']
    _response = adapter.login(username=username, password=password)

    if isinstance(_response, dict):
        return _response
    ctx['config'].access_token = _response
    return response('Great! The c7n tool access token has been saved.')


@c7n.command(cls=ViewCommand, name='cleanup')
@cli_response(check_access_token=False, check_api_link=False)
def cleanup(ctx: ContextObj):
    """
    Removes all the configuration data related to the tool.
    """
    ctx['config'].clear()
    return response('The c7n tool configuration has been deleted.')


@c7n.command(cls=ViewCommand, name='health_check')
@click.option('--identifier', '-id', type=str,
              help='Concrete check id to retrieve')
@click.option('--status', '-st',
              type=click.Choice(['OK', 'UNKNOWN', 'NOT_OK']),
              help='Filter checks by status')
@cli_response(attributes_order=['id', 'status', 'details', 'impact',
                                'remediation'])
def health_check(ctx: ContextObj, identifier, status):
    """
    Checks Custodian Service components availability
    """
    if identifier:
        return ctx['api_client'].health_check_get(identifier)
    return ctx['api_client'].health_check_list(status=status)


@c7n.command(cls=ViewCommand, name='show_config')
@cli_response()
def show_config(ctx: ContextObj):
    """
    Returns the cli configuration
    """
    return response(dict(ctx['config'].items()))


c7n.add_command(customer)
c7n.add_command(tenant)
c7n.add_command(role)
c7n.add_command(policy)
c7n.add_command(rule)
c7n.add_command(job)
c7n.add_command(report)
c7n.add_command(ruleset)
c7n.add_command(rulesource)
c7n.add_command(license)
# c7n.add_command(trigger)
c7n.add_command(user)
c7n.add_command(setting)
c7n.add_command(results)
c7n.add_command(application)
c7n.add_command(parent)
c7n.add_command(metrics)
