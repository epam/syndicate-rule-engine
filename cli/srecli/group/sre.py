import click

from srecli.group import ContextObj, ViewCommand, cli_response, response
from srecli.group.customer import customer
from srecli.group.integrations import integrations
from srecli.group.job import job
from srecli.group.license import license
from srecli.group.metrics import metrics
from srecli.group.platform import platform
from srecli.group.policy import policy
from srecli.group.report import report
from srecli.group.results import results
from srecli.group.role import role
from srecli.group.rule import rule
from srecli.group.ruleset import ruleset
from srecli.group.rulesource import rulesource
from srecli.group.setting import setting
from srecli.group.tenant import tenant
from srecli.group.users import users
from srecli.service.helpers import validate_api_link, check_version_compatibility
from srecli.service.logger import get_logger
from srecli import __version__


_LOG = get_logger(__name__)


@click.group(name='sre')
@click.version_option(__version__)
def sre():
    """The main click's group to accumulate all the CLI commands"""


@sre.command(cls=ViewCommand, name='configure')
@click.option('--api_link', '-api', type=str,
              help='Link to the Syndicate Rule Engine host.')
@click.option('--items_per_column', '-ipc', type=click.IntRange(min=0),
              help='Specify how many items per table. '
                   'Set `0` to disable the limitation')
@cli_response(check_api_link=False, check_access_token=False, )
def configure(ctx: ContextObj, api_link, items_per_column, **kwargs):
    """
    Configures sre tool to work with Syndicate Rule Engine.
    """
    _is_given = lambda x: x is not None
    if not any(_is_given(param) for param in (api_link, items_per_column)):
        raise click.ClickException('At least one parameter must be provided')
    if api_link:
        message = validate_api_link(api_link)
        if message:
            _LOG.warning(f'invalid link: {message}')
            raise click.ClickException(message)
        ctx['config'].api_link = api_link
    if isinstance(items_per_column, int):
        if items_per_column == 0:
            ctx['config'].items_per_column = None
        else:
            ctx['config'].items_per_column = items_per_column
    return response('Great! The sre cli tool api_link has been configured.')


@sre.command(cls=ViewCommand, name='login')
@click.option('--username', '-u', type=str,
              required=True,
              help='Your username.')
@click.option('--password', '-p', type=str,
              required=True, hide_input=True, prompt=True,
              help='Your password.')
@cli_response(check_access_token=False)
def login(ctx: ContextObj, username: str, password: str, **kwargs):
    """
    Authenticates user to work with Syndicate Rule Engine.
    """
    adapter = ctx['api_client']
    resp = adapter.login(username=username, password=password)
    if resp.exc or not resp.ok:
        return resp
    check_version_compatibility(resp.api_version, __version__)

    ctx['config'].access_token = resp.data['access_token']
    if rt := resp.data.get('refresh_token'):
        ctx['config'].refresh_token = rt
    return response('Great! The sre cli tool access token has been saved.')


@sre.command(cls=ViewCommand, name='whoami')
@cli_response(attributes_order=('username', 'customer', 'role', 'latest_login'))
def whoami(ctx: ContextObj, customer_id: str):
    """
    Returns information about the current user
    """
    return ctx['api_client'].whoami()


@sre.command(cls=ViewCommand, name='cleanup')
@cli_response(check_access_token=False, check_api_link=False)
def cleanup(ctx: ContextObj, **kwargs):
    """
    Removes all the configuration data related to the tool.
    """
    ctx['config'].clear()
    return response('The sre cli tool configuration has been deleted.')


@sre.command(cls=ViewCommand, name='health_check')
@click.option('--identifier', '-id', type=str,
              help='Concrete check id to retrieve')
@click.option('--status', '-st',
              type=click.Choice(('OK', 'UNKNOWN', 'NOT_OK')),
              help='Filter checks by status')
@cli_response(attributes_order=('id', 'status', 'details', 'impact',
                                'remediation'))
def health_check(ctx: ContextObj, identifier, status, **kwargs):
    """
    Checks SRE components availability
    """
    if identifier:
        return ctx['api_client'].health_check_get(identifier)
    return ctx['api_client'].health_check_list(status=status)


@sre.command(cls=ViewCommand, name='show_config')
@cli_response()
def show_config(ctx: ContextObj, **kwargs):
    """
    Returns the cli configuration
    """
    return response(dict(ctx['config'].items()))


sre.add_command(customer)
sre.add_command(tenant)
sre.add_command(role)
sre.add_command(policy)
sre.add_command(rule)
sre.add_command(job)
sre.add_command(report)
sre.add_command(ruleset)
sre.add_command(rulesource)
sre.add_command(license)
sre.add_command(setting)
sre.add_command(results)
sre.add_command(metrics)
sre.add_command(platform)
sre.add_command(integrations)
sre.add_command(users)
