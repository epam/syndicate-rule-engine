import click

from c7ncli.group import ContextObj, ViewCommand, cli_response, response
from c7ncli.group.ruleset_eventdriven import eventdriven
from c7ncli.service.constants import RULE_CLOUDS

attributes_order = 'name', 'version', 'cloud', 'licensed', 'license_manager_id'


@click.group(name='ruleset')
def ruleset():
    """Manages Customer rulesets"""


@ruleset.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, required=False, help='Ruleset name')
@click.option('--version', '-v', type=float, required=False,
              help='Ruleset version')
@click.option('--active', '-act', help='Filter only active rulesets')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              help='Cloud name to filter rulesets')
@click.option('--get_rules', '-r', is_flag=True,
              help='If specified, ruleset\'s rules ids will be returned. '
                   'MAKE SURE to use \'--json\' flag to get a clear output ')
@click.option('--licensed', '-ls', type=bool,
              help='If True, only licensed rule-sets are returned. '
                   'If False, only standard rule-sets')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, name, version, active, cloud, get_rules,
             customer_id, licensed):
    """
    Describes Customer rulesets
    """
    if version and not name:
        return response('The attribute \'--name\' is required if '
                        '\'--version\' is specified')
    return ctx['api_client'].ruleset_get(
        name=name,
        version=version,
        cloud=cloud,
        customer_id=customer_id,
        get_rules=get_rules,
        active=active,
        licensed=licensed
    )


@ruleset.command(cls=ViewCommand, name='add')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=float, default=1.0,
              help='Ruleset version')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=True, help='Ruleset cloud')
@click.option('--rule', '-r', multiple=True, required=False,
              help='Rule ids to attach to the ruleset. '
                   'Multiple ids can be specified')
@click.option('--git_project_id', '-pid', required=False, type=str,
              help='Project id of git repo to build a ruleset')
@click.option('--git_ref', '-gr', required=False, type=str,
              help='Branch of git repo to build a ruleset')
@click.option('--active', '-act', is_flag=True, required=False, default=False,
              help='Force set ruleset version as active')
@click.option('--standard', '-st', type=str, multiple=True,
              help='Filter rules by the security standard name')
@click.option('--service_section', '-ss', type=str,
              help='Filter rules by the service section')
@click.option('--severity', '-s', type=str,
              help='Filter rules by severity')
@click.option('--mitre', '-m', type=str, multiple=True,
              help='Filter rules by mitre')
@cli_response(attributes_order=attributes_order)
def add(ctx: ContextObj, name: str, version: float, cloud: str, rule: tuple,
        git_project_id: str, git_ref: str,
        active: bool, standard: tuple, service_section: tuple,
        severity: tuple, mitre: tuple,  customer_id: str):
    """
    Creates Customers ruleset.
    """
    if git_ref and not git_project_id:
        return response('--git_project_id must be provided with --git_ref')
    return ctx['api_client'].ruleset_post(
        name=name,
        version=version,
        cloud=cloud,
        rules=rule,
        git_project_id=git_project_id,
        git_ref=git_ref,
        active=active,
        standard=standard,
        service_section=service_section,
        severity=severity,
        mitre=mitre,
        customer_id=customer_id
    )


@ruleset.command(cls=ViewCommand, name='update')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=float, required=True,
              help='Ruleset version')
@click.option('--attach_rules', '-ar', multiple=True, required=False,
              help='Rule ids to attach to the ruleset. '
                   'Multiple values allowed')
@click.option('--detach_rules', '-dr', multiple=True, required=False,
              help='Rule ids to detach from the ruleset. '
                   'Multiple values allowed')
@click.option('--active', '-act', type=bool, required=False,
              help='Force set/unset ruleset version as active')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, customer_id, name, version, attach_rules,
           detach_rules, active):
    """
    Updates Customers ruleset.
    """
    if not (attach_rules or detach_rules or isinstance(active, bool)):
        return response(
            'At least one of the following arguments must be '
            'provided: \'--attach_rules\', \'--detach_rules\','
            ' \'--active\'')

    return ctx['api_client'].ruleset_update(
        name=name,
        version=version,
        rules_to_attach=attach_rules,
        rules_to_detach=detach_rules,
        active=active,
        customer_id=customer_id,
    )


@ruleset.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=float, required=True,
              help='Ruleset version')
@cli_response()
def delete(ctx: ContextObj, customer_id, name, version):
    """
    Deletes Customer ruleset. For successful deletion, the ruleset must be
    inactive
    """
    return ctx['api_client'].ruleset_delete(
        customer_id=customer_id,
        name=name,
        version=version
    )


ruleset.add_command(eventdriven)
