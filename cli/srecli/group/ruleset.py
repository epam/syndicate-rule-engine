import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group.ruleset_eventdriven import eventdriven
from srecli.service.constants import RULE_CLOUDS

attributes_order = 'name', 'version', 'cloud', 'licensed', 'license_manager_id'


@click.group(name='ruleset')
def ruleset():
    """Manages Customer rulesets"""


@ruleset.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, required=False, help='Ruleset name')
@click.option('--version', '-v', type=str, required=False,
              help='Ruleset version')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              help='Cloud name to filter rulesets')
@click.option('--get_rules', '-r', is_flag=True,
              help='If specified, ruleset\'s rules ids will be returned. '
                   'MAKE SURE to use \'--json\' flag to get a clear output ')
@click.option('--licensed', '-ls', type=bool,
              help='If True, only licensed rule-sets are returned. '
                   'If False, only standard rule-sets')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, name, version, cloud, get_rules,
             customer_id, licensed):
    """
    Describes Customer rulesets
    """
    if version and not name:
        raise click.ClickException(
            'The attribute \'--name\' is required if '
            '\'--version\' is specified'
        )
    return ctx['api_client'].ruleset_get(
        name=name,
        version=version,
        cloud=cloud,
        customer_id=customer_id,
        get_rules=get_rules,
        licensed=licensed
    )


@ruleset.command(cls=ViewCommand, name='add')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=True, help='Ruleset cloud')
@click.option('--version', '-v', type=str,
              help='Ruleset version to create. If not specified, will be '
                   'resolved automatically based on the previous ruleset '
                   'with the same name or based on rulesource of type '
                   'GITHUB_RELEASE')
@click.option('--rule', '-r', multiple=True, type=str, required=False,
              help='Rule ids to attach to the ruleset. '
                   'Multiple ids can be specified')
@click.option('--exclude_rule', '-er', multiple=True, required=False,
              type=str, help='Rules to exclude from the ruleset')
@click.option('--rule_source_id', '-rsid', required=False, type=str,
              help='The id of rule source object to use rules from')
@click.option('--git_project_id', '-pid', required=False, type=str,
              help='Project id of git repo to build a ruleset')
@click.option('--git_ref', '-gr', required=False, type=str,
              help='Branch of git repo to build a ruleset')
@click.option('--platform', required=False, type=click.Choice(('kubernetes', 'openshift', 'kubernetes and openshift')),
              multiple=True, help='Platform for k8s')
@click.option('--category', required=False, type=str, multiple=True,
              help='Rules category to use')
@click.option('--service_section', required=False, type=str, multiple=True,
              help='Rules service section to use')
@click.option('--source', required=False, type=str, multiple=True,
              help='Rules source to use')
@cli_response(attributes_order=attributes_order)
def add(ctx: ContextObj, name: str, version: str, cloud: str, rule: tuple,
        exclude_rule: tuple, rule_source_id: str, git_project_id: str, git_ref: str,
        platform: tuple[str], category: tuple[str], service_section: tuple[str],
        source: tuple[str], customer_id: str):
    """
    Creates Customers ruleset.
    """
    if git_ref and not git_project_id:
        raise click.ClickException(
            '--git_project_id must be provided with --git_ref'
        )
    if rule_source_id and (git_ref or git_project_id):
        raise click.ClickException(
            'do not provide --git_ref or --git_project_id if '
            '--rule_source_id is specified'
        )
    return ctx['api_client'].ruleset_post(
        name=name,
        version=version,
        cloud=cloud,
        rules=rule,
        excluded_rules=exclude_rule,
        rule_source_id=rule_source_id,
        git_project_id=git_project_id,
        git_ref=git_ref,
        platforms=platform,
        categories=category,
        service_sections=service_section,
        sources=source,
        customer_id=customer_id
    )


@ruleset.command(cls=ViewCommand, name='update')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=str, required=False,
              help='Ruleset version to update. If not specified, '
                   'the latest version will be updated')
@click.option('--new_version', '-nv', type=str, required=True,
              help='New version for the updated ruleset')
@click.option('--attach_rules', '-ar', multiple=True, required=False,
              help='Rule ids to attach to the ruleset. '
                   'Multiple values allowed')
@click.option('--detach_rules', '-dr', multiple=True, required=False,
              help='Rule ids to detach from the ruleset. '
                   'Multiple values allowed')
@click.option('--force', '-f', is_flag=True, default=False,
              help='If specified the new version of ruleset will be created '
                   'even if there are no changes')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, customer_id, name, version, new_version, 
           attach_rules, detach_rules, force):
    """
    Updates Customers ruleset.
    """
    return ctx['api_client'].ruleset_update(
        name=name,
        version=version,
        new_version=new_version,
        rules_to_attach=attach_rules,
        rules_to_detach=detach_rules,
        customer_id=customer_id,
        force=force
    )


@ruleset.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=str, required=True,
              help='Ruleset version to remove. Specify * to remove all '
                   'the versions of a specific ruleset')
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


@ruleset.command(cls=ViewCommand, name='release')
@click.option('--name', '-n', type=str, required=True, help='Ruleset name')
@click.option('--version', '-v', type=str, required=False,
              help='Ruleset version to release. Specify * to release all '
                   'the versions of a specific ruleset. If not specified, '
                   'the latest version is released')
@click.option('--display_name', '-dn', type=str, required=True,
              help='Ruleset display name')
@click.option('--description', '-d', type=str, required=True,
              help='Ruleset description')
@cli_response()
def release(ctx: ContextObj, customer_id, name, version, display_name,
            description):
    """
    Released a specific version of ruleset
    """
    return ctx['api_client'].ruleset_release(
        customer_id=customer_id,
        name=name,
        version=version,
        display_name=display_name,
        description=description
    )


ruleset.add_command(eventdriven)
