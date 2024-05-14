import click

from c7ncli.group import (
    ContextObj,
    ViewCommand,
    build_rule_source_id_option,
    cli_response,
    response,
)
from c7ncli.group import limit_option, next_option
from c7ncli.service.constants import RULE_CLOUDS

attributes_order = ('name', 'cloud', 'resource', 'description', 'branch',
                    'project')


@click.group(name='rule')
def rule():
    """Manages Rule Entity"""


@rule.command(cls=ViewCommand, name='describe')
@click.option('--rule_name', '-r', type=str, required=False,
              help='Rule id to describe')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=False,
              help='Display only rules of specific cloud.')
@click.option('--git_project_id', '-pid', required=False, type=str,
              help='Project id of git repo to build a ruleset')
@click.option('--git_ref', '-gr', required=False, type=str,
              help='Branch of git repo to build a ruleset')
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id,
             rule_name, cloud, git_project_id, git_ref,
             limit, next_token):
    """
    Describes rules within your customer
    """
    if git_ref and not git_project_id:
        return response('--git_project_id must be provided with --git_ref')
    return ctx['api_client'].rule_get(
        rule=rule_name,
        customer_id=customer_id,
        cloud=cloud,
        git_project_id=git_project_id,
        git_ref=git_ref,
        limit=limit,
        next_token=next_token,
    )


@rule.command(cls=ViewCommand, name='update')
@build_rule_source_id_option(required=False)
@cli_response()
def update(ctx: ContextObj, rule_source_id, customer_id):
    """
    Pulls the latest versions of rules within your customer
    """
    return ctx['api_client'].trigger_rule_meta_updater(
        rule_source_id=rule_source_id,
        customer_id=customer_id,
    )


@rule.command(cls=ViewCommand, name='delete')
@click.option('--rule_name', '-r', type=str, required=False,
              help='Rule id to delete')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=False,
              help='Delete only rules of specific cloud.')
@click.option('--git_project_id', '-pid', required=False, type=str,
              help='Project id of git repo to delete rules')
@click.option('--git_ref', '-gr', required=False, type=str,
              help='Branch of git repo to delete rules')
@cli_response()
def delete(ctx: ContextObj, customer_id, rule_name, cloud,
           git_project_id, git_ref):
    """
    Deletes rules within your customer
    """
    return ctx['api_client'].rule_delete(
        customer_id=customer_id,
        rule=rule_name,
        cloud=cloud,
        git_project_id=git_project_id,
        git_ref=git_ref
    )
