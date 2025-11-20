import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_rule_source_id_option,
    cli_response,
    build_background_job_status_command,
)
from srecli.group import limit_option, next_option
from srecli.service.constants import RULE_CLOUDS, BackgroundJobName

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
@click.option('--rule_source_id', '-rsid', required=False, type=str,
              help='The id of rule source object the rule must belong to')
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id,
             rule_name, cloud, git_project_id, git_ref, rule_source_id,
             limit, next_token):
    """
    Describes rules within your customer
    """
    if rule_source_id and (git_ref or git_project_id):
        raise click.ClickException(
            'do not provide --git_ref or --git_project_id if '
            '--rule_source_id is provided'
        )
    if git_ref and not git_project_id:
        raise click.ClickException(
            '--git_project_id must be provided with --git_ref'
        )
    return ctx['api_client'].rule_get(
        rule=rule_name,
        customer_id=customer_id,
        cloud=cloud,
        git_project_id=git_project_id,
        git_ref=git_ref,
        rule_source_id=rule_source_id,
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


build_background_job_status_command(
    group=rule,
    background_job_name=BackgroundJobName.RULE_SOURCE_SYNC,
    help_text='Execution status of the last rule source sync operation',
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
