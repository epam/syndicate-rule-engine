import click

from srecli.group import (
    cli_response, 
    ViewCommand, 
    ContextObj, 
    build_rule_source_id_option, 
    next_option, 
    limit_option, 
    service_job_from_date_option,
    service_job_to_date_option,
    get_service_job_status,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import ServiceJobType

attributes_order = ('id', 'type', 'description', 'git_project_id', 'git_url',
                    'git_ref', 'git_rules_prefix')


@click.group(name='rulesource')
def rulesource():
    """Manages Rule Source entity"""


@rulesource.command(cls=ViewCommand, name='describe')
@build_rule_source_id_option(required=False)
@click.option('--git_project_id', '-gpid', type=str, required=False,
              help='Git project id to describe rule source')
@click.option('--type', '-t',
              type=click.Choice(('GITHUB', 'GITLAB', 'GITHUB_RELEASE')),
              required=False, help='Rule source type')
@click.option('--has_secret', '-hs', type=bool,
              help='Specify whether returned rule sources should have secrets')
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, rule_source_id, git_project_id, type, limit,
             next_token, has_secret, customer_id):
    """
    Describes rule source
    """
    if rule_source_id:
        return ctx['api_client'].rule_source_get(rule_source_id,
                                                 customer_id=customer_id)
    return ctx['api_client'].rule_source_query(
        git_project_id=git_project_id,
        type=type,
        limit=limit,
        next_token=next_token,
        has_secret=has_secret,
        customer_id=customer_id
    )


@rulesource.command(cls=ViewCommand, name='add')
@click.option('--git_project_id', '-gpid', type=str, required=True,
              help='GitLab Project id')
@click.option('--type', '-t',
              type=click.Choice(('GITHUB', 'GITLAB', 'GITHUB_RELEASE')),
              required=False, help='Rule source type')
@click.option('--git_url', '-gurl', type=str,
              help=f'API endpoint of a Git-based platform that hosts the '
                   f'repository containing the rules',
              show_default=True)
@click.option('--git_ref', '-gref', type=str, default='main',
              show_default=True, help='Name of the branch to grab rules from')
@click.option('--git_rules_prefix', '-gprefix', type=str, default='/',
              help='Rules path prefix', show_default=True)
@click.option('--git_access_secret', '-gsecret', type=str,
              help='Secret token to be able to access the repository')
@click.option('--description', '-d', type=str, required=True,
              help='Human-readable description or the repo')
@cli_response(attributes_order=attributes_order)
def add(ctx: ContextObj, git_project_id, type, git_url, git_ref,
        git_rules_prefix, git_access_secret,  description, customer_id):
    """
    Creates rule source
    """

    return ctx['api_client'].rule_source_post(
        git_project_id=git_project_id,
        type=type,
        git_url=git_url,
        git_ref=git_ref,
        git_rules_prefix=git_rules_prefix,
        git_access_secret=git_access_secret,
        description=description,
        customer_id=customer_id
    )


@rulesource.command(cls=ViewCommand, name='update')
@build_rule_source_id_option(required=True)
@click.option('--git_access_secret', '-gsecret', type=str, required=False)
@click.option('--description', '-d', type=str,
              help='Human-readable description of the repo')
@cli_response(attributes_order=attributes_order)
def update(ctx: ContextObj, rule_source_id,
           git_access_secret, description, customer_id):
    """Updates rule source"""

    if not (git_access_secret or description):
        raise click.ClickException(
            'At least one of these parameters must be given'
            ': \'--git_access_secret\' or \'--description\''
        )
    return ctx['api_client'].rule_source_patch(
        id=rule_source_id,
        git_access_secret=git_access_secret,
        customer_id=customer_id,
        description=description
    )


@rulesource.command(cls=ViewCommand, name='delete')
@build_rule_source_id_option(required=True)
@click.option('--delete_rules', '-dr', is_flag=True,
              help='Whether to remove all rules belonging to this rule source')
@cli_response()
def delete(ctx: ContextObj, rule_source_id, delete_rules, customer_id):
    """
    Deletes rule source
    """
    return ctx['api_client'].rule_source_delete(
        id=rule_source_id,
        delete_rules=delete_rules,
        customer_id=customer_id
    )


@rulesource.command(cls=ViewCommand, name='sync')
@build_rule_source_id_option(required=True)
@cli_response(hint="Use 'sre rulesource sync_status' to check execution status")
def sync(ctx: ContextObj, rule_source_id, customer_id):
    """
    Updates rules for this rule source
    """
    return ctx['api_client'].rule_source_sync(
        id=rule_source_id,
        customer_id=customer_id
    )


@rulesource.command(cls=ViewCommand, name='sync_status')
@service_job_from_date_option
@service_job_to_date_option
@cli_response()
def sync_status(
    ctx: ContextObj,
    from_date: str | None,
    to_date: str | None,
    customer_id: str | None = None,
) -> SREResponse:
    """Execution status of the last rule source sync operation"""
    return get_service_job_status(
        ctx=ctx,
        service_job_type=ServiceJobType.RULE_SOURCE_SYNC.value,
        from_date=from_date,
        to_date=to_date,
    )
