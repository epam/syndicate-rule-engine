from typing import Optional

import click

from c7ncli.group import cli_response, ViewCommand, \
    customer_option, response, ContextObj, \
    build_rule_source_id_option
from c7ncli.service.constants import GIT_ACCESS_TOKEN, \
    PARAM_GIT_URL, PARAM_GIT_REF, PARAM_GIT_ACCESS_TYPE, \
    PARAM_GIT_RULES_PREFIX, PARAM_ID, PARAM_CUSTOMER, \
    PARAM_LAST_SYNC_CURRENT_STATUS, PARAM_GIT_PROJECT_ID

response_attributes_order = [
    PARAM_ID, PARAM_CUSTOMER, PARAM_GIT_PROJECT_ID,
    PARAM_GIT_URL, PARAM_GIT_REF,
    PARAM_GIT_RULES_PREFIX, PARAM_GIT_ACCESS_TYPE,
    PARAM_LAST_SYNC_CURRENT_STATUS,
]

mapped_notation_template = 'Value of \'{}\' parameter' \
                           ' must adhere to the following notation: ' \
                           '$tenant:$account1,$account2'
NULL_KEY = ''


@click.group(name='rulesource')
def rulesource():
    """Manages Rule Source entity"""


@rulesource.command(cls=ViewCommand, name='describe')
@build_rule_source_id_option(required=False)
@click.option('--git_project_id', '-gpid', type=str, required=False,
              help='Git project id to describe rule source')
@customer_option
@cli_response(attributes_order=response_attributes_order)
def describe(ctx: ContextObj, rule_source_id: Optional[str] = None,
             customer_id=None, git_project_id=None):
    """
    Describes rule source
    """
    return ctx['api_client'].rule_source_get(
        rule_source_id=rule_source_id,
        customer=customer_id,
        git_project_id=git_project_id
    )


@rulesource.command(cls=ViewCommand, name='add')
@click.option('--git_project_id', '-gpid', type=str, required=True,
              help='GitLab Project id')
@click.option('--git_url', '-gurl', type=str,
              help=f'Link to GitLab repository with c7n rules',
              show_default=True)
@click.option('--git_ref', '-gref', type=str, default='main',
              show_default=True, help='Name of the branch to grab rules from')
@click.option('--git_rules_prefix', '-gprefix', type=str, default='/',
              help='Rules path prefix', show_default=True)
# todo uncomment when other access types are available
# @click.option('--git_access_type', '-gtype',
#               type=click.Choice(AVAILABLE_GIT_ACCESS_TYPES),
#               default=GIT_ACCESS_TOKEN, show_default=True)
@click.option('--git_access_secret', '-gsecret', type=str,
              help='Secret token to be able to access the repository')
@click.option('--allow_tenant', '-at', type=str, multiple=True,
              help='Allow ruleset for tenant. '
                   'Your user must have access to tenant')
@click.option('--description', '-d', type=str, required=True,
              help='Human-readable description or the repo')
@customer_option
@cli_response(attributes_order=response_attributes_order,
              secured_params=['git_access_secret'])
def add(ctx: ContextObj, git_project_id, git_url, git_ref, git_rules_prefix,
        git_access_secret, allow_tenant, description, customer_id):
    """
    Creates rule source
    """

    return ctx['api_client'].rule_source_post(
        git_project_id=git_project_id,
        git_url=git_url,
        git_ref=git_ref,
        git_rules_prefix=git_rules_prefix,
        git_access_type=GIT_ACCESS_TOKEN,
        git_access_secret=git_access_secret,
        tenant_allowance=allow_tenant,
        description=description,
        customer=customer_id
    )


@rulesource.command(cls=ViewCommand, name='update')
@build_rule_source_id_option(required=True)
@click.option('--git_access_secret', '-gsecret', type=str, required=False)
@click.option('--allow_tenant', '-at', type=str, multiple=True,
              help='Allow ruleset for tenant. '
                   'Your user must have access to tenant')
@click.option('--restrict_tenant', '-rt', type=str, multiple=True,
              help='Restrict ruleset for tenant. '
                   'Your user must have access to tenant')
@click.option('--description', '-d', type=str,
              help='Human-readable description or the repo')
@customer_option
@cli_response(attributes_order=response_attributes_order)
def update(ctx: ContextObj, rule_source_id,
           git_access_secret, allow_tenant,
           restrict_tenant, description, customer_id):
    """Updates rule source"""

    if not (git_access_secret or allow_tenant or restrict_tenant or
            description):
        return response(
            'At least one of these parameters must be given'
            ': \'--git_access_secret\', '
            '\'--allow_tenant\', \'--description\' or '
            '\'--restrict_tenant\'.')
    return ctx['api_client'].rule_source_patch(
        id=rule_source_id,
        git_access_type=GIT_ACCESS_TOKEN,
        git_access_secret=git_access_secret,
        tenant_allowance=list(allow_tenant),
        tenant_restriction=list(restrict_tenant),
        customer=customer_id,
        description=description
    )


@rulesource.command(cls=ViewCommand, name='delete')
@build_rule_source_id_option(required=True)
@customer_option
@cli_response()
def delete(ctx: ContextObj, rule_source_id, customer_id):
    """
    Deletes rule source
    """
    return ctx['api_client'].rule_source_delete(
        rule_source_id=rule_source_id,
        customer=customer_id
    )
