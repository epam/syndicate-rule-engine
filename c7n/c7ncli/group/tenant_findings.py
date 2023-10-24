import click
from click import Choice

from c7ncli.group import cli_response, ViewCommand, response, ContextObj
from c7ncli.group import tenant_option
from c7ncli.service.constants import (
    PARAM_RULES_TO_INCLUDE, PARAM_REGIONS_TO_INCLUDE,
    PARAM_RESOURCE_TYPES_TO_INCLUDE, PARAM_SEVERITIES_TO_INCLUDE,
    PARAM_DATA_TYPE, PARAM_MAP_KEY
)

PARAM_RESOURCES = 'resources'
EXPANSION_PARAMETERS = [PARAM_RESOURCES]
SEVERITY_PARAMETERS = ['High', 'Medium', 'Low', 'Info']
LIST_TYPE = 'list_type'
MAP_TYPE = 'map_type'


@click.group(name='findings')
def findings():
    """Manages Tenant Findings state"""


@findings.command(cls=ViewCommand, name='describe')
@tenant_option
@click.option('--rule', '-rl', type=str, required=False,
              help='Rule to include in a Findings state.')
@click.option('--region', '-r', type=str, required=False,
              help='Region to include in a Findings state.')
@click.option('--resource_type', '-rt', type=str, required=False,
              help='Resource type to include in a Findings state.')
@click.option('--severity', '-s', type=Choice(SEVERITY_PARAMETERS),
              required=False,
              help='Severity values to include in a Findings state.')
@click.option('--subset_targets', '-st', is_flag=True, required=False,
              help='Applies dependent subset inclusion.')
@click.option('--expand', '-exp', type=Choice(EXPANSION_PARAMETERS),
              default=PARAM_RESOURCES,
              help='Expansion parameter to invert Findings collection on.')
@click.option('--mapped', '-map', type=str, required=False,
              help='Applies mapping format of an expanded Findings collection,'
                   ' by a given key, rather than a listed one.')
@click.option('--get_url', '-url', is_flag=True, required=False,
              help='Returns a presigned URL rather than a raw Findings '
                   'collection.')
@click.option('--raw', is_flag=True, default=False, required=False,
              help='Specify if you want to receive raw findings content')
@cli_response()
def describe(ctx: ContextObj, tenant_name, rule, region, resource_type,
             severity, subset_targets, expand, mapped, get_url, raw):
    """
    Describes Findings state of a tenant.
    """
    _filters_relation = zip(
        (PARAM_RULES_TO_INCLUDE, PARAM_REGIONS_TO_INCLUDE,
         PARAM_RESOURCE_TYPES_TO_INCLUDE, PARAM_SEVERITIES_TO_INCLUDE),
        (rule, region, resource_type, severity)
    )

    _filters = {key: value for key, value in _filters_relation if value}

    if not _filters and subset_targets:
        return response('One may apply \'--subset_targets\', given '
                        'either  of the following parameters has been'
                        ' supplied: \'--rule\', \'--region\', '
                        '\'--resource_type\' or \'--severity\'.')

    if mapped:
        _format = {PARAM_DATA_TYPE: MAP_TYPE, PARAM_MAP_KEY: mapped}
    else:
        _format = {PARAM_DATA_TYPE: LIST_TYPE}
    return ctx['api_client'].findings_get(
        tenant_name=tenant_name,
        filter_dict=_filters, expansion=expand,
        dependent=subset_targets, format_dict=_format, get_url=get_url, raw=raw
    )


@findings.command(cls=ViewCommand, name='delete')
@tenant_option
@cli_response()
def delete(ctx: ContextObj, tenant_name):
    """
    Clears Findings state of a tenant.
    """
    return ctx['api_client'].findings_delete(tenant_name=tenant_name)
