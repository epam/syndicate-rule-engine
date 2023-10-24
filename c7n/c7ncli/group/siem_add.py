import click

from c7ncli.group import cli_response, ViewCommand, tenant_option, ContextObj
from c7ncli.service.constants import \
    PARAM_PRODUCT_TYPE_NAME, PARAM_PRODUCT_NAME, PARAM_ENGAGEMENT_NAME, \
    PARAM_TEST_TITLE, PARAM_DOJO_APIKEY, PARAM_DOJO_HOST, PARAM_DOJO_USER, \
    PARAM_DOJO_UPLOAD_FILES, PARAM_DOJO_DISPLAY_ALL_FIELDS, \
    PARAM_DOJO_RESOURCE_PER_FINDING


@click.group(name='add')
def add():
    """Manages SIEM configuration create action"""


@add.command(cls=ViewCommand, name='dojo')
@tenant_option
@click.option('--host', '-h', type=str, required=True,
              help='DefectDojo host:port')
@click.option('--api_key', '-key', type=str, required=True,
              help='DefectDojo API key')
@click.option('--user', '-u', type=str, required=True, default='admin',
              help='DefectDojo user name')
@click.option('--display_all_fields', '-ALL', is_flag=True, default=False,
              help='Flag for displaying all fields')
@click.option('--upload_files', '-U', is_flag=True, default=False,
              help='Flag for displaying a file for each resource with its '
                   'full description in the \"file\" field')
@click.option('--product_type_name', type=str,
              help='DefectDojo\'s product type name. Customer\'s name will '
                   'be used by default: \'{customer}\'')
@click.option('--product_name', type=str,
              help='DefectDojo\'s product name. '
                   'Tenant and account names will be used by '
                   'default: \'{tenant} - {account}\'')
@click.option('--engagement_name', type=str,
              help='DefectDojo\'s engagement name. Account name and day\'s '
                   'date scope will be used by default: '
                   '\'{account}: {day_scope}\'')
@click.option('--test_title', type=str,
              help='Tests\' title name in DefectDojo. Job\'s date scope '
                   'and job id will be used by default: '
                   '\'{job_scope}: {job_id}\'')
@click.option('--resource_per_finding', is_flag=True,
              help='Specify if you want each finding to represent a separate '
                   'violated resource')
@cli_response(secured_params=['api_key'])
def dojo(ctx: ContextObj, tenant_name, host, api_key, user, display_all_fields,
         upload_files, product_type_name, product_name, engagement_name,
         test_title, resource_per_finding):
    """
    Adds dojo configuration. When you specify '--product_type_name',
    '--product_name', '--engagement_name', '--test_title', you can use these
    special key-words: 'customer', 'tenant', 'job_id', 'day_scope',
    'job_scope' inside curly braces to map the entities.
    Example: 'c7n siem add dojo ... --product_name
    "Product {tenant}: {day_scope}"'
    """
    entities_mapping = {
        PARAM_PRODUCT_TYPE_NAME: product_type_name,
        PARAM_PRODUCT_NAME: product_name,
        PARAM_ENGAGEMENT_NAME: engagement_name,
        PARAM_TEST_TITLE: test_title
    }
    entities_mapping = {k: v for k, v in entities_mapping.items() if v}
    return ctx['api_client'].siem_dojo_post(
        tenant_name=tenant_name,
        configuration={
            PARAM_DOJO_HOST: host,
            PARAM_DOJO_APIKEY: api_key,
            PARAM_DOJO_USER: user,
            PARAM_DOJO_DISPLAY_ALL_FIELDS: display_all_fields,
            PARAM_DOJO_UPLOAD_FILES: upload_files,
            PARAM_DOJO_RESOURCE_PER_FINDING: resource_per_finding
        }, entities_mapping=entities_mapping)


@add.command(cls=ViewCommand, name='security_hub')
@tenant_option
@click.option('--region', '-r', type=str, required=True,
              help='AWS region name')
@click.option('--product_arn', '-p', type=str, required=True,
              help='ARN of security product')
@click.option('--trusted_role_arn', '-tra', type=str, required=False,
              help='Role that will be assumed to upload findings')
@cli_response()
def security_hub(ctx: ContextObj, tenant_name, region, product_arn,
                 trusted_role_arn):
    """
    Adds security hub configuration.
    """
    configuration = {
        'aws_region': region,
        'product_arn': product_arn,
    }
    if trusted_role_arn:
        configuration['trusted_role_arn'] = trusted_role_arn
    return ctx['api_client'].siem_security_hub_post(
        tenant_name=tenant_name,
        configuration=configuration)
