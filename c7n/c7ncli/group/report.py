import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, \
    build_tenant_option
from c7ncli.group import customer_option
from c7ncli.group import tenant_display_name_option
from c7ncli.group.report_compliance import compliance
from c7ncli.group.report_details import details
from c7ncli.group.report_digests import digests
from c7ncli.group.report_errors import errors
from c7ncli.group.report_push import push
from c7ncli.group.report_rules import rules
from c7ncli.group.report_resource import resource

START_END_DATES_MISSING_MESSAGE = 'At least either --start_date and' \
                                  ' --end_date should be specified'
REPORT_JOB_MISSING_PARAMS_MESSAGE = 'Either \'--account_name\' or ' \
                                    '\'--job_id\' must be specified'


@click.group(name='report')
def report():
    """Manages Custodian Service reports"""

@report.command(cls=ViewCommand, name='operational')
@build_tenant_option(required=True)
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'RULE', 'ATTACK_VECTOR',
     'FINOPS']),
              required=False, help='Report type')
@customer_option
@cli_response(attributes_order=[])
def operational(ctx: ContextObj, tenant_name, report_types, customer_id):
    """
    Retrieves operational-level reports
    """
    return ctx['api_client'].operational_report_get(
        tenant_name=tenant_name,
        report_types=', '.join(report_types) if report_types else None,
        customer=customer_id
    )


@report.command(cls=ViewCommand, name='project')
@tenant_display_name_option
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'ATTACK_VECTOR', 'FINOPS']),
              required=False, help='Report type')
@customer_option
@cli_response(attributes_order=[])
def project(ctx: ContextObj, report_types, tenant_display_name, customer_id):
    """
    Retrieves project-level reports for a tenant group
    """
    return ctx['api_client'].project_report_get(
        tenant_display_name=tenant_display_name,
        report_types=', '.join(report_types) if report_types else None,
        customer=customer_id
    )


@report.command(cls=ViewCommand, name='department')
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ['TOP_RESOURCES_BY_CLOUD', 'TOP_TENANTS_RESOURCES',
     'TOP_TENANTS_COMPLIANCE', 'TOP_COMPLIANCE_BY_CLOUD',
     'TOP_TENANTS_ATTACKS', 'TOP_ATTACK_BY_CLOUD']), required=False,
              help='Report type')
@customer_option
@cli_response(attributes_order=[])
def department(ctx: ContextObj, report_types, customer_id):
    """
    Retrieves department-level reports
    """
    return ctx['api_client'].department_report_get(
        report_types=', '.join(report_types) if report_types else None,
        customer=customer_id)


@report.command(cls=ViewCommand, name='clevel')
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ['OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR']), required=False,
              help='Report type')
@customer_option
@cli_response(attributes_order=[])
def clevel(ctx: ContextObj, report_types, customer_id):
    """
    Retrieves c-level reports
    """
    return ctx['api_client'].c_level_report_get(
        report_types=', '.join(report_types) if report_types else None,
        customer=customer_id)


report.add_command(digests)
report.add_command(details)
report.add_command(compliance)
report.add_command(errors)
report.add_command(rules)
report.add_command(push)
report.add_command(resource)
