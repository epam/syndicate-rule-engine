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

START_END_DATES_MISSING_MESSAGE = 'At least either --start_date and' \
                                  ' --end_date should be specified'
REPORT_JOB_MISSING_PARAMS_MESSAGE = 'Either \'--account_name\' or ' \
                                    '\'--job_id\' must be specified'


@click.group(name='report')
def report():
    """Manages Custodian Service reports"""


# @report.command(cls=ViewCommand, name='reactive')
# @click.option('--start_date', '-s', type=datetime.fromisoformat,
#               help='Generate report FROM date. ISO 8601 format. '
#                    'Example: 2021-09-22T00:00:00.000000')
# @click.option('--end_date', '-e', type=datetime.fromisoformat,
#               help='Generate report TILL date. ISO 8601 format. '
#                    'Example: 2021-09-22T00:00:00.000000')
# @click.option('--tenant', '-t', type=str)
# @customer_option
# @cli_response(attributes_order=[])
# def event_driven(start_date, end_date, tenant, customer):
#     """
#     Describes error report
#     """
#     if not (bool(start_date) or bool(end_date)):
#         return response('You must specify either --start_date or --end_date')
#     from c7ncli.service.initializer import ADAPTER_SDK
#     return ADAPTER_SDK.event_driven_report_get(
#         start=start_date.isoformat() if start_date else None,
#         end=end_date.isoformat() if end_date else None,
#         tenant_name=tenant,
#         customer=customer
#     )

@report.command(cls=ViewCommand, name='operational')
@build_tenant_option(required=True)
@click.option('--report_type', '-rt', type=click.Choice(
    ['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'RULE', 'ATTACK_VECTOR']),
              required=False, help='Report type')
@customer_option
@cli_response(attributes_order=[])
def operational(ctx: ContextObj, tenant_name, report_type, customer_id):
    """
    Retrieves operational-level reports
    """
    return ctx['api_client'].operational_report_get(
        tenant_name=tenant_name, report_type=report_type, customer=customer_id
    )


@report.command(cls=ViewCommand, name='project')
@tenant_display_name_option
@click.option('--report_type', '-rt', type=click.Choice(
    ['OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'ATTACK_VECTOR']), required=False,
              help='Report type')
@customer_option
@cli_response(attributes_order=[])
def project(ctx: ContextObj, report_type, tenant_display_name, customer_id):
    """
    Retrieves project-level reports for a tenant group
    """
    return ctx['api_client'].project_report_get(
        tenant_display_name=tenant_display_name,
        report_type=report_type, customer=customer_id
    )


@report.command(cls=ViewCommand, name='department')
@click.option('--report_type', '-rt', type=click.Choice(
    ['TOP_RESOURCES_BY_CLOUD', 'TOP_TENANTS_RESOURCES',
     'TOP_TENANTS_COMPLIANCE', 'TOP_COMPLIANCE_BY_CLOUD',
     'TOP_TENANTS_ATTACKS', 'TOP_ATTACK_BY_CLOUD']), required=False,
              help='Report type')
@customer_option
@cli_response(attributes_order=[])
def department(ctx: ContextObj, report_type, customer_id):
    """
    Retrieves department-level reports
    """
    return ctx['api_client'].department_report_get(report_type=report_type,
                                                   customer=customer_id)


@report.command(cls=ViewCommand, name='clevel')
@click.option('--report_type', '-rt', type=click.Choice(
    ['OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR']), required=False,
              help='Report type')
@customer_option
@cli_response(attributes_order=[])
def clevel(ctx: ContextObj, report_type, customer_id):
    """
    Retrieves c-level reports
    """
    return ctx['api_client'].c_level_report_get(report_type=report_type,
                                                customer=customer_id)


report.add_command(digests)
report.add_command(details)
report.add_command(compliance)
report.add_command(errors)
report.add_command(rules)
report.add_command(push)
