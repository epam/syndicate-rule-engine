import click

from srecli.group import ContextObj, ViewCommand, build_tenant_option, \
    cli_response, response, convert_in_lower_case_if_present
from srecli.group.report_compliance import compliance
from srecli.group.report_details import details
from srecli.group.report_digests import digests
from srecli.group.report_errors import errors
from srecli.group.report_findings import findings
from srecli.group.report_push import push
from srecli.group.report_raw import raw
from srecli.group.report_resource import resource
from srecli.group.report_rules import rules

START_END_DATES_MISSING_MESSAGE = 'At least either --start_date and' \
                                  ' --end_date should be specified'
REPORT_JOB_MISSING_PARAMS_MESSAGE = 'Either \'--account_name\' or ' \
                                    '\'--job_id\' must be specified'


@click.group(name='report')
def report():
    """Manages Custodian Service reports"""


@report.command(cls=ViewCommand, name='operational')
@build_tenant_option(required=True, multiple=True)
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ('OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'RULE', 'ATTACK_VECTOR',
     'FINOPS', 'KUBERNETES', 'DEPRECATIONS')),
              required=False, help='Report type')
@click.option('--receiver', '-r', multiple=True, type=str,
              help='Emails that will receive this notification')
@cli_response()
def operational(ctx: ContextObj, tenant_name, report_types, customer_id,
                receiver):
    """
    Retrieves operational-level reports
    """
    res = ctx['api_client'].operational_report_post(
        tenant_names=tenant_name,
        types=report_types,
        receivers=receiver,
        customer_id=customer_id
    )
    if not res.ok:
        return res
    report_id = res.data.get('data', {}).get('report_id')
    if not report_id:
        return res
    return response(
        f'To see job status, call the `sre report status -id {report_id}`'
    )


@report.command(cls=ViewCommand, name='project')
@click.option('--tenant_display_name', '-tdn', type=str, required=True,
              callback=convert_in_lower_case_if_present,
              help='Display name of tenants to use for project report')
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ('OVERVIEW', 'RESOURCES', 'COMPLIANCE', 'ATTACK_VECTOR', 'FINOPS')),
              required=False, help='Report type')
@click.option('--receiver', '-r', multiple=True, type=str,
              help='Emails that will receive this notification')
@cli_response()
def project(ctx: ContextObj, report_types, tenant_display_name, customer_id,
            receiver):
    """
    Retrieves project-level reports for tenants
    """
    res = ctx['api_client'].project_report_post(
        tenant_display_names=[tenant_display_name],
        types=report_types,
        receivers=receiver,
        customer_id=customer_id
    )
    if not res.ok:
        return res
    report_id = res.data.get('data', {}).get('report_id')
    if not report_id:
        return res
    return response(
        f'To see job status, call the `sre report status -id {report_id}`'
    )


@report.command(cls=ViewCommand, name='department')
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ('TOP_RESOURCES_BY_CLOUD', 'TOP_TENANTS_RESOURCES',
     'TOP_TENANTS_COMPLIANCE', 'TOP_COMPLIANCE_BY_CLOUD',
     'TOP_TENANTS_ATTACKS', 'TOP_ATTACK_BY_CLOUD')), required=False,
              help='Report type')
@cli_response()
def department(ctx: ContextObj, report_types, customer_id):
    """
    Retrieves department-level reports
    """
    res = ctx['api_client'].department_report_post(
        types=report_types,
        customer_id=customer_id
    )
    if not res.ok:
        return res
    report_id = res.data.get('data', {}).get('report_id')
    if not report_id:
        return res
    return response(
        f'To see job status, call the `sre report status -id {report_id}`'
    )


@report.command(cls=ViewCommand, name='clevel')
@click.option('--report_types', '-rt', multiple=True, type=click.Choice(
    ('OVERVIEW', 'COMPLIANCE', 'ATTACK_VECTOR')), required=False,
              help='Report type')
@click.option('--receiver', '-r', multiple=True, type=str,
              help='Emails that will receive this notification')
@cli_response()
def clevel(ctx: ContextObj, report_types, receiver, customer_id):
    """
    Retrieves c-level reports
    """
    res = ctx['api_client'].c_level_report_post(
        types=report_types,
        customer_id=customer_id,
        receivers=receiver
    )
    if not res.ok:
        return res
    report_id = res.data.get('data', {}).get('report_id')
    if not report_id:
        return res
    return response(
        f'To see job status, call the `sre report status -id {report_id}`'
    )


@report.command(cls=ViewCommand, name='diagnostic')
@cli_response()
def diagnostic(ctx: ContextObj, customer_id):
    """
    Retrieves diagnostic reports
    """
    return ctx['api_client'].diagnostic_report_get(
        customer_id=customer_id
    )


@report.command(cls=ViewCommand, name='status')
@click.option('--job_id', '-id', type=str, required=True,
              help='Report job type')
@click.option('--full', '-f', is_flag=True,
              help='Flag to list all attempts related to specified ID')
@cli_response()
def status(ctx: ContextObj, job_id, full, customer_id):
    """
    Retrieves report status by its ID
    """
    return ctx['api_client'].report_status_get(
        job_id=job_id,
        complete=full,
        customer_id=customer_id
    )


report.add_command(digests)
report.add_command(details)
report.add_command(compliance)
report.add_command(errors)
report.add_command(rules)
report.add_command(push)
report.add_command(resource)
report.add_command(findings)
report.add_command(raw)
