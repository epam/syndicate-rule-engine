import click
from c7ncli.group import ViewCommand, tenant_option, customer_option, \
    cli_response, ContextObj, response
from c7ncli.service.constants import PARAM_NAME, PARAM_CUSTOMER_NAME, \
    PARAM_TENANT, PARAM_ACCOUNT


@click.group(name='scheduled')
def scheduled():
    """Manages Job submit action"""


@scheduled.command(cls=ViewCommand, name='add')
@tenant_option
@click.option('--schedule', '-sch', type=str, required=True,
              help='Cron or Rate expression: cron(0 20 * * *), '
                   'rate(2 minutes)')
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, '
                   'all available rulesets will be used')
@click.option('--region', '-r', type=str, required=False,
              multiple=True,
              help='Regions to scan. If not specified, '
                   'all active regions will be used')
@click.option('--name', '-n', type=str, required=False,
              help='Name for the scheduled job. Must be unique. If not '
                   'given, will be generated automatically')
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER_NAME,
                                PARAM_TENANT, PARAM_ACCOUNT])
def add(ctx: ContextObj, tenant_name, schedule,
        ruleset, region, name, customer_id):
    """
    Registers a scheduled job
    """
    return ctx['api_client'].scheduled_job_post(
        tenant=tenant_name,
        schedule=schedule,
        target_ruleset=ruleset,
        target_region=region,
        name=name,
        customer=customer_id
    )


@scheduled.command(cls=ViewCommand, name='describe')
@click.option('--name', '-n', type=str, required=False,
              help='Scheduled job name to remove')
@tenant_option
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER_NAME,
                                PARAM_TENANT, PARAM_ACCOUNT])
def describe(ctx: ContextObj, name, tenant_name, customer_id):
    """
    Describes registered scheduled jobs
    """
    if name and (tenant_name or customer_id):
        return response('You don`t have to specify other attributes if'
                        ' \'--name\' is specified')
    if name:
        return ctx['api_client'].scheduled_job_get(name)
    return ctx['api_client'].scheduled_job_query(
        tenant_name=tenant_name,
        customer=customer_id
    )


@scheduled.command(cls=ViewCommand, name='delete')
@click.option('--name', '-n', type=str, required=True,
              help='Scheduled job name to remove')
@customer_option
@cli_response()
def delete(ctx: ContextObj, name, customer_id):
    """
    Removes a scheduled job
    """
    return ctx['api_client'].scheduled_job_delete(name=name)


@scheduled.command(cls=ViewCommand, name='update')
@click.option('--name', '-n', type=str, required=True,
              help='Scheduled job name to update')
@click.option('--schedule', '-sch', type=str, required=False,
              help='Cron or Rate expression: cron(0 20 * * *), '
                   'rate(2 minutes)')
@click.option('--enabled', '-e', type=bool, required=False,
              help='Param to enable or disable the job temporarily')
@customer_option
@cli_response(attributes_order=[PARAM_NAME, PARAM_CUSTOMER_NAME,
                                PARAM_TENANT, PARAM_ACCOUNT])
def update(ctx: ContextObj, name, schedule, enabled, customer_id):
    """
    Updates an existing scheduled job
    """
    if all(param is None for param in (enabled, schedule)):
        return response('You must specify at least one parameter to update.')
    return ctx['api_client'].scheduled_job_update(
        name=name, schedule=schedule, enabled=enabled, customer=customer_id
    )
