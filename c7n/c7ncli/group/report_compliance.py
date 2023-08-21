import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, \
    build_tenant_option, build_job_id_option, optional_job_type_option


@click.group(name='compliance')
def compliance():
    """Describes compliance reports"""


@compliance.command(cls=ViewCommand, name='jobs')
@build_job_id_option(required=True)
@optional_job_type_option
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@cli_response()
def jobs(ctx: ContextObj, job_id: str, job_type: str, href: bool):
    """
    Describes job compliance reports
    """
    return ctx['api_client'].report_compliance_get(
        job_id=job_id, job_type=job_type, href=href, jobs=True
    )


@compliance.command(cls=ViewCommand, name='accumulated')
@build_tenant_option(required=True)
@click.option('--href', '-hf', is_flag=True, help='Return hypertext reference')
@cli_response(attributes_order=[])
def accumulated(ctx: ContextObj, tenant_name: str, href: bool):
    """
    Describes tenant-specific compliance report
    """

    return ctx['api_client'].report_compliance_get(
        tenant_name=tenant_name,
        href=href,
        jobs=False
    )
