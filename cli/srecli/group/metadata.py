import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    DYNAMIC_DATE_ONLY_EXAMPLE, DYNAMIC_DATE_ONLY_PAST_EXAMPLE
)


@click.group(name='metadata')
def metadata():
    """Manages locally stored metadata"""


@metadata.command(cls=ViewCommand, name='update')
@cli_response()
def update(ctx: ContextObj, customer_id: str | None = None):
    """
    Triggers a complete synchronization with License Manager for all customers.
    
    This updates:
    - Rule metadata (severity, category, MITRE mappings, standards, remediation)
    - License information (description, allowance, event_driven settings, validity dates)
    - Rulesets (downloads compiled ruleset files from License Manager to S3)
    - Ruleset metadata in DB (versions, license associations)
    """
    return ctx['api_client'].trigger_metadata_update()


@metadata.command(cls=ViewCommand, name='status')
@click.option('--from_date', '-from', type=str,
              help='Query metadata statuses from this date. Accepts date ISO '
                   f'string. Example: {DYNAMIC_DATE_ONLY_PAST_EXAMPLE}')
@click.option('--to_date', '-to', type=str,
              help='Query metadata statuses till this date. Accepts date ISO '
                   f'string. Example: {DYNAMIC_DATE_ONLY_EXAMPLE}')
@cli_response()
def status(
    ctx: ContextObj,
    from_date: str,
    to_date: str,
    customer_id: str | None = None,
):
    """
    Execution status of the last metadata update
    """
    params = {
        'from': from_date,
        'to': to_date
    }
    return ctx['api_client'].background_job_status(
        background_job_name='metadata',
        **params
    )
