import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    build_background_job_status_command
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import BackgroundJobName


@click.group(name='metadata')
def metadata():
    """Manages locally stored metadata"""


@metadata.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response()
def update(
    ctx: ContextObj,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Triggers a complete synchronization with License Manager for all customers.
    
    This updates:
    - Rule metadata (severity, category, MITRE mappings, standards, remediation)
    - License information (description, allowance, event_driven settings, validity dates)
    - Rulesets (downloads compiled ruleset files from License Manager to S3)
    - Ruleset metadata in DB (versions, license associations)
    """
    return ctx['api_client'].trigger_metadata_update()


build_background_job_status_command(
    group=metadata,
    background_job_name=BackgroundJobName.METADATA,
    help_text='Execution status of the last metadata update',
)
