import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response,
    service_job_from_date_option, service_job_to_date_option,
    get_service_job_status,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import ServiceJobType


@click.group(name='metadata')
def metadata():
    """Manages locally stored metadata"""


@metadata.command(
    cls=ViewCommand, 
    name='update',
)
@cli_response(
    hint="Use 'sre metadata update_status' to check execution status",
)
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


@metadata.command(
    cls=ViewCommand, 
    name='update_status',
)
@service_job_from_date_option
@service_job_to_date_option
@cli_response()
def update_status(
    ctx: ContextObj,
    from_date: str | None,
    to_date: str | None,
    customer_id: str | None = None,
) -> SREResponse:
    """Execution status of the last metadata update"""
    return get_service_job_status(
        ctx=ctx,
        service_job_type=ServiceJobType.UPDATE_METADATA.value,
        from_date=from_date,
        to_date=to_date,
    )
