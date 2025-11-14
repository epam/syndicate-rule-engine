import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response
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

