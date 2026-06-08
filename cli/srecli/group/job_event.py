import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group.job_event_maestro import maestro
from srecli.service.adapter_client import SREResponse
from srecli.service.event_files import (
    events_file_option,
    load_events_from_file,
)


@click.group(name='event', hidden=True)
def event():
    """Manages Job submit action"""


@event.command(cls=ViewCommand, name='cloudtrail')
@events_file_option(
    help=(
        'JSON file with an array of EventBridge events '
        '(detail-type "AWS API Call via CloudTrail"; passed as-is to the API)'
    ),
)
@cli_response()
def cloudtrail(
    ctx: ContextObj,
    events_file: str,
    customer_id: str,
) -> SREResponse:
    """
    Simulate event-driven ingest from CloudTrail/EventBridge (vendor AWS).
    Events are loaded from a JSON file, same as ``event maestro``.
    """
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='AWS',
        events=load_events_from_file(events_file),
        customer_id=customer_id,
    )


@event.command(cls=ViewCommand, name='k8s')
@events_file_option(
    help=(
        'JSON file with an array of SRE_K8S_AGENT events '
        '(type, reason, platformId, metadata; passed as-is to the API)'
    ),
)
@cli_response()
def k8s(
    ctx: ContextObj,
    events_file: str,
    customer_id: str,
) -> SREResponse:
    """Send SRE_K8S_AGENT events to POST /event (event-driven ingest)."""
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='SRE_K8S_AGENT',
        events=load_events_from_file(events_file),
        customer_id=customer_id,
    )


event.add_command(maestro)
