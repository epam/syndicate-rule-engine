import click
import uuid
from typing import Any

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.service.helpers import normalize_lists
from srecli.service.constants import Cloud


@click.group(name="maestro")
def maestro():
    """Builds maestro audit events by cloud vendor"""


def _build_maestro_record(
    tenant_name: str,
    region_name: str,
    cloud: Cloud,
    event_source: str | None = None,
    event_name: str | None = None,
) -> dict[str, Any]:
    """
    Only necessary attributes are kept
    :param tenant_name:
    :param cloud:
    :return:
    """
    if cloud == Cloud.AWS and (event_source is None or event_name is None):
        raise click.ClickException("Event source and event name are required for AWS")
    return {
        "_id": str(uuid.uuid4()),
        "tenantName": tenant_name,
        "regionName": region_name,
        "eventMetadata": {
            "request": {"cloud": cloud},
            # todo native maestro events have string here
            "cloud": cloud.value,
            "eventSource": event_source,
            "eventName": event_name,
        },
    }


def _maestro_cloud_options(f):
    """Shared options for maestro aws/azure/google commands."""
    f = click.option(
        "--tenant_name",
        "-tn",
        type=str,
        required=True,
        multiple=True,
    )(f)
    f = click.option(
        "--region_name",
        "-rn",
        type=str,
        required=True,
        default="eu-west-1",
        show_default=True,
    )(f)
    return f


def _maestro_event_metadata_options(f):
    """Shared metadata options for maestro cloud commands."""
    f = click.option(
        "--event_source",
        "-es",
        type=str,
        required=True,
        multiple=True,
    )(f)
    f = click.option(
        "--event_name",
        "-en",
        type=str,
        required=True,
        multiple=True,
    )(f)
    return f


def _run_maestro_events(
    ctx: ContextObj,
    tenant_name: tuple,
    region_name: str,
    cloud: Cloud,
    customer_id: str,
    event_source: tuple | None = None,
    event_name: tuple | None = None,
):
    tn_list = list(tenant_name)
    rn_list = [region_name]
    es_list = list(event_source) if event_source else None
    en_list = list(event_name) if event_name else None

    to_normalize = [
        l
        for l in [tn_list, rn_list, es_list, en_list]
        if l is not None
    ]
    normalize_lists(to_normalize)

    events = []
    for i in range(len(tn_list)):
        events.append(
            _build_maestro_record(
                tenant_name=tn_list[i],
                region_name=rn_list[i],
                cloud=cloud,
                event_source=es_list[i] if es_list else None,
                event_name=en_list[i] if en_list else None,
            )
        )
    return ctx["api_client"].event_action(
        version="1.0.0",
        vendor="MAESTRO",
        events=events,
        customer_id=customer_id,
    )


@maestro.command(cls=ViewCommand, name="aws")
@_maestro_cloud_options
@_maestro_event_metadata_options
@cli_response()
def maestro_aws(
    ctx: ContextObj,
    tenant_name: tuple,
    region_name: str,
    event_source: tuple,
    event_name: tuple,
    customer_id: str,
):
    """Builds maestro audit event for AWS"""
    return _run_maestro_events(
        ctx=ctx,
        tenant_name=tenant_name,
        region_name=region_name,
        event_source=event_source,
        event_name=event_name,
        cloud=Cloud.AWS,
        customer_id=customer_id,
    )


@maestro.command(cls=ViewCommand, name="azure")
@_maestro_cloud_options
@_maestro_event_metadata_options
@cli_response()
def maestro_azure(
    ctx: ContextObj,
    tenant_name: tuple,
    region_name: str,
    event_source: tuple,
    event_name: tuple,
    customer_id: str,
):
    """Builds maestro audit event for Azure"""
    return _run_maestro_events(
        ctx=ctx,
        tenant_name=tenant_name,
        region_name=region_name,
        event_source=event_source,
        event_name=event_name,
        cloud=Cloud.AZURE,
        customer_id=customer_id,
    )


@maestro.command(cls=ViewCommand, name="google")
@_maestro_cloud_options
@_maestro_event_metadata_options
@cli_response()
def maestro_google(
    ctx: ContextObj,
    tenant_name: tuple,
    region_name: str,
    event_source: tuple,
    event_name: tuple,
    customer_id: str,
):
    """Builds maestro audit event for Google Cloud"""
    return _run_maestro_events(
        ctx=ctx,
        tenant_name=tenant_name,
        region_name=region_name,
        event_source=event_source,
        event_name=event_name,
        cloud=Cloud.GOOGLE,
        customer_id=customer_id,
    )
