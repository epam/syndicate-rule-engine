import click
import uuid

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.service.helpers import normalize_lists
from srecli.service.constants import Cloud


@click.group(name="maestro")
def maestro():
    """Builds maestro audit events by cloud vendor"""


def _build_maestro_record(
    event_action: str,
    group: str,
    sub_group: str,
    tenant_name: str,
    cloud: Cloud,
    event_source: str | None = None,
    event_name: str | None = None,
):
    """
    Only necessary attributes are kept
    :param event_action:
    :param group:
    :param sub_group:
    :param tenant_name:
    :param cloud:
    :return:
    """
    if cloud == Cloud.AWS and (event_source is None and event_name is None):
        raise click.ClickException("Event source and event name are required for AWS")
    return {
        "_id": str(uuid.uuid4()),
        "eventAction": event_action,
        "group": group,
        "subGroup": sub_group,
        "tenantName": tenant_name,
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
        "--event_action",
        "-ea",
        type=click.Choice(("COMMAND", "CREATE", "DELETE", "DISABLE", "UPDATE")),
        required=True,
        multiple=True,
    )(f)
    f = click.option(
        "--group",
        type=click.Choice(("MANAGEMENT",)),
        required=True,
        default="MANAGEMENT",
        show_default=True,
    )(f)
    f = click.option(
        "--sub_group",
        type=click.Choice(("INSTANCE",)),
        required=True,
        default="INSTANCE",
        show_default=True,
    )(f)
    f = click.option(
        "--tenant_name",
        "-tn",
        type=str,
        required=True,
        multiple=True,
    )(f)
    return f


def _run_maestro_events(
    ctx: ContextObj,
    event_action: tuple,
    group: str,
    sub_group: str,
    tenant_name: tuple,
    cloud: Cloud,
    customer_id: str,
    event_source: tuple | None = None,
    event_name: tuple | None = None,
):
    lists = [
        list(event_action),
        [
            group,
        ],
        [
            sub_group,
        ],
        list(tenant_name),
        list(event_source) if event_source else None,
        list(event_name) if event_name else None,
    ]
    normalize_lists(lists)
    events = []
    for i in range(len(lists[0])):
        events.append(
            _build_maestro_record(
                event_action=lists[0][i],
                group=lists[1][i],
                sub_group=lists[2][i],
                tenant_name=lists[3][i],
                cloud=cloud,
                event_source=lists[4][i] if event_source else None,
                event_name=lists[5][i] if event_name else None,
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
@click.option(
    "--event_source",
    "-es",
    type=str,
    required=True,
    multiple=True,
)
@click.option(
    "--event_name",
    "-en",
    type=str,
    required=True,
    multiple=True,
)
@cli_response()
def maestro_aws(
    ctx: ContextObj,
    event_action: tuple,
    group: str,
    sub_group: str,
    tenant_name: tuple,
    event_source: tuple,
    event_name: tuple,
    customer_id: str,
):
    """Builds maestro audit event for AWS"""
    return _run_maestro_events(
        ctx=ctx,
        event_action=event_action,
        group=group,
        sub_group=sub_group,
        tenant_name=tenant_name,
        event_source=event_source,
        event_name=event_name,
        cloud=Cloud.AWS,
        customer_id=customer_id,
    )


@maestro.command(cls=ViewCommand, name="azure")
@_maestro_cloud_options
@cli_response()
def maestro_azure(
    ctx: ContextObj,
    event_action: tuple,
    group: str,
    sub_group: str,
    tenant_name: tuple,
    customer_id,
):
    """Builds maestro audit event for Azure"""
    return _run_maestro_events(
        ctx=ctx,
        event_action=event_action,
        group=group,
        sub_group=sub_group,
        tenant_name=tenant_name,
        cloud=Cloud.AZURE,
        customer_id=customer_id,
    )


@maestro.command(cls=ViewCommand, name="google")
@_maestro_cloud_options
@cli_response()
def maestro_google(
    ctx: ContextObj,
    event_action: tuple,
    group: str,
    sub_group: str,
    tenant_name: tuple,
    customer_id,
):
    """Builds maestro audit event for Google Cloud"""
    return _run_maestro_events(
        ctx=ctx,
        event_action=event_action,
        group=group,
        sub_group=sub_group,
        tenant_name=tenant_name,
        cloud=Cloud.GOOGLE,
        customer_id=customer_id,
    )
