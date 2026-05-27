import click
import uuid

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group.job_event_maestro import maestro
from srecli.service.helpers import normalize_lists, utc_iso


def _build_cloudtrail_record(
    cloud_identifier: str,
    region: str,
    event_source: str,
    event_name: str,
) -> dict:
    return {
        "eventTime": utc_iso(),
        "awsRegion": region,
        "userIdentity": {"accountId": cloud_identifier},
        "eventSource": event_source,
        "eventName": event_name,
    }


def _build_cloudtrail_records(
    cloud_identifier: list,
    region: list,
    event_source: list,
    event_name: list,
) -> list:
    """
    Builds CloudTrail log records based on given params. If you still
    don't get it just execute the function with some random parameters
    (no validation of parameters content provided) and see the result.
    """
    records = []
    lists = [cloud_identifier, region, event_source, event_name]
    normalize_lists(lists)

    for i in range(len(lists[0])):
        records.append(
            _build_cloudtrail_record(
                cloud_identifier=cloud_identifier[i],
                region=region[i],
                event_source=event_source[i],
                event_name=event_name[i],
            )
        )
    return records


def _build_eventbridge_record(
    detail_type: str,
    source: str,
    account: str,
    region: str,
    detail: dict,
) -> dict:
    return {
        "version": "0",
        "id": str(uuid.uuid4()),
        "detail-type": detail_type,
        "source": source,
        "account": account,
        "time": utc_iso(),
        "region": region,
        "resources": [],
        "detail": detail,
    }


def _build_k8s_agent_events(
    event_type: list[str],
    reason: list[str],
    platform_id: list[str],
    involved_name: list[str],
    resource_uid: list[str],
    namespace: list[str],
) -> list[dict]:
    """Shape matches ingest ``K8sNativeEventAdapter`` and k8s watcher normalizer."""
    lists = [event_type, reason, platform_id, involved_name]
    has_uid = bool(resource_uid)
    has_ns = bool(namespace)
    if has_uid:
        lists.append(resource_uid)
    if has_ns:
        lists.append(namespace)
    normalize_lists(lists)
    out: list[dict] = []
    for i in range(len(event_type)):
        kind = event_type[i]
        meta: dict = {
            "name": involved_name[i],
            "kind": kind,
            "resourceUid": resource_uid[i] if has_uid else str(uuid.uuid4()),
        }
        if has_ns:
            meta["namespace"] = namespace[i]
        out.append(
            {
                "type": kind,
                "reason": reason[i],
                "platformId": platform_id[i],
                "metadata": meta,
            }
        )
    return out


@click.group(name="event")
def event():
    """Manages Job submit action"""


@event.command(cls=ViewCommand, name="cloudtrail")
@click.option(
    "--cloud_identifier",
    "-cid",
    type=str,
    required=True,
    multiple=True,
    help="Account id to build event payload with",
)
@click.option(
    "--region",
    "-r",
    type=str,
    default=["eu-central-1"],
    multiple=True,
    show_default=True,
    help="Region which the event came from",
)
@click.option(
    "--event_source",
    "-es",
    type=str,
    default=["ssm.amazonaws.com"],
    show_default=True,
    help="CloudTrail event source to simulate",
    multiple=True,
)
@click.option(
    "--event_name",
    "-en",
    type=str,
    default=["UpdateInstanceInformation"],
    show_default=True,
    help="CloudTrail event name to simulate",
    multiple=True,
)
@cli_response()
def cloudtrail(
    ctx: ContextObj,
    cloud_identifier: tuple,
    region: tuple,
    event_source: tuple,
    event_name: tuple,
    customer_id,
):
    """
    Command to simulate event-driven request from CloudTrail-based
    event-listener. Use it just to check whether
    event-driven jobs work properly
    """
    events = _build_cloudtrail_records(
        list(cloud_identifier), list(region), list(event_source), list(event_name)
    )
    _temp = events
    events = []
    for rec in _temp:
        events.append(
            _build_eventbridge_record(
                detail_type="AWS API Call via CloudTrail",
                source="aws.resource-groups",  # not important here
                account=cloud_identifier[0],
                region=region[0],
                detail=rec,
            )
        )
    return ctx["api_client"].event_action(
        version="1.0.0", vendor="AWS", events=events, customer_id=customer_id
    )


@event.command(cls=ViewCommand, name="k8s")
@click.option(
    "--event-type",
    "-t",
    type=str,
    required=True,
    multiple=True,
    help="Kubernetes resource type (maps to rule metadata service), e.g. Pod",
)
@click.option(
    "--reason",
    "-R",
    type=str,
    required=True,
    multiple=True,
    help="Event reason, e.g. PodDeleted",
)
@click.option(
    "--platform-id",
    "-p",
    type=str,
    required=True,
    multiple=True,
    help="K8s platform id (Modular parent id for the cluster)",
)
@click.option(
    "--involved-object-name",
    "--resource-name",
    "-rn",
    type=str,
    default=["sre-cli"],
    multiple=True,
    show_default=True,
    help="involved object name (metadata.name); default for quick local simulation",
)
@click.option(
    "--resource-uid",
    "-u",
    default=["12345678-9012-3456-7890-123456789012"],
    type=str,
    multiple=True,
    help="Kubernetes involved object UID (metadata.resourceUid); "
    "if omitted a random UUID is used per event",
)
@click.option(
    "--namespace",
    "-ns",
    default=["default"],
    type=str,
    multiple=True,
    help="Involved object namespace (metadata.namespace)",
)
@cli_response()
def k8s(
    ctx: ContextObj,
    event_type: tuple[str, ...],
    reason: tuple[str, ...],
    platform_id: tuple[str, ...],
    involved_object_name: tuple[str, ...],
    resource_uid: tuple[str, ...],
    namespace: tuple[str, ...],
    customer_id,
):
    """Send SRE_K8S_AGENT events to POST /event (event-driven ingest)."""
    events = _build_k8s_agent_events(
        list(event_type),
        list(reason),
        list(platform_id),
        list(involved_object_name),
        list(resource_uid),
        list(namespace),
    )
    return ctx["api_client"].event_action(
        version="1.0.0",
        vendor="SRE_K8S_AGENT",
        events=events,
        customer_id=customer_id,
    )


event.add_command(maestro)
