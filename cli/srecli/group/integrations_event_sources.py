"""
CLI commands for managing SQS event sources.
"""

import click

from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name="sources")
def sources():
    """
    Manages event sources for integrations
    """


@sources.command(cls=ViewCommand, name="add")
@click.option(
    "--queue_url",
    "-qu",
    type=str,
    required=True,
    help="SQS queue URL",
)
@click.option(
    "--region",
    "-r",
    type=str,
    required=True,
    help="AWS region",
)
@click.option(
    "--enabled",
    "-e",
    type=bool,
    required=False,
    default=True,
    help="Param to enable or disable the event source temporarily",
)
@click.option(
    "--aws_access_key_id",
    type=str,
    default=None,
    help="AWS access key (optional)",
)
@click.option(
    "--aws_secret_access_key",
    type=str,
    default=None,
    help="AWS secret key (optional)",
)
@click.option(
    "--aws_session_token",
    type=str,
    default=None,
    help="AWS session token for temporary creds (optional)",
)
@cli_response()
def add(
    ctx: ContextObj,
    queue_url: str,
    region: str | None,
    enabled: bool,
    aws_access_key_id: str | None,
    aws_secret_access_key: str | None,
    aws_session_token: str | None,
    customer_id: str | None,
):
    """
    Creates an SQS event source configuration.
    """
    data = {
        "queue_url": queue_url,
        "region": region,
        "enabled": enabled,
    }
    if customer_id:
        data["customer_id"] = customer_id
    if aws_access_key_id:
        data["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        data["aws_secret_access_key"] = aws_secret_access_key
    if aws_session_token:
        data["aws_session_token"] = aws_session_token
    return ctx["api_client"].event_sources_post(**data)


@sources.command(cls=ViewCommand, name="describe")
@click.option(
    "--id",
    "event_source_id",
    type=str,
    required=False,
    help="Event source ID. If provided, describes a specific event source.",
)
@cli_response()
def describe(
    ctx: ContextObj,
    event_source_id: str | None,
    customer_id: str | None,
):
    """
    Lists SQS event sources or describes one by ID.

    Without --id: lists all event sources for a customer.
    With --id: describes a specific event source.
    """
    if event_source_id:
        return ctx["api_client"].event_sources_get(
            event_source_id=event_source_id,
            customer_id=customer_id,
        )
    return ctx["api_client"].event_sources_list(customer_id=customer_id)


@sources.command(cls=ViewCommand, name="delete")
@click.option(
    "--id",
    "event_source_id",
    type=str,
    required=True,
    help="Event source ID",
)
@cli_response()
def delete(
    ctx: ContextObj,
    event_source_id: str,
    customer_id: str | None,
):
    """
    Deletes an event source by ID
    """
    return ctx["api_client"].event_sources_delete(
        event_source_id=event_source_id,
        customer_id=customer_id,
    )
