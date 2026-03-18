"""
CLI commands for managing SQS event sources.
"""

import os

import click

from srecli.group import ContextObj, ViewCommand, cli_response


def _env_or(value: str | None, env_key: str) -> str | None:
    """Return value if set, otherwise env var."""
    return value or os.environ.get(env_key) or None


@click.group(name="event_sources")
def event_sources():
    """
    Manages SQS event source configuration
    """


@event_sources.command(cls=ViewCommand, name="add")
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
    default=None,
    help="AWS region (or AWS_DEFAULT_REGION env)",
)
@click.option(
    "--enabled/--disabled",
    default=True,
    help="Whether the event source is enabled (default: enabled)",
)
@click.option(
    "--aws_access_key_id",
    type=str,
    default=None,
    help="AWS access key (or AWS_ACCESS_KEY_ID env)",
)
@click.option(
    "--aws_secret_access_key",
    type=str,
    default=None,
    help="AWS secret key (or AWS_SECRET_ACCESS_KEY env)",
)
@click.option(
    "--aws_session_token",
    type=str,
    default=None,
    help="AWS session token for temporary creds (or AWS_SESSION_TOKEN env)",
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
    region = _env_or(region, "AWS_DEFAULT_REGION")
    if not region:
        raise click.UsageError("--region / -r is required or set AWS_DEFAULT_REGION")
    aws_access_key_id = _env_or(aws_access_key_id, "AWS_ACCESS_KEY_ID")
    aws_secret_access_key = _env_or(aws_secret_access_key, "AWS_SECRET_ACCESS_KEY")
    aws_session_token = _env_or(aws_session_token, "AWS_SESSION_TOKEN")

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


@event_sources.command(cls=ViewCommand, name="list")
@cli_response()
def list_cmd(ctx: ContextObj, customer_id: str | None):
    """
    Lists all SQS event sources for a customer
    """
    return ctx["api_client"].event_sources_list(customer_id=customer_id)


@event_sources.command(cls=ViewCommand, name="describe")
@click.option(
    "--id",
    "event_source_id",
    type=str,
    required=True,
    help="Event source ID",
)
@cli_response()
def describe(
    ctx: ContextObj,
    event_source_id: str,
    customer_id: str | None,
):
    """
    Describes an event source by ID
    """
    return ctx["api_client"].event_sources_get(
        event_source_id=event_source_id,
        customer_id=customer_id,
    )


@event_sources.command(cls=ViewCommand, name="delete")
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

