"""CLI commands for managing SQS event sources."""

import click
from srecli.group import ContextObj, ViewCommand, cli_response


@click.group(name='sources')
def sources():
    """
    Manages event sources for integrations
    """


@sources.command(cls=ViewCommand, name='add')
@click.option(
    '--queue_url',
    '-qu',
    type=str,
    required=True,
    help='SQS queue URL',
)
@click.option(
    '--region',
    '-r',
    type=str,
    required=True,
    help='AWS region',
)
@click.option(
    '--enabled',
    '-e',
    type=bool,
    required=False,
    default=True,
    help='Param to enable or disable the event source temporarily',
)
@click.option(
    '--aws_access_key_id',
    type=str,
    default=None,
    help='AWS access key (optional)',
)
@click.option(
    '--aws_secret_access_key',
    type=str,
    default=None,
    help='AWS secret key (optional)',
)
@click.option(
    '--aws_session_token',
    type=str,
    default=None,
    help='AWS session token for temporary creds (optional)',
)
@click.option(
    '--role_arn',
    '-ra',
    type=str,
    default=None,
    help='IAM role ARN to assume for SQS access (optional, uses instance profile as base)',
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
    role_arn: str | None,
    customer_id: str | None,
):
    """
    Creates an SQS event source configuration.
    """
    data = {
        'source_type': 'SQS',
        'queue_url': queue_url,
        'region': region,
        'enabled': enabled,
    }
    if customer_id:
        data['customer_id'] = customer_id
    if aws_access_key_id:
        data['aws_access_key_id'] = aws_access_key_id
    if aws_secret_access_key:
        data['aws_secret_access_key'] = aws_secret_access_key
    if aws_session_token:
        data['aws_session_token'] = aws_session_token
    if role_arn:
        data['role_arn'] = role_arn
    return ctx['api_client'].event_sources_post(**data)


@sources.command(cls=ViewCommand, name='describe')
@click.option(
    '--event_source_id',
    '-esid',
    type=str,
    required=False,
    help='Event source ID. If provided, describes a specific event source.',
)
@cli_response()
def describe(
    ctx: ContextObj, event_source_id: str | None, customer_id: str | None
):
    """
    Lists event sources or describes one by event_source_id.
    """
    if event_source_id:
        return ctx['api_client'].event_sources_get(
            event_source_id=event_source_id, customer_id=customer_id
        )
    return ctx['api_client'].event_sources_list(customer_id=customer_id)


@sources.command(cls=ViewCommand, name='update')
@click.option(
    '--event_source_id',
    '-esid',
    type=str,
    required=True,
    help='Event source ID to update',
)
@click.option(
    '--queue_url',
    '-qu',
    type=str,
    required=False,
    help='SQS queue URL',
)
@click.option(
    '--region',
    '-r',
    type=str,
    required=False,
    help='AWS region',
)
@click.option(
    '--enabled',
    '-e',
    type=bool,
    required=False,
    help='Param to enable or disable the event source temporarily',
)
@click.option(
    '--role_arn',
    '-ra',
    type=str,
    required=False,
    help='IAM role ARN to assume for SQS access',
)
@cli_response()
def update(
    ctx: ContextObj,
    event_source_id: str,
    queue_url: str | None,
    region: str | None,
    enabled: bool | None,
    role_arn: str | None,
    customer_id: str | None,
):
    """
    Updates an event source configuration.
    """
    data: dict = {}
    if queue_url is not None:
        data['queue_url'] = queue_url
    if region is not None:
        data['region'] = region
    if enabled is not None:
        data['enabled'] = enabled
    if role_arn is not None:
        data['role_arn'] = role_arn
    if customer_id:
        data['customer_id'] = customer_id
    return ctx['api_client'].event_sources_put(
        event_source_id=event_source_id, **data
    )


@sources.command(cls=ViewCommand, name='delete')
@click.option(
    '--event_source_id',
    '-esid',
    type=str,
    required=True,
    help='Event source ID',
)
@cli_response()
def delete(ctx: ContextObj, event_source_id: str, customer_id: str | None):
    """
    Deletes an event source by ID
    """
    return ctx['api_client'].event_sources_delete(
        event_source_id=event_source_id, customer_id=customer_id
    )
