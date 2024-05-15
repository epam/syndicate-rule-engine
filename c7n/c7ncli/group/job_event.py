import click

from c7ncli.group import ContextObj, ViewCommand, cli_response, response
from c7ncli.service.helpers import (
    build_cloudtrail_records,
    build_eventbridge_record,
    build_maestro_record,
    normalize_lists,
)


@click.group(name='event')
def event():
    """Manages Job submit action"""


@event.command(cls=ViewCommand, name='cloudtrail')
@click.option('--cloud_identifier', '-cid', type=str, required=True,
              multiple=True, help='Account id to build event payload with')
@click.option('--region', '-r', type=str, default=['eu-central-1'],
              multiple=True,
              show_default=True, help='Region which the event came from')
@click.option('--event_source', '-es', type=str, default=['ssm.amazonaws.com'],
              show_default=True, help='CloudTrail event source to simulate',
              multiple=True)
@click.option('--event_name', '-en', type=str,
              default=['UpdateInstanceInformation'], show_default=True,
              help='CloudTrail event name to simulate', multiple=True)
@click.option('--wrap_in_eventbridge', '-eb', is_flag=True,
              help='Wraps CloudTrail event in EventBridge event with '
                   'detail-type `AWS API Call via CloudTrail`')
@cli_response()
def cloudtrail(ctx: ContextObj, cloud_identifier: tuple, region: tuple,
               event_source: tuple, event_name: tuple,
               wrap_in_eventbridge: bool, customer_id):
    """
    Command to simulate event-driven request from CloudTrail-based
    event-listener. Use it just to check whether
    event-driven jobs work properly
    """
    events = build_cloudtrail_records(
        list(cloud_identifier), list(region),
        list(event_source), list(event_name)
    )
    if wrap_in_eventbridge:
        _temp = events
        events = []
        for rec in _temp:
            events.append(build_eventbridge_record(
                detail_type='AWS API Call via CloudTrail',
                source='aws.resource-groups',  # not important here
                account=cloud_identifier[0],
                region=region[0],
                detail=rec
            ))
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='AWS',
        events=events,
        customer_id=customer_id
    )


@event.command(cls=ViewCommand, name='maestro')
@click.option('--event_action', '-ea',
              type=click.Choice(
                  ('COMMAND', 'CREATE', 'DELETE', 'DISABLE', 'UPDATE')),
              required=True, multiple=True)
@click.option('--group', type=click.Choice(('MANAGEMENT', )), required=True,
              default='MANAGEMENT', show_default=True)
@click.option('--sub_group', type=click.Choice(('INSTANCE', )), required=True,
              default='INSTANCE', show_default=True)
@click.option('--tenant_name', '-tn', type=str, required=True, multiple=True)
@click.option('--cloud', '-c', type=click.Choice(('AZURE', 'GOOGLE')),
              required=True)
@cli_response()
def maestro(ctx: ContextObj, event_action: tuple, group: str, sub_group: str,
            tenant_name: tuple, cloud: str, customer_id):
    """
    Builds maestro audit event
    """
    lists = [list(event_action), [group, ], [sub_group, ], list(tenant_name),
             [cloud, ]]
    normalize_lists(lists)
    events = []
    for i in range(len(lists[0])):
        events.append(build_maestro_record(
            event_action=lists[0][i],
            group=lists[1][i],
            sub_group=lists[2][i],
            tenant_name=lists[3][i],
            cloud=lists[4][i]
        ))
    return ctx['api_client'].event_action(
        version='1.0.0',
        vendor='MAESTRO',
        events=events,
        customer_id=customer_id
    )


@event.command(cls=ViewCommand, name='eventbridge')
@click.option('--account', '-cid', type=str, required=True,
              multiple=True, help='Account id to build event payload with')
@click.option('--region', '-r', type=str, default=['eu-central-1'],
              multiple=True,
              show_default=True, help='Region which the event came from')
@click.option('--source', '-es', type=str, default=['aws.ec2'],
              show_default=True, help='CloudTrail event source to simulate',
              multiple=True)
@click.option('--detail_type', '-dt', type=str,
              default=['EC2 Instance State-change Notification'],
              show_default=True,
              help='CloudTrail event name to simulate', multiple=True)
@cli_response()
def eventbridge(ctx: ContextObj, account: tuple, region: tuple, source: tuple,
                detail_type: tuple, customer_id):
    """
    Command to simulate event-driven request from EventBridge-based
    event-listener. Use it just to check whether
    event-driven jobs work properly
    """
    return response('Not implemented yet')
