import click

from c7ncli.group import ContextObj, ViewCommand, cli_response
from c7ncli.service.constants import RULE_CLOUDS


@click.group(name='eventdriven')
def eventdriven():
    """
    Manages event-driven rule-sets
    """


@eventdriven.command(cls=ViewCommand, name='add')
@click.option('--version', '-v', type=float, default=1.0,
              help='Ruleset version')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=True, help='Ruleset cloud')
@click.option('--rule_id', '-rid', multiple=True, required=False,
              help='Rule ids to attach to the ruleset')
@click.option('--rule_version', '-rv', type=str, required=False,
              help='Rule version to choose in case of duplication '
                   '(the highest version by default). Used with '
                   '--full_cloud or --standard flags')
@cli_response()
def add(ctx: ContextObj, version: float, cloud: str,
        rule_id: tuple, rule_version: str, customer_id):
    """
    Creates Event-driven ruleset with all the rules
    """

    return ctx['api_client'].ed_ruleset_add(
        version=version,
        cloud=cloud,
        rule=list(rule_id),
        rule_version=rule_version
    )


@eventdriven.command(cls=ViewCommand, name='describe')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              help='Event-driven ruleset cloud to describe')
@click.option('--get_rules', '-r', is_flag=True, default=False,
              help='If specified, ruleset\'s rules ids will be returned. '
                   'MAKE SURE to use \'--json\' flag to get a clear output')
@cli_response()
def describe(ctx: ContextObj, cloud: str, get_rules: bool, customer_id):
    """
    Describes Event-driven ruleset
    """
    return ctx['api_client'].ed_ruleset_get(
        cloud=cloud,
        get_rules=get_rules
    )


@eventdriven.command(cls=ViewCommand, name='delete')
@click.option('--cloud', '-c', required=True,
              type=click.Choice(RULE_CLOUDS),
              help='Event-driven ruleset cloud to describe')
@click.option('--version', '-v', type=float, default=1.0,
              help='Event-driven ruleset version to delete')
@cli_response()
def delete(ctx: ContextObj, cloud: str, version: float, customer_id):
    """
    Deletes Event-driven ruleset
    """
    return ctx['api_client'].ed_ruleset_delete(cloud=cloud, version=version)
