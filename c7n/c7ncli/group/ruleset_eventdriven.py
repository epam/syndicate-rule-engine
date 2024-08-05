import click

from c7ncli.group import ContextObj, ViewCommand, cli_response
from c7ncli.service.constants import RULE_CLOUDS


@click.group(name='eventdriven')
def eventdriven():
    """
    Manages event-driven rule-sets
    """


@eventdriven.command(cls=ViewCommand, name='add')
@click.option('--cloud', '-c', type=click.Choice(RULE_CLOUDS),
              required=True, help='Ruleset cloud')
@click.option('--version', '-v', type=str,
              help='Ruleset version to create. If not specified, will be '
                   'resolved automatically based on the previous ruleset '
                   'with the same name or based on rulesource of type '
                   'GITHUB_RELEASE')
@click.option('--rule_source_id', '-rsid', required=False, type=str,
              help='The id of rule source object to use rules from')
@cli_response()
def add(ctx: ContextObj, cloud, version, rule_source_id, customer_id):
    """
    Creates Event-driven ruleset with all the rules
    """

    return ctx['api_client'].ed_ruleset_add(
        version=version,
        cloud=cloud,
        rule_source_id=rule_source_id,
        customer_id=customer_id
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
@click.option('--version', '-v', type=str, required=True,
              help='Event-driven ruleset version to delete. '
                   '\'*\' to delete all versions')
@cli_response()
def delete(ctx: ContextObj, cloud: str, version: float, customer_id):
    """
    Deletes Event-driven ruleset
    """
    return ctx['api_client'].ed_ruleset_delete(cloud=cloud, version=version)
