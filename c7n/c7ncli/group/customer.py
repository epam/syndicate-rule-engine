import click

from c7ncli.group import ContextObj, ViewCommand, cli_response, response
from c7ncli.group.customer_rabbitmq import rabbitmq

attributes_order = 'name', 'display_name', 'admins'


@click.group(name='customer')
def customer():
    """Manages Custodian Service Customer Entities"""


@customer.command(cls=ViewCommand, name='describe')
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, customer_id: str):
    """
    Describes your user's customer
    """
    return ctx['api_client'].customer_get(
        name=customer_id,
    )


@customer.command(cls=ViewCommand, name='set_excluded_rules')
@click.option('--rules', '-r', type=str, multiple=True,
              help='Rules that you want to exclude for a customer. '
                   'They will be excluded for each tenant')
@click.option('--empty', is_flag=True, help='Whether to reset the '
                                            'list of excluded rules')
@cli_response()
def set_excluded_rules(ctx: ContextObj, customer_id: str | None,
                       rules: tuple[str, ...], empty: bool):
    """
    Excludes rules for a customer
    """
    if not rules and not empty:
        return response('Specify either --rules '' or --empty')
    if empty:
        rules = ()
    return ctx['api_client'].customer_set_excluded_rules(
        customer_id=customer_id,
        rules=rules
    )


@customer.command(cls=ViewCommand, name='get_excluded_rules')
@cli_response()
def get_excluded_rules(ctx: ContextObj, customer_id):
    """
    Returns excluded rules for a customer
    """
    return ctx['api_client'].customer_get_excluded_rules(
        customer_id=customer_id
    )


customer.add_command(rabbitmq)
