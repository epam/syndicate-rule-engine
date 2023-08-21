import click

from c7ncli.group import cli_response, ViewCommand, ContextObj, customer_option
from c7ncli.group.customer_rabbitmq import rabbitmq
from c7ncli.service.constants import PARAM_DISPLAY_NAME, \
    PARAM_NAME


@click.group(name='customer')
def customer():
    """Manages Custodian Service Customer Entities"""


@customer.command(cls=ViewCommand, name='describe')
@customer_option
@click.option('--full', '-f', is_flag=True,
              help='Show full command output', show_default=True)
@cli_response(attributes_order=[PARAM_NAME, PARAM_DISPLAY_NAME])
def describe(ctx: ContextObj, customer_id: str, full: bool):
    """
    Describes your user's customer
    """
    return ctx['api_client'].customer_get(
        name=customer_id,
        complete=full
    )


customer.add_command(rabbitmq)
