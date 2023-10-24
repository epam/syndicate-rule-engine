from datetime import datetime
import click

from c7ncli.group import tenant_option, limit_option, next_option, \
    ContextObj, cli_response, ViewCommand, customer_option, build_iso_date_option, \
    from_date_iso_args, to_date_iso_args

from_date_results_option = build_iso_date_option(
    *from_date_iso_args, required=False,
    help='Obtain batched-results FROM date.'
)
to_date_results_option = build_iso_date_option(
    *to_date_iso_args, required=False,
    help='Obtain batched-results TILL date.'
)


@click.group(name='results')
def results():
    """Manages Custodian Service Results of Batched Scan Entities"""


@results.command(cls=ViewCommand, name='describe')
@click.option('--batch_result_id', '-id', type=str, required=False,
              help='Batch Result identifier to describe by')
@tenant_option
@customer_option
@from_date_results_option
@to_date_results_option
@limit_option
@next_option
@cli_response(attributes_order=[])
def describe(ctx: ContextObj, batch_result_id: str,  tenant_name: str,
             customer_id: str, from_date: datetime, to_date: datetime,
             limit: int, next_token: str):
    """
    Describes results of Custodian Service reactive, batched scans
    """
    if batch_result_id:
        return ctx['api_client'].batch_results_get(br_id=batch_result_id)

    if from_date:
        from_date = from_date.isoformat()

    if to_date:
        to_date = to_date.isoformat()

    return ctx['api_client'].batch_results_query(
        tenant=tenant_name, customer=customer_id,
        start_date=from_date, end_date=to_date,
        next_token=next_token, limit=limit
    )
