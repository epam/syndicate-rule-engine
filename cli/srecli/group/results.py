from datetime import datetime

import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_iso_date_option,
    cli_response,
    from_date_iso_args,
    limit_option,
    next_option,
    tenant_option,
    to_date_iso_args,
)

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
@from_date_results_option
@to_date_results_option
@limit_option
@next_option
@cli_response()
def describe(ctx: ContextObj, batch_result_id: str,  tenant_name: str,
             customer_id: str, from_date: datetime, to_date: datetime,
             limit: int, next_token: str):
    """
    Describes results of Custodian Service reactive, batched scans
    """
    if batch_result_id:
        return ctx['api_client'].batch_results_get(br_id=batch_result_id,
                                                   customer_id=customer_id)

    if from_date:
        from_date = from_date.isoformat()

    if to_date:
        to_date = to_date.isoformat()

    return ctx['api_client'].batch_results_query(
        customer_id=customer_id,
        tenant_name=tenant_name,
        start=from_date,
        end=to_date,
        limit=limit,
        next_token=next_token
    )
