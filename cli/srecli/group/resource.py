import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response, next_option, build_tenant_option,
    build_limit_option,
)
from srecli.group.resource_exception import exception
from srecli.service.adapter_client import SREResponse


@click.group(name='resource')
def resource():
    """Manage resources"""


@resource.command(cls=ViewCommand, name='describe')
@click.option('--arn', type=str,
              help='Amazon Resource Name (ARN) of a specific AWS resource. '
                   'If provided, other filters are ignored')
@build_tenant_option()
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the results '
                   '(e.g., aws.ec2, azure.vm)')
@click.option('--location', '-l', type=str, required=False,
              help='Location/region to filter the results')
@click.option('--resource_id', '-resid', type=str,
              help='Resource ID to filter the results')
@click.option('--name', '-n', type=str,
              help='Resource name to filter the results')
@build_limit_option()
@next_option
@cli_response()
def describe(
    ctx: ContextObj,
    arn: str | None = None,
    tenant_name: str | None = None,
    resource_type: str | None = None,
    location: str | None = None,
    resource_id: str | None = None,
    name: str | None = None,
    limit: int | None = None,
    next_token: str | None = None,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Retrieve cloud resource(s) with optional filtering and pagination.

    Can retrieve either:
    - A specific AWS resource by ARN (when --arn is provided)
    - A filtered list of resources (when using other filter options)
    """
    if arn:
        return ctx['api_client'].resources_by_arn_get(
            arn=arn,
            customer_id=customer_id,
        )

    return ctx['api_client'].resources_get(
        tenant_name=tenant_name,
        resource_type=resource_type,
        location=location,
        resource_id=resource_id,
        name=name,
        limit=limit,
        next_token=next_token,
        customer_id=customer_id,
    )


resource.add_command(exception)
