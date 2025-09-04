import click

from srecli.group import (
    ContextObj, ViewCommand, cli_response, next_option, build_tenant_option,
    build_limit_option, exception_expire_at_required_option,
)
from srecli.service.adapter_client import SREResponse


@click.group(name='exception')
def exception():
    """Manages resource exception"""


@exception.command(cls=ViewCommand, name='describe')
@click.option('--exception_id', '-excid', type=str,
              help='Resource exception ID. '
                   'If provided, other filters are ignored')
@build_tenant_option()
@click.option('--resource_type', '-rt', type=str,
              help='Resource type to filter the results '
                   '(e.g., aws.ec2, azure.vm)')
@click.option('--location', '-l', type=str, required=False,
              help='Location/region to filter the results')
@click.option('--resource_id', '-resid', type=str,
              help='Resource ID to filter the results')
@click.option('--arn', type=str,
              help='Amazon Resource Name (ARN) of a specific AWS resource to '
                   'filter the results')
@click.option('--tags_filters', '-tf', type=str, multiple=True,
              help='Tag filters to apply to the results '
                   '(e.g., key=value)')
@build_limit_option()
@next_option
@cli_response()
def describe(
    ctx: ContextObj,
    exception_id: str | None = None,
    tenant_name: str | None = None,
    resource_type: str | None = None,
    location: str | None = None,
    resource_id: str | None = None,
    arn: str | None = None,
    tags_filters: tuple[str] | None = None,
    limit: int | None = None,
    next_token: str | None = None,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Retrieve resource exception(s) with optional filtering and pagination.

    Can retrieve either:
    - A specific resource exception by ID (when --exception_id is provided)
    - A filtered list of resource exceptions (when using other filter options)
    """
    if exception_id:
        return ctx['api_client'].resource_exception_get_by_id(
            exception_id=exception_id,
            customer_id=customer_id,
        )

    return ctx['api_client'].resource_exception_get(
        tenant_name=tenant_name,
        resource_type=resource_type,
        location=location,
        resource_id=resource_id,
        arn=arn,
        tags_filters=list(tags_filters) if tags_filters else None,
        limit=limit,
        next_token=next_token,
        customer_id=customer_id,
    )


def _validate_and_prepare_exception_params(
    resource_id: str | None,
    resource_type: str | None,
    location: str | None,
    arn: str | None,
    tags_filters: tuple[str] | None,
) -> dict:
    # Validate that exactly one exception type is provided
    has_resource_params = bool(resource_id or resource_type or location)
    has_arn = bool(arn)
    has_tags = bool(tags_filters)
    # Count how many exception types are being used
    exception_types_count = sum([has_resource_params, has_arn, has_tags])

    if exception_types_count == 0:
        raise click.ClickException(
            "You must provide one of the following exception types:\n"
            "  1. Resource-specific: --resource_id, --resource_type, and "
            "--location\n"
            "  2. ARN-based: --arn\n"
            "  3. Tag-based: --tags_filters"
        )

    if exception_types_count > 1:
        raise click.ClickException(
            "You can only use one exception type at a time. Choose either:\n"
            "  1. Resource-specific exception (--resource_id, --resource_type,"
            " --location), OR\n"
            "  2. ARN-based exception (--arn), OR\n"
            "  3. Tag-based exception (--tags_filters)"
        )

    # Validate resource-specific exception
    if has_resource_params:
        missing_params = []
        if not resource_id:
            missing_params.append('--resource_id')
        if not resource_type:
            missing_params.append('--resource_type')
        if not location:
            missing_params.append('--location')

        if missing_params:
            raise click.ClickException(
                f"Resource-specific exception requires all three parameters:\n"
                f"  Missing: {', '.join(missing_params)}"
            )

    # Convert tags_filters tuple to list if present
    tags_filters_list = list(tags_filters) if tags_filters else None

    # Validate tag filter format
    if tags_filters_list:
        for tag_filter in tags_filters_list:
            if '=' not in tag_filter:
                raise click.ClickException(
                    f"Invalid tag filter format: '{tag_filter}'\n"
                    f"Tag filters must be in 'key=value' format."
                )

    # Build the exception type specific fields
    exception_params = {}

    if has_resource_params:
        exception_params.update({
            'resource_id': resource_id,
            'resource_type': resource_type,
            'location': location,
        })
    elif has_arn:
        exception_params['arn'] = arn
    elif has_tags:
        exception_params['tags_filters'] = tags_filters_list

    return exception_params


@exception.command(cls=ViewCommand, name='add')
@build_tenant_option(required=True)
@exception_expire_at_required_option
@click.option('--resource_id', '-resid', type=str,
              help='Resource ID (required with `resource_type` and `location` '
                   'for resource-specific exception)')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type (e.g., aws.ec2, azure.vm) - required with '
                   '`resource_id` and `location`')
@click.option('--location', '-l', type=str,
              help='Location/region (required with `resource_id` and '
                   '`resource_type`)')
@click.option('--arn', type=str,
              help='Amazon Resource Name for ARN-based exception '
                   '(cannot be combined with other filters)')
@click.option('--tags_filters', '-tf', type=str, multiple=True,
              help='Tag filters in format "key=value" for tag-based exception '
                   '(cannot be combined with other filters)')
@cli_response()
def add(
    ctx: ContextObj,
    tenant_name: str,
    expire_at: str,
    resource_type: str | None = None,
    location: str | None = None,
    resource_id: str | None = None,
    arn: str | None = None,
    tags_filters: tuple[str] | None = None,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Create a new resource exception to exclude resources from rule execution.

    Three types of exceptions are supported:
    1. Resource-specific: requires --resource_id, --resource_type, and --location
    2. ARN-based: requires --arn
    3. Tag-based: requires --tags_filters

    All exception types require --tenant_name and --expire_at
    """
    # Validate and prepare exception parameters
    exception_params = _validate_and_prepare_exception_params(
        resource_id=resource_id,
        resource_type=resource_type,
        location=location,
        arn=arn,
        tags_filters=tags_filters,
    )

    # Build the complete payload
    payload = {
        'customer_id': customer_id,
        'tenant_name': tenant_name,
        'expire_at': expire_at,
        **exception_params,
    }

    return ctx['api_client'].resource_exception_add(**payload)


@exception.command(cls=ViewCommand, name='update')
@click.option('--exception_id', '-exid', type=str, required=True,
              help='The resource exception ID to update')
@build_tenant_option(required=True)
@click.option('--resource_id', '-resid', type=str,
              help='Resource ID (required with resource_type and location for '
                   'resource-specific exception)')
@click.option('--resource_type', '-rt', type=str,
              help='Resource type (e.g., aws.ec2, azure.vm) - required with '
                   'resource_id and location')
@click.option('--location', '-l', type=str,
              help='Location/region (required with resource_id and '
                   'resource_type)')
@click.option('--arn', type=str,
              help='Amazon Resource Name for ARN-based exception (cannot be '
                   'combined with other filters)')
@click.option('--tags_filters', '-tf', type=str, multiple=True,
              help='Tag filters in format "key=value" for tag-based exception '
                   '(cannot be combined with other filters)')
@exception_expire_at_required_option
@cli_response()
def update(
    ctx: ContextObj,
    exception_id: str,
    tenant_name: str,
    resource_type: str | None = None,
    location: str | None = None,
    resource_id: str | None = None,
    arn: str | None = None,
    tags_filters: tuple[str] | None = None,
    expire_at: str = None,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Update an existing resource exception.

    The update replaces the entire exception configuration. You must provide
    the complete exception type configuration (not just the fields to update).

    Three types of exceptions are supported:
    1. Resource-specific: requires --resource_id, --resource_type, and --location
    2. ARN-based: requires --arn
    3. Tag-based: requires --tags_filters

    All exception types require --exception_id, --tenant_name and --expire_at
    """
    # Validate and prepare exception parameters
    exception_params = _validate_and_prepare_exception_params(
        resource_id=resource_id,
        resource_type=resource_type,
        location=location,
        arn=arn,
        tags_filters=tags_filters,
    )

    # Build the complete payload
    payload = {
        'customer_id': customer_id,
        'tenant_name': tenant_name,
        'expire_at': expire_at,
        **exception_params,
    }

    return ctx['api_client'].resource_exception_update(
        exception_id=exception_id,
        **payload,
    )


@exception.command(cls=ViewCommand, name='delete')
@click.option('--exception_id', '-exid', type=str, required=True,
              help='The resource exception ID to delete')
@cli_response()
def delete(
    ctx: ContextObj,
    exception_id: str,
    customer_id: str | None = None,
) -> SREResponse:
    """
    Delete a resource exception by its ID
    """
    return ctx['api_client'].resource_exception_delete(
        exception_id=exception_id,
        customer_id=customer_id,
    )
