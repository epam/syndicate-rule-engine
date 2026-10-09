import json
from pathlib import Path
from http import HTTPStatus

import click

from srecli.group import (
    ContextObj,
    ViewCommand,
    build_tenant_option,
    cli_response,
    dojo_engagement_option,
    dojo_product_option,
    dojo_test_option,
    validate_file_optional,
)
from srecli.service.adapter_client import SREResponse
from srecli.service.constants import TenantModel
from srecli.service.creds import (
    AWSCredentialsResolver,
    AZURECredentialsResolver,
    CredentialsLookupError,
    GOOGLECredentialsResolver, CredentialsResolver,
)
from srecli.service.logger import get_logger


_LOG = get_logger(__name__)

attributes_order = 'id', 'tenant_name', 'status', 'submitted_at',


def load_rules_to_scan(rules_to_scan: tuple[str, ...]) -> list:
    """
    Each item of the tuple can be either a raw rule id, or path to a file
    containing json with ids or just a JSON string. This method resolves it
    :param rules_to_scan:
    :return:
    """
    rules = set()
    for item in rules_to_scan:
        path = Path(item)
        if path.exists():
            try:
                with open(path, 'r') as fp:
                    content = fp.read()
            except OSError:  # file read error
                content = '[]'  # todo raise
        else:
            content = item
        try:
            loaded = json.loads(content)
            if isinstance(loaded, list):
                rules.update(loaded)
        except json.JSONDecodeError:
            rules.add(content)
    return list(rules)


def get_tenant(
    ctx: ContextObj,
    name: str,
    customer_id: str | None = None
) -> TenantModel | SREResponse:
    resp = ctx['api_client'].tenant_get(name, customer_id=customer_id)
    # todo cache in temp files
    if not resp.was_sent or not resp.ok:
        return resp

    return next(resp.iter_items())


def cloud_submit_options(func):
    """Add options shared by cloud provider job submissions."""
    options = (
        dojo_test_option,
        dojo_engagement_option,
        dojo_product_option,
        click.option(
            '--license_key',
            '-lk',
            required=False,
            type=str,
            help='License key to utilize for this job in case an ambiguous '
            'situation occurs',
        ),
        click.option(
            '--rules_to_scan',
            required=False,
            multiple=True,
            type=str,
            help='Rules that must be scanned. Ruleset must contain them. '
                 'You can specify some subpart of rule names. SRE '
                 'will try to resolve the full names: aws-002 -> '
                 'ecc-aws-002-encryption... Also you can specify some part '
                 'that is common to multiple rules. All the them will be '
                 'resolved: postgresql -> [ecc-aws-001-postgresql..., '
                 'ecc-aws-002-postgresql...]. This CLI param can accept '
                 'both raw rule names and path to file with JSON list '
                 'of rules',
        ),
        click.option(
            '--region',
            '-r',
            type=str,
            required=False,
            multiple=True,
            help='Regions to scan. If not specified, all active regions '
            'will be used',
        ),
        click.option(
            '--ruleset',
            '-rs',
            type=str,
            required=False,
            multiple=True,
            help='Rulesets to scan. If not specified, all available by '
            'license rulesets will be used',
        ),
        build_tenant_option(required=True),
    )
    for option in options:
        func = option(func)
    return func


def resolve_credentials(
    tenant: TenantModel,
    resolver_type: type[CredentialsResolver],
    resolver_kwargs: dict,
    resolve_local_credentials: bool,
):
    """Resolve credentials for a provider-specific submission."""
    if not resolve_local_credentials and not any(resolver_kwargs.values()):
        return {}

    resolver = resolver_type(tenant)
    try:
        return resolver.resolve(**resolver_kwargs)
    except CredentialsLookupError as e:
        _LOG.warning(f'Could not find credentials: {e}')
        return {}


@click.group(name='scan')
def scan():
    """
    Submits a job to scan Cloud or K8S cluster
    """


@scan.command(cls=ViewCommand, name='aws')
@cloud_submit_options
@click.option('--access_key_id', type=str, help='AWS access key')
@click.option('--secret_access_key', type=str, help='AWS secret key')
@click.option('--session_token', type=str, help='AWS session token')
@click.option('--resolve_local_credentials', is_flag=True,
              help='Flag to resolve credentials that can be found '
                   'in the cli session')
@cli_response(attributes_order=attributes_order)
def aws(
    ctx: ContextObj,
    tenant_name: str,
    ruleset: tuple[str, ...],
    region: tuple[str, ...],
    rules_to_scan: tuple[str, ...],
    license_key: str,
    dojo_product: str | None,
    dojo_engagement: str | None,
    dojo_test: str | None,
    access_key_id: str | None,
    secret_access_key: str | None,
    session_token: str | None,
    resolve_local_credentials: bool,
    customer_id: str | None,
):
    """Submit a job to scan an AWS tenant"""
    tenant = get_tenant(ctx, tenant_name, customer_id)
    if isinstance(tenant, SREResponse):
        return tenant
    if tenant['cloud'].lower() != 'aws':
        return SREResponse.build(
            f'Tenant {tenant_name} is not an AWS tenant',
            code=HTTPStatus.BAD_REQUEST,
        )

    credentials = resolve_credentials(
        tenant=tenant,
        resolver_type=AWSCredentialsResolver,
        resolver_kwargs={
            'aws_access_key_id': access_key_id,
            'aws_secret_access_key': secret_access_key,
            'aws_session_token': session_token,
        },
        resolve_local_credentials=resolve_local_credentials,
    )

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=credentials,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan),
        license_key=license_key,
        dojo_product=dojo_product,
        dojo_engagement=dojo_engagement,
        dojo_test=dojo_test,
    )


@scan.command(cls=ViewCommand, name='azure')
@cloud_submit_options
@click.option('--subscription_id',  type=str,
              help='Azure subscription id')
@click.option('--tenant_id', type=str, help='Azure tenant id')
@click.option('--client_id', type=str, help='Azure client id')
@click.option('--client_secret', type=str, help='Azure client secret')
@click.option('--resolve_local_credentials', is_flag=True,
              help='Flag to resolve credentials that can be found '
                   'in the cli session')
@cli_response(attributes_order=attributes_order)
def azure(
    ctx: ContextObj,
    tenant_name: str,
    ruleset: tuple[str, ...],
    region: tuple[str, ...],
    rules_to_scan: tuple[str, ...],
    license_key: str,
    dojo_product: str | None,
    dojo_engagement: str | None,
    dojo_test: str | None,
    subscription_id: str | None,
    tenant_id: str | None,
    client_id: str | None,
    client_secret: str | None,
    resolve_local_credentials: bool,
    customer_id: str | None,
):
    """Submit a job to scan an Azure tenant"""
    tenant = get_tenant(ctx, tenant_name, customer_id)
    if isinstance(tenant, SREResponse):
        return tenant
    if tenant['cloud'].lower() != 'azure':
        return SREResponse.build(
            f'Tenant {tenant_name} is not an Azure tenant',
            code=HTTPStatus.BAD_REQUEST,
        )

    credentials = resolve_credentials(
        tenant=tenant,
        resolver_type=AZURECredentialsResolver,
        resolver_kwargs={
            'azure_subscription_id': subscription_id,
            'azure_tenant_id': tenant_id,
            'azure_client_id': client_id,
            'azure_client_secret': client_secret,
        },
        resolve_local_credentials=resolve_local_credentials,
    )

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=credentials,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan),
        license_key=license_key,
        dojo_product=dojo_product,
        dojo_engagement=dojo_engagement,
        dojo_test=dojo_test,
    )


@scan.command(cls=ViewCommand, name='gcp')
@cloud_submit_options
@click.option('--application_credentials_path', type=str,
              callback=validate_file_optional,
              help='Path to file with google credentials')
@click.option('--resolve_local_credentials', is_flag=True,
              help='Flag to resolve credentials that can be found '
                   'in the cli session')
@cli_response(attributes_order=attributes_order)
def gcp(
    ctx: ContextObj,
    tenant_name: str,
    ruleset: tuple[str, ...],
    region: tuple[str, ...],
    rules_to_scan: tuple[str, ...],
    license_key: str,
    dojo_product: str | None,
    dojo_engagement: str | None,
    dojo_test: str | None,
    application_credentials_path: str | None,
    resolve_local_credentials: bool,
    customer_id: str | None,
):
    """Submit a job to scan a Google Cloud tenant"""
    tenant = get_tenant(ctx, tenant_name, customer_id)
    if isinstance(tenant, SREResponse):
        return tenant
    if tenant['cloud'].lower() not in ('google', 'gcp'):
        return SREResponse.build(
            f'Tenant {tenant_name} is not a Google Cloud tenant',
            code=HTTPStatus.BAD_REQUEST,
        )

    credentials = resolve_credentials(
        tenant=tenant,
        resolver_type=GOOGLECredentialsResolver,
        resolver_kwargs={
            'google_application_credentials_path': (
                application_credentials_path
            ),
        },
        resolve_local_credentials=resolve_local_credentials,
    )

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=credentials,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan),
        license_key=license_key,
        dojo_product=dojo_product,
        dojo_engagement=dojo_engagement,
        dojo_test=dojo_test,
    )


@scan.command(cls=ViewCommand, name='k8s')
@click.option('-pid', '--platform_id', required=True, type=str,
              help='Platform id to scan')
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
@click.option('--license_key', '-lk', required=False, type=str,
              help='License key to utilize for this job in case an ambiguous '
                   'situation occurs')
@click.option('--token', '-t', type=str, required=False,
              help='Short-lived token to perform k8s scan with')
@dojo_product_option
@dojo_engagement_option
@dojo_test_option
@cli_response()
def k8s(
    ctx: ContextObj,
    platform_id: str,
    ruleset: tuple,
    license_key: str,
    customer_id: str | None,
    token: str | None,
    dojo_product: str | None,
    dojo_engagement: str | None,
    dojo_test: str | None,
):
    """Submit a job to scan a Kubernetes cluster"""
    return ctx['api_client'].k8s_job_post(
        platform_id=platform_id,
        target_rulesets=ruleset,
        customer_id=customer_id,
        token=token,
        license_key=license_key,
        dojo_product=dojo_product,
        dojo_engagement=dojo_engagement,
        dojo_test=dojo_test,
    )
