import json
from pathlib import Path

import click

from srecli.group import ContextObj, ViewCommand, cli_response
from srecli.group import limit_option, next_option, tenant_option, \
    build_tenant_option, response
from srecli.group.job_scheduled import scheduled
from srecli.service.constants import Env
from srecli.service.constants import JobState, TenantModel
from srecli.service.creds import AWSCredentialsResolver, \
    AZURECredentialsResolver, GOOGLECredentialsResolver, CredentialsLookupError
from srecli.service.logger import get_logger

_LOG = get_logger(__name__)

attributes_order = 'id', 'tenant_name', 'status', 'submitted_at',


@click.group(name='job')
def job():
    """Manages Custodian Service jobs"""


@job.command(cls=ViewCommand, name='describe')
@click.option('--job_id', '-id', type=str, required=False,
              help='Job id to describe')
@tenant_option
@click.option('--status', '-s', type=click.Choice(tuple(JobState.iter())),
              required=False, help='Status to query jobs by')
@click.option('--from_date', '-from', type=str,
              help='Query jobs from this date. Accepts date ISO string. '
                   'Example: 2023-10-20')
@click.option('--to_date', '-to', type=str,
              help='Query jobs till this date. Accepts date ISO string. '
                   'Example: 2023-10-20')
@limit_option
@next_option
@cli_response(attributes_order=attributes_order)
def describe(ctx: ContextObj, job_id: str, tenant_name: str, customer_id: str,
             limit: int, next_token: str, status: str, from_date: str,
             to_date: str):
    """
    Describes Custodian Service Scans
    """

    if job_id and tenant_name:
        raise click.ClickException(
            'You do not have to specify account of tenant name '
            'if job id is specified.'
        )
    if job_id:
        return ctx['api_client'].job_get(job_id, customer_id=customer_id)
    dct = {
        'tenant_name': tenant_name,
        'customer_id': customer_id,
        'limit': limit,
        'next_token': next_token,
        'status': status,
        'from': from_date,
        'to': to_date
    }
    return ctx['api_client'].job_list(**dct)


@job.command(cls=ViewCommand, name='submit')
@build_tenant_option(required=True)
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
@click.option('--region', '-r', type=str, required=False,
              multiple=True,
              help='Regions to scan. If not specified, '
                   'all active regions will be used')
@click.option('--rules_to_scan', required=False, multiple=True, type=str,
              help='Rules that must be scanned. Ruleset must contain them. '
                   'You can specify some subpart of rule names. Custodian '
                   'will try to resolve the full names: aws-002 -> '
                   'ecc-aws-002-encryption... Also you can specify some part '
                   'that is common to multiple rules. All the them will be '
                   'resolved: postgresql -> [ecc-aws-001-postgresql..., '
                   'ecc-aws-002-postgresql...]. This CLI param can accept '
                   'both raw rule names and path to file with JSON list '
                   'of rules')
@click.option('--license_key', '-lk', required=False, type=str,
              help='License key to utilize for this job in case an ambiguous '
                   'situation occurs')
@click.option('--aws_access_key_id', type=str, help='AWS access key')
@click.option('--aws_secret_access_key', type=str, help='AWS secret key')
@click.option('--aws_session_token', type=str, help='AWS session token')
@click.option('--azure_subscription_id',  type=str,
              help='Azure subscription id')
@click.option('--azure_tenant_id', type=str, help='Azure tenant id')
@click.option('--azure_client_id', type=str, help='Azure client id')
@click.option('--azure_client_secret', type=str, help='Azure client secret')
@click.option('--google_application_credentials_path', type=str,
              help='Path to file with google credentials')
@click.option('--only_preconfigured_credentials', is_flag=True,
              help='Specify flag to ignore any credentials that can be found '
                   'in cli session and use those that are preconfigured '
                   'by admin')
@cli_response(attributes_order=attributes_order)
def submit(ctx: ContextObj, tenant_name: str,
           ruleset: tuple[str, ...], region: tuple[str, ...],
           customer_id: str | None, rules_to_scan: tuple[str, ...],
           license_key: str, aws_access_key_id: str | None,
           aws_secret_access_key: str | None, aws_session_token: str | None,
           azure_subscription_id: str | None, azure_tenant_id: str | None,
           azure_client_id: str | None, azure_client_secret: str | None,
           google_application_credentials_path: str | None,
           only_preconfigured_credentials: bool):
    """
    Submits a job to scan either AWS, AZURE or GOOGLE account
    """
    resp = ctx['api_client'].tenant_get(tenant_name, customer_id=customer_id)
    # todo cache in temp files
    if not resp.was_sent or not resp.ok:
        return resp
    tenant: TenantModel = next(resp.iter_items())
    match tenant['cloud']:
        case 'AWS': resolver = AWSCredentialsResolver(tenant)
        case 'AZURE': resolver = AZURECredentialsResolver(tenant)
        case 'GOOGLE' | 'GCP': resolver = GOOGLECredentialsResolver(tenant)
        case _: return response('Not supported tenant cloud')  # newer happen

    if only_preconfigured_credentials:
        creds = {}
    else:
        try:
            creds = resolver.resolve(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                azure_subscription_id=azure_subscription_id,
                azure_tenant_id=azure_tenant_id,
                azure_client_id=azure_client_id,
                azure_client_secret=azure_client_secret,
                google_application_credentials_path=google_application_credentials_path
            )
        except CredentialsLookupError as e:
            _LOG.warning(f'Could not find credentials: {e}')
            creds = {}

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=creds,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan),
        license_key=license_key
    )


@job.command(cls=ViewCommand, name='submit_k8s')
@click.option('-pid', '--platform_id', required=True, type=str)
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
@click.option('--license_key', '-lk', required=False, type=str,
              help='License key to utilize for this job in case an ambiguous '
                   'situation occurs')
@click.option('--token', '-t', type=str, required=False,
              help='Short-lived token to perform k8s scan with')
@cli_response()
def submit_k8s(ctx: ContextObj, platform_id: str, ruleset: tuple,
               license_key: str,
               customer_id: str | None, token: str | None):
    """
    Submits a job for kubernetes cluster
    """
    return ctx['api_client'].k8s_job_post(
        platform_id=platform_id,
        target_rulesets=ruleset,
        customer_id=customer_id,
        token=token,
        license_key=license_key
    )


@job.command(cls=ViewCommand, name='terminate')
@click.option('--job_id', '-id', type=str, required=True,
              help='Job id to terminate')
@cli_response()
def terminate(ctx: ContextObj, job_id: str, customer_id):
    """
    Terminates Custodian Service Scan
    """
    return ctx['api_client'].job_delete(job_id=job_id, customer_id=customer_id)


job.add_command(scheduled)


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
            except Exception as e:  # file read error
                content = '[]'  # todo raise
        else:
            content = item
        try:
            loaded = json.loads(content)
            if isinstance(loaded, list):
                rules.update(loaded)
        except json.JSONDecodeError as e:
            rules.add(content)
    return list(rules)


if Env.DEVELOPER_MODE.get():
    from srecli.group.job_event import event

    job.add_command(event)
