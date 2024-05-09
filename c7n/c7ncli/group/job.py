import json
import os
from pathlib import Path

import click

from c7ncli.group import ContextObj, ViewCommand, cli_response, response
from c7ncli.group import limit_option, next_option, tenant_option
from c7ncli.group.job_scheduled import scheduled
from c7ncli.service.constants import AWS, AZURE, GOOGLE, JobState, \
    ENV_AWS_ACCESS_KEY_ID, ENV_AWS_SESSION_TOKEN, ENV_AWS_SECRET_ACCESS_KEY, \
    ENV_AZURE_TENANT_ID, ENV_AZURE_CLIENT_ID, ENV_AZURE_CLIENT_SECRET, \
    ENV_AZURE_SUBSCRIPTION_ID
from c7ncli.service.constants import C7NCLI_DEVELOPER_MODE_ENV_NAME
from c7ncli.service.credentials import EnvCredentialsResolver
from c7ncli.service.helpers import Color
from c7ncli.service.logger import get_user_logger

USER_LOG = get_user_logger(__name__)

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
        return response('You do not have to specify account of tenant '
                        'name if job id is specified.')
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
@tenant_option
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
@click.option('--region', '-r', type=str, required=False,
              multiple=True,
              help='Regions to scan. If not specified, '
                   'all active regions will be used')
@click.option('--cloud', '-c', type=click.Choice((AWS, AZURE, GOOGLE)),
              required=False, help='Cloud to scan. Required, if '
                                   '`--credentials_from_env` flag is set.')
@click.option('--credentials_from_env', '-cenv', is_flag=True, default=False,
              help='Specify to get credentials for scan from environment variables. '
                   'Requires `--cloud` to be set.')
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
@cli_response(attributes_order=attributes_order)
def submit(ctx: ContextObj, cloud: str, tenant_name: str,
           ruleset: tuple[str, ...], region: tuple[str, ...],
           credentials_from_env: bool,
           customer_id: str | None, rules_to_scan: tuple[str]):
    """
    Submits a job to scan either AWS, AZURE or GOOGLE account
    """
    credentials = None
    if not cloud and credentials_from_env:
        raise click.UsageError('Error missing option \'--cloud\' / \'-c\'.')

    if credentials_from_env:
        resolver = EnvCredentialsResolver(cloud)
        try:
            credentials = resolver.resolve()
        except LookupError as e:
            USER_LOG.error(Color.red(str(e)))
            return response(str(e))

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=credentials,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan)
    )


@job.command(cls=ViewCommand, name='submit_aws')
@tenant_option
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
@click.option('--access_key', '-ak', type=str, help='AWS access key')
@click.option('--secret_key', '-sk', type=str, help='AWS secret key')
@click.option('--session_token', '-st', type=str, help='AWS session token')
@cli_response(attributes_order=attributes_order)
def submit_aws(ctx: ContextObj,  tenant_name: str,
               ruleset: tuple[str, ...], region: tuple[str, ...],
               customer_id: str | None, rules_to_scan: tuple[str],
               access_key, secret_key, session_token):
    """
    Submits a job to scan an AWS account
    """
    if access_key and secret_key and session_token:
        creds = {
            ENV_AWS_ACCESS_KEY_ID: access_key,
            ENV_AWS_SECRET_ACCESS_KEY: secret_key,
            ENV_AWS_SESSION_TOKEN: session_token
        }
    elif access_key and secret_key:
        creds = {
            ENV_AWS_ACCESS_KEY_ID: access_key,
            ENV_AWS_SECRET_ACCESS_KEY: secret_key,
        }
    elif not any((access_key, secret_key, session_token)):
        creds = None
    else:
        return response('either provide --access_key and --secret_key and '
                        'optionally --session_token or do not provide anything')
    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=creds,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan)
    )


@job.command(cls=ViewCommand, name='submit_azure')
@tenant_option
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
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
@click.option('--tenant_id', '-ti', type=str, help='Azure tenant id')
@click.option('--client_id', '-ci', type=str, help='Azure client id')
@click.option('--client_secret', '-cs', type=str, help='Azure client secret')
@click.option('--subscription_id', '-si', type=str,
              help='Azure subscription id')
@cli_response(attributes_order=attributes_order)
def submit_azure(ctx: ContextObj, tenant_name: str,
                 ruleset: tuple[str, ...], region: tuple[str, ...],
                 customer_id: str | None, rules_to_scan: tuple[str],
                 tenant_id, client_id, client_secret, subscription_id):
    """
    Submits a job to scan an Azure subscription
    """
    if tenant_id and client_id and client_secret and subscription_id:
        creds = {
            ENV_AZURE_CLIENT_ID: client_id,
            ENV_AZURE_TENANT_ID: tenant_id,
            ENV_AZURE_CLIENT_SECRET: client_secret,
            ENV_AZURE_SUBSCRIPTION_ID: subscription_id
        }
    elif tenant_id and client_id and client_secret:
        creds = {
            ENV_AZURE_CLIENT_ID: client_id,
            ENV_AZURE_TENANT_ID: tenant_id,
            ENV_AZURE_CLIENT_SECRET: client_secret,
        }
    elif not any((tenant_id, client_id, client_secret, subscription_id)):
        creds = None
    else:
        return response(
            'Provide --tenant_id, --client_id, --client_secret '
            'and optionally --subscription_id or do not provide anything'
        )
    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=creds,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan)
    )


@job.command(cls=ViewCommand, name='submit_google')
@tenant_option
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
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
@click.option('--application_credentials_path', '-acp', type=str,
              help='Path to file with google credentials')
@cli_response(attributes_order=attributes_order)
def submit_google(ctx: ContextObj, tenant_name: str,
                  ruleset: tuple[str, ...], region: tuple[str, ...],
                  customer_id: str | None, rules_to_scan: tuple[str],
                  application_credentials_path: str):
    """
    Submits a job to scan a Google project
    """
    if application_credentials_path:
        path = Path(application_credentials_path)
        if not path.exists() or not path.is_file():
            return response('provided path must point to existing file')
        with open(path, 'r') as file:
            try:
                creds = json.load(file)
            except json.JSONDecodeError:
                return response('cannot load json')
    else:
        creds = None

    return ctx['api_client'].job_post(
        tenant_name=tenant_name,
        target_rulesets=ruleset,
        target_regions=region,
        credentials=creds,
        customer_id=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan)
    )


@job.command(cls=ViewCommand, name='submit_k8s')
@click.option('-pid', '--platform_id', required=True, type=str)
@click.option('--ruleset', '-rs', type=str, required=False,
              multiple=True,
              help='Rulesets to scan. If not specified, all available by '
                   'license rulesets will be used')
@click.option('--token', '-t', type=str, required=False,
              help='Short-lived token to perform k8s scan with')
@cli_response()
def submit_k8s(ctx: ContextObj, platform_id: str, ruleset: tuple,
               customer_id: str | None, token: str | None):
    """
    Submits a job for kubernetes cluster
    """
    return ctx['api_client'].k8s_job_post(
        platform_id=platform_id,
        target_rulesets=ruleset,
        customer_id=customer_id,
        token=token
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
                content = '[]'
        else:
            content = item
        try:
            loaded = json.loads(content)
            if isinstance(loaded, list):
                rules.update(loaded)
        except json.JSONDecodeError as e:
            rules.add(content)
    return list(rules)


if str(os.getenv(C7NCLI_DEVELOPER_MODE_ENV_NAME)).lower() == 'true':
    from c7ncli.group.job_event import event

    job.add_command(event)
