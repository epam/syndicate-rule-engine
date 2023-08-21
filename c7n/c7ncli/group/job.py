import json
import os
from pathlib import Path
from typing import Tuple, Optional

import click

from c7ncli.group import cli_response, ViewCommand, response, ContextObj, \
    customer_option
from c7ncli.group import tenant_option, limit_option, next_option
from c7ncli.group.job_scheduled import scheduled
from c7ncli.service.constants import C7NCLI_DEVELOPER_MODE_ENV_NAME
from c7ncli.service.constants import PARAM_JOB_ID, PARAM_STARTED_AT, \
    PARAM_JOB_OWNER, PARAM_STATUS, \
    PARAM_STOPPED_AT, PARAM_SCAN_REGIONS, PARAM_SUBMITTED_AT, \
    PARAM_SCAN_RULESETS, PARAM_CREATED_AT, AWS, AZURE, GOOGLE
from c7ncli.service.credentials import EnvCredentialsResolver
from c7ncli.service.helpers import Color
from c7ncli.service.logger import get_user_logger

USER_LOG = get_user_logger(__name__)

AVAILABLE_CLOUDS = [AWS, AZURE, GOOGLE]


@click.group(name='job')
def job():
    """Manages Custodian Service jobs"""


@job.command(cls=ViewCommand, name='describe')
@click.option('--job_id', '-id', type=str, required=False,
              help='Job id to describe')
@tenant_option
@customer_option
@limit_option
@next_option
@cli_response(attributes_order=[PARAM_JOB_ID, PARAM_JOB_OWNER, PARAM_STATUS,
                                PARAM_SCAN_REGIONS, PARAM_SCAN_RULESETS,
                                PARAM_SUBMITTED_AT, PARAM_STARTED_AT,
                                PARAM_STOPPED_AT])
def describe(ctx: ContextObj, job_id: str, tenant_name: str, customer_id: str,
             limit: int, next_token: str):
    """
    Describes Custodian Service Scans
    """

    if job_id and tenant_name:
        return response('You do not have to specify account of tenant '
                        'name if job id is specified.')
    if job_id:
        return ctx['api_client'].job_get(job_id)
    return ctx['api_client'].job_list(
        tenant_name=tenant_name,
        customer=customer_id,
        limit=limit,
        next_token=next_token
    )


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
@click.option('--not_check_permission', '-ncp', is_flag=True, default=False,
              help='Force the server not to check execution permissions. '
                   'Job that is not permitted but has started will '
                   'eventually fail')
@click.option('--cloud', '-c', type=click.Choice(AVAILABLE_CLOUDS),
              required=False, help='Cloud to scan. Required, if '
                                   '`--credentials_from_env` flag is set.')
@click.option('--credentials_from_env', '-cenv', is_flag=True, default=False,
              help='Specify to get credentials for scan from environment variables. '
                   'Requires `--cloud` to be set.')
@click.option('--rules_to_scan', required=False, multiple=True, type=str,
              help='Concrete rule ids to scan. These rules will be filtered '
                   'from the ruleset and scanned. '
                   'Can be json string with list of rules or path to a '
                   'json file with list or rules')
@customer_option
@cli_response(
    attributes_order=[PARAM_JOB_ID, PARAM_JOB_OWNER, PARAM_STATUS,
                      PARAM_SUBMITTED_AT, PARAM_CREATED_AT,
                      PARAM_STARTED_AT, PARAM_STOPPED_AT]
)
def submit(ctx: ContextObj, cloud: str, tenant_name: str,
           ruleset: Tuple[str, ...], region: Tuple[str, ...],
           not_check_permission: bool, credentials_from_env: bool,
           customer_id: Optional[str], rules_to_scan: Optional[Tuple[str]]):
    """
    Submits a job to scan an infrastructure
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
        check_permission=not not_check_permission,
        credentials=credentials,
        customer=customer_id,
        rules_to_scan=load_rules_to_scan(rules_to_scan)
    )


@job.command(cls=ViewCommand, name='terminate')
@click.option('--job_id', '-id', type=str, required=True,
              help='Job id to terminate')
@cli_response()
def terminate(ctx: ContextObj, job_id: str):
    """
    Terminates Custodian Service Scan
    """
    return ctx['api_client'].job_delete(job_id=job_id)


job.add_command(scheduled)


def load_rules_to_scan(rules_to_scan: Tuple[str, ...]) -> list:
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
