import argparse
import operator
from collections import namedtuple
from typing import List, Optional
from pathlib import Path
from commons import Case, Step, Equal, Empty, write_cases, NotEmpty, True_, \
    IsInstance, Len, In, WaitUntil, Contains

TenantPayload = namedtuple('TenantPayload', ['name', 'regions'])


class TenantRegionsType:
    def __init__(self):
        pass

    def __call__(self, item: str) -> TenantPayload:
        res = item.split(':', maxsplit=1)
        if len(res) == 1:
            return TenantPayload(name=res[0], regions=[])
        # len(res) == 2
        name, regions = res
        return TenantPayload(name=name, regions=regions.split(','))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Entrypoint for smokes',
    )
    parser.add_argument('--username', required=True, type=str)
    parser.add_argument('--password', required=True, type=str)
    parser.add_argument('--api_link', required=True, type=str)
    parser.add_argument('--tenants', nargs='+', required=True,
                        type=TenantRegionsType(),
                        help='Tenant to list of regions: '
                             '--tenants EOOS:eu-central-1,eu-west-1 '
                             'CIT2:eu-west-1')
    parser.add_argument('--customer', required=False, type=str,
                        help='Tenants to submit jobs for')

    def markdown(value: str) -> Path:
        if not value.endswith('.md'):
            value = value + '.md'
        return Path(value)
    parser.add_argument('--filename', required=False, type=markdown,
                        help='Output file')
    return parser


def main(username: str, password: str, api_link: str,
         tenants: List[TenantPayload], customer: Optional[str],
         filename: Optional[Path]):
    _customer_check = Equal(customer) if customer else NotEmpty()
    authentication_case = Case(steps=(
        Step(f'c7n configure --api_link {api_link} --json', {
            '$.message': Equal(
                'Great! The c7n tool api_link has been configured.')
        }),
        Step('c7n show_config', {
            '$.api_link': Equal(api_link),
        }),
        Step(f'c7n login -u {username} -p {password} --json',
             {
                 '$.message': Equal(
                     f'Great! The c7n tool access token has been saved.')
             }),
        Step('c7n health_check --status NOT_OK', {
            '$.items': Empty()
        }),
    ), name='Authentication')
    entities_describe_case = Case(steps=(
        Step(f'c7n customer describe', {
            '$.items[0].name': _customer_check,
        }),
        Step(f'c7n customer rabbitmq describe', {
            '$.data.customer': _customer_check,
            '$.data.maestro_user': NotEmpty(),
            '$.data.request_queue': NotEmpty(),
            '$.data.response_queue': NotEmpty(),
            '$.data.sdk_access_key': NotEmpty(),
        }),
        *[Step(f'c7n tenant describe -tn {tenant.name}', {
            '$.data.name': Equal(tenant.name),
            '$.data.activation_date': NotEmpty(),
            '$.data.customer_name': _customer_check,
            '$.data.is_active': True_(),
            '$.data.account_id': NotEmpty(),
            '$.data.regions': Len(operator.ge, 1)
        }) for tenant in tenants],
        Step(f'c7n policy describe', {
            '$.items[0].customer': _customer_check,
            '$.items[0].name': NotEmpty(),
            '$.items[0].permissions': IsInstance(list)
        }),  # at least one
        Step('c7n role describe', {
            '$.items[0].name': NotEmpty(),
            '$.items[0].customer': _customer_check,
            '$.items[0].policies': IsInstance(list)
        }),
        Step('c7n setting lm client describe', {
            '$.data.algorithm': Equal('ECC:p521_DSS_SHA:256'),
            '$.data.b64_encoded': IsInstance(bool),
            '$.data.format': NotEmpty() & IsInstance(str),
            '$.data.key_id': NotEmpty() & IsInstance(str),
            '$.data.public_key': NotEmpty() & IsInstance(str)
        }),
        Step('c7n setting lm config describe', {
            '$.data.host': NotEmpty() & IsInstance(str),
            '$.data.port': NotEmpty() & IsInstance(int),
            '$.data.protocol': In('HTTP', 'HTTPS'),
            '$.data.stage': IsInstance(str),
        }),
        Step('c7n setting mail describe', {
            '$.data.default_sender': NotEmpty() & Contains(
                '.com') & Contains('@'),
            '$.data.host': NotEmpty(),
            '$.data.max_emails': IsInstance(int),
            '$.data.password': NotEmpty(),
            '$.data.port': IsInstance(int),
            '$.data.username': NotEmpty(),
        }),
        Step('c7n results describe', {
            '$.items': IsInstance(list)
        }),
        Step('c7n ruleset describe -ls False', {
            '$.items': IsInstance(list)
        }),
        Step('c7n ruleset describe -ls True', {
            '$.items': IsInstance(list) & NotEmpty(),
            '$.items[0].customer': Equal('CUSTODIAN_SYSTEM'),
            '$.items[0].name': NotEmpty(),
            '$.items[0].version': NotEmpty() & IsInstance(str),
            '$.items[0].cloud': In('AWS', 'AZURE', 'GCP', 'KUBERNETES'),
            '$.items[0].rules_number': IsInstance(int),
            '$.items[0].active': True_(),
            '$.items[0].license_keys': IsInstance(list),
            '$.items[0].license_manager_id': NotEmpty(),
            '$.items[0].licensed': True_()
        }),
        Step('c7n metrics status', {
            '$.message': Equal('Cannot find latest metrics update job') | (
                        Contains('Last metrics update was started at') & Contains('with status SUCCESS'))
        })
    ), name='Entities describe')

    executing_scans_cases = []
    for tenant in tenants:
        _regions = ' '.join(f'--region {r}' for r in tenant.regions)
        job_submit_step = Step(
            f'c7n job submit --tenant_name {tenant.name} {_regions}', {
                '$.data.id': NotEmpty(),
                '$.data.status': Equal('SUBMITTED'),
                '$.data.customer_name': _customer_check,
                '$.data.tenant_name': Equal(tenant.name)
            })
        executing_scans_cases.append(Case(steps=(
            job_submit_step,
            WaitUntil(f'c7n job describe -id $.[0].data.id', {
                '$.data.status': Equal('SUCCEEDED')
            }, break_if={
                '$.data.status': Equal('FAILED')
            }, depends_on=(job_submit_step,), sleep=15, timeout=1800),
            Step(f'c7n job describe -id $.[0].data.id', {
                '$.data.rulesets': Len(operator.eq, 1),
                '$.data.stopped_at': NotEmpty()
            }, depends_on=(job_submit_step,))
        ), name=f'Executing scans for tenant: {tenant.name}'))

        executing_scans_cases.append(Case(steps=(
            Step('c7n report compliance jobs -id $.[0].data.id', {
                '$.data.job_type': Equal('manual'),
                **{
                    f'$.data.content.{region}.HIPAA': NotEmpty()
                    for region in tenant.regions
                },
                **{
                    f'$.data.content.{region}.NERC-CIP': NotEmpty()
                    for region in tenant.regions
                },
            }, depends_on=(job_submit_step,)),
            Step(f'c7n report compliance accumulated -tn {tenant.name}', {
                **{
                    f'$.data.content.{region}.HIPAA': NotEmpty()
                    for region in tenant.regions
                },
                **{
                    f'$.data.content.{region}.NERC-CIP': NotEmpty()
                    for region in tenant.regions
                },
            }, depends_on=(job_submit_step,)),
            Step('c7n report digests jobs -id $.[0].data.id', {
                '$.data.job_type': Equal('manual'),
                '$.data.content.total_checks': NotEmpty(),
                '$.data.content.successful_checks': NotEmpty(),
                '$.data.content.failed_checks': NotEmpty(),
                '$.data.content.violating_resources': NotEmpty(),
            }, depends_on=(job_submit_step,)),
            Step('c7n report errors jobs -id $.[0].data.id', {
                '$.items[0].type': Equal('manual') | Empty(),
                '$.items[0].content': IsInstance(dict) | Empty(),
            }, depends_on=(job_submit_step,)),
            Step('c7n report rules jobs -id $.[0].data.id', {
                '$.items': NotEmpty() & IsInstance(list),
                '$.items[0].policy': NotEmpty(),
                '$.items[0].region': NotEmpty(),
            }, depends_on=(job_submit_step,)),
            Step('c7n report details jobs -id $.[0].data.id', {
                '$.data.job_type': Equal('manual'),
                '$.data.job_id': NotEmpty(),
                '$.data.content': IsInstance(dict),
                **{
                    f'$.data.content.{region}': IsInstance(list)
                    for region in tenant.regions
                },
            }, depends_on=(job_submit_step,))
        ), name=f'Generating reports for tenant: {tenant.name}'))

    cases = (
        authentication_case,
        entities_describe_case,
        *executing_scans_cases
    )
    for case in cases:
        case.execute()
    if filename:
        filename.parent.mkdir(parents=True, exist_ok=True)
        filename = str(filename)
    write_cases(list(cases), filename)


if __name__ == '__main__':
    main(**vars(build_parser().parse_args()))
