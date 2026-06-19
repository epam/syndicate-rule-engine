import operator
from collections import namedtuple
from pathlib import Path

from smoke.core.cli import cmd
from smoke.core.commons import (
    Case,
    Condition,
    Empty,
    Equal,
    In,
    IsInstance,
    Len,
    NotEmpty,
    Step,
    True_,
    WaitUntil,
    write_cases,
)
from smoke.core.settings import SmokeSettings, get_settings

TenantPayload = namedtuple('TenantPayload', ['name', 'regions'])

JOB_TYPE = In('manual', 'standard')


class TenantRegionsType:
    def __call__(self, item: str) -> TenantPayload:
        res = item.split(':', maxsplit=1)
        if len(res) == 1:
            return TenantPayload(name=res[0], regions=[])
        name, regions = res
        return TenantPayload(name=name, regions=regions.split(','))


def build_authentication_case(
    *,
    username: str,
    password: str,
    api_link: str,
) -> Case:
    return Case(
        steps=(
            Step(
                cmd(f'configure --api_link {api_link} --json'),
                {
                    '$.message': Equal(
                        'Great! The sre cli tool api_link has been configured.'
                    )
                },
            ),
            Step(
                cmd('show_config'),
                {
                    '$.api_link': Equal(api_link),
                },
            ),
            Step(
                cmd(f'login -u {username} -p {password} --json'),
                {
                    '$.message': Equal(
                        'Great! The sre cli tool access token has been saved'
                    )
                },
            ),
            Step(cmd('health_check --status NOT_OK'), {'$.items': Empty()}),
        ),
        name='Authentication',
    )


def build_entities_describe_case(
    *,
    settings: SmokeSettings,
    customer: str,
    customer_check: Condition,
    tenants: list[TenantPayload],
) -> Case:
    return Case(
        steps=(
            Step(
                cmd('customer describe'),
                {
                    '$.items[0].name': customer_check,
                },
            ),
            Step(
                cmd('customer rabbitmq describe', customer),
                {
                    '$.data.customer': customer_check,
                    '$.data.maestro_user': NotEmpty(),
                    '$.data.request_queue': NotEmpty(),
                    '$.data.response_queue': NotEmpty(),
                    '$.data.sdk_access_key': NotEmpty(),
                },
            ),
            *[
                Step(
                    cmd(f'tenant describe -tn {tenant.name}', customer),
                    {
                        '$.data.name': Equal(tenant.name),
                        '$.data.activation_date': NotEmpty(),
                        '$.data.customer_name': customer_check,
                        '$.data.is_active': True_(),
                        '$.data.account_id': NotEmpty(),
                        '$.data.regions': Len(operator.ge, 1),
                    },
                )
                for tenant in tenants
            ],
            Step(
                cmd('policy describe', customer),
                {
                    '$.items[0].customer': customer_check,
                    '$.items[0].name': NotEmpty(),
                    '$.items[0].permissions': IsInstance(list),
                },
            ),
            Step(
                cmd('role describe', customer),
                {
                    '$.items[0].name': NotEmpty(),
                    '$.items[0].customer': customer_check,
                    '$.items[0].policies': IsInstance(list),
                },
            ),
            Step(
                cmd('setting lm client describe'),
                {
                    '$.data.algorithm': Equal('ECC:p521_DSS_SHA:256'),
                    '$.data.b64_encoded': IsInstance(bool),
                    '$.data.format': NotEmpty() & IsInstance(str),
                    '$.data.key_id': NotEmpty() & IsInstance(str),
                    '$.data.public_key': NotEmpty() & IsInstance(str),
                },
            ),
            Step(
                cmd('setting lm config describe'),
                {
                    '$.data.host': NotEmpty() & IsInstance(str),
                    '$.data.port': NotEmpty() & IsInstance(int),
                    '$.data.protocol': In('HTTP', 'HTTPS'),
                },
            ),
            Step(
                cmd('ruleset describe -ls False', customer),
                {
                    '$.items': IsInstance(list),
                },
            ),
            Step(
                cmd('ruleset describe -ls True', customer),
                {
                    '$.items': IsInstance(list),
                },
            ),
            Step(
                cmd('service operations status --operation metrics_update'),
                {
                    '$.items': IsInstance(list),
                },
            ),
        ),
        name='Entities describe',
    )


def build_executing_scans_cases(
    *,
    customer: str,
    customer_check: Condition,
    tenants: list[TenantPayload],
) -> list[Case]:
    cases: list[Case] = []
    for tenant in tenants:
        _regions = ' '.join(f'--region {r}' for r in tenant.regions)
        job_submit_step = Step(
            cmd(
                f'job submit --tenant_name {tenant.name} {_regions}', customer
            ),
            {
                '$.data.id': NotEmpty(),
                '$.data.status': In('SUBMITTED', 'PENDING'),
                '$.data.customer_name': customer_check,
                '$.data.tenant_name': Equal(tenant.name),
            },
        )
        cases.append(
            Case(
                steps=(
                    job_submit_step,
                    WaitUntil(
                        cmd('job describe -id $.[0].data.id', customer),
                        {'$.data.status': Equal('SUCCEEDED')},
                        break_if={'$.data.status': Equal('FAILED')},
                        depends_on=(job_submit_step,),
                        sleep=15,
                        timeout=1800,
                    ),
                    Step(
                        cmd('job describe -id $.[0].data.id', customer),
                        {
                            '$.data.rulesets': Len(operator.ge, 1),
                            '$.data.stopped_at': NotEmpty(),
                        },
                        depends_on=(job_submit_step,),
                    ),
                ),
                name=f'Executing scans for tenant: {tenant.name}',
            )
        )

        cases.append(
            Case(
                steps=(
                    Step(
                        cmd(
                            'report compliance jobs -id $.[0].data.id',
                            customer,
                        ),
                        {
                            '$.data.job_type': JOB_TYPE,
                            **{
                                f'$.data.content.{region}.HIPAA': NotEmpty()
                                for region in tenant.regions
                            },
                            **{
                                f'$.data.content.{region}.NERC-CIP': NotEmpty()
                                for region in tenant.regions
                            },
                        },
                        depends_on=(job_submit_step,),
                    ),
                    Step(
                        cmd(
                            f'report compliance accumulated -tn {tenant.name}',
                            customer,
                        ),
                        {
                            **{
                                f'$.data.content.{region}.HIPAA': NotEmpty()
                                for region in tenant.regions
                            },
                            **{
                                f'$.data.content.{region}.NERC-CIP': NotEmpty()
                                for region in tenant.regions
                            },
                        },
                        depends_on=(job_submit_step,),
                    ),
                    Step(
                        cmd('report digests jobs -id $.[0].data.id', customer),
                        {
                            '$.data.job_type': JOB_TYPE,
                            '$.data.content.total_checks': NotEmpty(),
                            '$.data.content.successful_checks': NotEmpty(),
                            '$.data.content.failed_checks': NotEmpty(),
                            '$.data.content.violating_resources': NotEmpty(),
                        },
                        depends_on=(job_submit_step,),
                    ),
                    Step(
                        cmd('report errors jobs -id $.[0].data.id', customer),
                        {
                            '$.items[0].type': JOB_TYPE | Empty(),
                            '$.items[0].content': IsInstance(dict) | Empty(),
                        },
                        depends_on=(job_submit_step,),
                    ),
                    Step(
                        cmd('report rules jobs -id $.[0].data.id', customer),
                        {
                            '$.items': NotEmpty() & IsInstance(list),
                            '$.items[0].policy': NotEmpty(),
                            '$.items[0].region': NotEmpty(),
                        },
                        depends_on=(job_submit_step,),
                    ),
                    Step(
                        cmd('report details jobs -id $.[0].data.id', customer),
                        {
                            '$.data.job_type': JOB_TYPE,
                            '$.data.job_id': NotEmpty(),
                            '$.data.content': IsInstance(dict),
                            **{
                                f'$.data.content.{region}': IsInstance(list)
                                for region in tenant.regions
                            },
                        },
                        depends_on=(job_submit_step,),
                    ),
                ),
                name=f'Generating reports for tenant: {tenant.name}',
            )
        )
    return cases


def run_main_flow(
    *,
    username: str,
    password: str,
    api_link: str,
    tenants: list[TenantPayload],
    customer: str,
    filename: Path | None = None,
) -> list[Case]:
    settings = get_settings()
    _customer_check = Equal(customer)

    cases: list[Case] = [
        build_authentication_case(
            username=username,
            password=password,
            api_link=api_link,
        ),
        build_entities_describe_case(
            settings=settings,
            customer=customer,
            customer_check=_customer_check,
            tenants=tenants,
        ),
        *build_executing_scans_cases(
            customer=customer,
            customer_check=_customer_check,
            tenants=tenants,
        ),
    ]

    for case in cases:
        case.execute()

    if filename:
        filename.parent.mkdir(parents=True, exist_ok=True)
        write_cases(
            cases,
            name=str(filename),
        )
    else:
        write_cases(cases)

    return cases
