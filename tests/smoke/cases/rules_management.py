import operator

from smoke.core.cli import cmd
from smoke.core.commons import (
    Case,
    Contains,
    Empty,
    Equal,
    False_,
    IsInstance,
    Len,
    NotEmpty,
    Step,
    WaitUntil,
    write_cases,
)
from smoke.core.settings import (
    RuleSourceSettings,
    SmokeSettings,
    get_rule_source,
    get_settings,
)


def build_authentication_case(
    *,
    settings: SmokeSettings,
) -> Case:
    return Case(
        steps=(
            Step(
                cmd(f'configure --api_link {settings.api_link} --json'),
                {
                    '$.message': Equal(
                        'Great! The sre cli tool api_link has been configured.'
                    )
                },
            ),
            Step(
                cmd(
                    f'login -u {settings.username} -p {settings.password} --json'
                ),
                {
                    '$.message': Equal(
                        'Great! The sre cli tool access token has been saved'
                    )
                },
            ),
        ),
        name='Authentication',
    )


def case_for_source(
    *,
    settings: SmokeSettings,
    source: RuleSourceSettings,
) -> Case:
    s = source
    customer = settings.customer
    assert customer is not None

    rs_add_step = Step(
        cmd(
            'rulesource add '
            f'--git_project_id {s.pid} --git_url {s.url} --git_ref {s.ref} '
            f'--git_rules_prefix {s.prefix} --description {s.cloud}',
            customer,
        )
        + ('' if not s.secret else f' --git_access_secret {s.secret}'),
        {},
    )
    rs_resolve_step = Step(
        cmd(f'rulesource describe -gpid {s.pid}', customer),
        {
            '$.items[0].customer': Equal(customer),
            '$.items[0].git_project_id': Equal(s.pid),
            '$.items[0].git_url': Equal(s.url),
            '$.items[0].git_ref': Equal(s.ref),
            '$.items[0].description': Equal(s.cloud),
            '$.items[0].type': Equal('GITHUB') | Equal('GITLAB'),
            '$.items[0].id': NotEmpty(),
        },
        depends_on=(rs_add_step,),
    )
    rs_delete_step = Step(
        cmd('rulesource delete --rule_source_id $.[0].items[0].id', customer),
        {'$.message': Equal('Request is successful. No content returned')},
        depends_on=(rs_resolve_step,),
    )

    rule_update_step = Step(
        cmd('rule update -rsid $.[0].items[0].id', customer),
        {
            '$.items[0].status': Contains('To check:'),
            '$.items[0].customer': Equal(customer),
            '$.items[0].git_project_id': Equal(s.pid),
        },
        depends_on=(rs_resolve_step,),
    )
    wait_rule_source_syncing_step = WaitUntil(
        cmd(
            'rulesource describe -rsid $.[0].items[0].rule_source_id',
            customer,
        ),
        {
            '$.data.latest_sync.current_status': Equal('SYNCING'),
        },
        depends_on=(rule_update_step,),
        sleep=2,
        timeout=120,
    )
    rule_update_while_syncing_step = Step(
        cmd('rule update -rsid $.[0].items[0].rule_source_id', customer),
        {
            '$.items[0].status': Contains('Rule source is currently being updated. Rule update event has not been submitted'),
            '$.items[0].customer': Equal(customer),
            '$.items[0].git_project_id': Equal(s.pid),
        },
        depends_on=(rule_update_step, wait_rule_source_syncing_step),
    )
    wait_rule_update_step = WaitUntil(
        cmd('rulesource describe -rsid $.[0].items[0].id', customer),
        {
            '$.data.id': NotEmpty(),
            '$.data.customer': Equal(customer),
            '$.data.git_project_id': Equal(s.pid),
            '$.data.latest_sync.current_status': Equal('SYNCED'),
        },
        break_if={'$.message': NotEmpty()},
        depends_on=(rs_resolve_step,),
        sleep=5,
    )
    rule_describe_step = Step(
        cmd(f'rule describe -l 1 -c {s.cloud}', customer),
        {
            '$.next_token': NotEmpty(),
            '$.items': Len(operator.eq, 1),
            '$.items[0].name': NotEmpty(),
            '$.items[0].cloud': Equal(s.cloud),
            '$.items[0].description': NotEmpty(),
            '$.items[0].branch': Equal(s.ref),
            '$.items[0].project': Equal(s.pid),
            '$.items[0].customer': Equal(customer),
        },
        depends_on=(wait_rule_update_step,),
    )
    rule_describe_concrete_step = Step(
        cmd('rule describe -r $.[0].items[0].name', customer),
        {
            '$.data.name': NotEmpty(),
            '$.data.cloud': Equal(s.cloud),
            '$.data.description': NotEmpty(),
            '$.data.branch': Equal(s.ref),
            '$.data.project': Equal(s.pid),
            '$.data.customer': Equal(customer),
        },
        depends_on=(rule_describe_step,),
    )
    rule_delete_concrete_step = Step(
        cmd('rule delete -r $.[0].items[0].name', customer),
        {'$.message': Equal('Request is successful. No content returned')},
        depends_on=(rule_describe_step,),
    )
    rule_describe_concrete_not_found_step = Step(
        cmd('rule describe -r $.[0].items[0].name', customer),
        {'$.items': Empty()},
        depends_on=(rule_describe_step,),
    )

    rule_describe_2_step = Step(
        cmd(f'rule describe -l 2 -c {s.cloud}', customer),
        {
            '$.next_token': NotEmpty(),
            '$.items': Len(operator.eq, 2),
            '$.items[1].name': NotEmpty(),
            '$.items[1].cloud': Equal(s.cloud),
            '$.items[1].description': NotEmpty(),
            '$.items[1].branch': Equal(s.ref),
            '$.items[1].project': Equal(s.pid),
            '$.items[1].customer': Equal(customer),
        },
        depends_on=(wait_rule_update_step,),
    )

    ruleset_expectations = {
        '$.data.customer': Equal(customer),
        '$.data.name': Equal('SMOKE'),
        '$.data.cloud': Equal(s.cloud),
        '$.data.rules_number': IsInstance(int),
        '$.data.license_keys': Empty(),
        '$.data.licensed': False_(),
    }

    ruleset_add_step_1 = Step(
        cmd(
            f'ruleset add -n SMOKE -v 1 -c {s.cloud} -pid {s.pid} '
            f'-gr {s.ref} -d "SMOKE ruleset v1"',
            customer,
        ),
        {
            **ruleset_expectations,
            '$.data.version': Contains('1.0'),
        },
        depends_on=(wait_rule_update_step,),
    )
    ruleset_add_step_2 = Step(
        cmd(
            f'ruleset add -n SMOKE -v 2 -c {s.cloud} -d "SMOKE ruleset v2"',
            customer,
        ),
        {
            **ruleset_expectations,
            '$.data.version': Contains('2.0'),
        },
        depends_on=(wait_rule_update_step,),
    )
    ruleset_add_step_3 = Step(
        cmd(
            f'ruleset add -n SMOKE -v 3 -c {s.cloud} '
            f'--category logging -d "SMOKE ruleset v3"',
            customer,
        ),
        {
            **ruleset_expectations,
            '$.data.version': Contains('3.0'),
        },
        depends_on=(wait_rule_update_step,),
    )
    ruleset_add_step_4 = Step(
        cmd(
            f'ruleset add -n SMOKE -v 4 -c {s.cloud} '
            f'--service_section Compute -d "SMOKE ruleset v4"',
            customer,
        ),
        {
            **ruleset_expectations,
            '$.data.version': Contains('4.0'),
        },
        depends_on=(wait_rule_update_step,),
    )
    ruleset_add_step_5 = Step(
        cmd(
            f'ruleset add -n SMOKE -v 5 -c {s.cloud} '
            f'--source epam -d "SMOKE ruleset v5"',
            customer,
        ),
        {
            **ruleset_expectations,
            '$.data.version': Contains('5.0'),
        },
        depends_on=(wait_rule_update_step,),
    )
    ruleset_add_the_same_step = Step(
        cmd(
            f'ruleset add -n SMOKE -v 5 -c {s.cloud} -d "SMOKE ruleset v5"',
            customer,
        ),
        {
            '$.message': Contains('already exists'),
        },
        depends_on=(ruleset_add_step_5,),
    )
    ruleset_add_invalid_step = Step(
        cmd(
            f'ruleset add -n SMOKE -v 6 -c {s.cloud} '
            f'--service_section invalid -d "SMOKE ruleset v6"',
            customer,
        ),
        {
            '$.errors[0].description': Contains(
                'not available service sections'
            ),
        },
    )
    ruleset_delete_step_1 = Step(
        cmd('ruleset delete -n SMOKE -v 1', customer),
        {
            '$.message': Equal('Request is successful. No content returned'),
        },
    )
    ruleset_delete_step_2 = Step(
        cmd('ruleset delete -n SMOKE -v 2', customer),
        {
            '$.message': Equal('Request is successful. No content returned'),
        },
    )
    ruleset_delete_step_3 = Step(
        cmd('ruleset delete -n SMOKE -v 3', customer),
        {
            '$.message': Equal('Request is successful. No content returned'),
        },
    )
    ruleset_delete_step_4 = Step(
        cmd('ruleset delete -n SMOKE -v 4', customer),
        {
            '$.message': Equal('Request is successful. No content returned'),
        },
    )
    ruleset_delete_step_5 = Step(
        cmd('ruleset delete -n SMOKE -v 5', customer),
        {
            '$.message': Equal('Request is successful. No content returned'),
        },
    )
    rule_describe_empty = Step(
        cmd(f'rule describe -c {s.cloud}', customer),
        {
            '$.items': IsInstance(list),
        },
        depends_on=(rs_delete_step,),
    )

    return Case(
        steps=(
            rs_add_step,
            rs_resolve_step,
            rule_update_step,
            wait_rule_source_syncing_step,
            rule_update_while_syncing_step,
            wait_rule_update_step,
            rule_describe_step,
            rule_describe_concrete_step,
            rule_delete_concrete_step,
            rule_describe_concrete_not_found_step,
            rule_describe_2_step,
            ruleset_add_step_1,
            ruleset_add_step_2,
            ruleset_add_step_3,
            ruleset_add_step_4,
            ruleset_add_step_5,
            ruleset_add_invalid_step,
            ruleset_add_the_same_step,
            ruleset_delete_step_1,
            ruleset_delete_step_2,
            ruleset_delete_step_3,
            ruleset_delete_step_4,
            ruleset_delete_step_5,
            rs_delete_step,
            rule_describe_empty,
        ),
        name=f'Rules management for {source.cloud}',
    )


def run_rules_management(
    *,
    report_name: str = 'smoke-rules-management.md',
) -> list[Case]:
    settings = get_settings()
    if not all(
        (
            settings.username,
            settings.password,
            settings.customer,
            settings.api_link,
        )
    ):
        raise RuntimeError(
            'SMOKE_SRE_USERNAME, SMOKE_SRE_PASSWORD, SMOKE_SRE_CUSTOMER and '
            'SMOKE_SRE_API_LINK must be provided'
        )

    cases: list[Case] = [build_authentication_case(settings=settings)]
    for cloud in ('AWS', 'AZURE', 'GCP'):
        source = get_rule_source(cloud)
        if source:
            cases.append(
                case_for_source(
                    settings=settings,
                    source=source,
                )
            )

    for case in cases:
        case.execute()
    write_cases(
        cases,
        name=report_name,
    )
    return cases
