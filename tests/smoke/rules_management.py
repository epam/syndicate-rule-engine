import dataclasses
import operator
import os
from typing import Optional

from commons import Step, Equal, Case, write_cases, Empty, NotEmpty, WaitUntil, \
    IsInstance, Contains, Len, True_, False_

SMOKE_CAAS_USERNAME = os.getenv('SMOKE_CAAS_USERNAME')
SMOKE_CAAS_PASSWORD = os.getenv('SMOKE_CAAS_PASSWORD')
SMOKE_CAAS_CUSTOMER = os.getenv('SMOKE_CAAS_CUSTOMER')
SMOKE_CAAS_API_LINK = os.getenv(
    'SMOKE_CAAS_API_LINK') or 'http://0.0.0.0:8000/caas'

assert (SMOKE_CAAS_USERNAME and SMOKE_CAAS_PASSWORD and
        SMOKE_CAAS_CUSTOMER and SMOKE_CAAS_API_LINK), \
    'username, password, customer and api link must be provided'


@dataclasses.dataclass(repr=False)
class Source:
    pid: str
    ref: str
    url: str
    prefix: str
    cloud: str
    secret: Optional[str]


def get_source(cloud: str) -> Optional[Source]:
    cloud = cloud.upper()
    assert cloud in ['AWS', 'AZURE', 'GCP']
    secret = os.getenv(f'SMOKE_CAAS_{cloud}_RULE_SOURCE_SECRET')
    pid = os.getenv(f'SMOKE_CAAS_{cloud}_RULE_SOURCE_PID')
    ref = os.getenv(f'SMOKE_CAAS_{cloud}_RULE_SOURCE_REF') or 'main'
    url = os.getenv(
        f'SMOKE_CAAS_{cloud}_RULE_SOURCE_URL') or 'https://api.github.com'
    prefix = os.getenv(f'SMOKE_CAAS_{cloud}_RULE_SOURCE_PREFIX') or 'policies/'
    if not pid:
        return
    return Source(pid=pid, ref=ref, url=url, prefix=prefix, cloud=cloud,
                  secret=secret)


authentication_case = Case(steps=(
    Step(f'c7n configure --api_link {SMOKE_CAAS_API_LINK} --json', {
        '$.message': Equal('Great! The c7n tool api_link has been configured.')
    }),
    Step(f'c7n login -u {SMOKE_CAAS_USERNAME} -p {SMOKE_CAAS_PASSWORD} --json',
         {
             '$.message': Equal(
                 f'Great! The c7n tool access token has been saved.')
         }),
), name='Authentication')


def case_for_source(source: Source) -> Case:
    s = source
    rs_add_step = Step(
        f'c7n rulesource add --git_project_id {s.pid} --git_url {s.url} --git_ref {s.ref} --git_rules_prefix {s.prefix} --description {s.cloud}' + (
            '' if not s.secret else f'--git_access_secret {s.secret}'), {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].git_project_id': Equal(s.pid),
            '$.items[0].git_url': Equal(s.url),
            '$.items[0].git_ref': Equal(s.ref),
            '$.items[0].description': Equal(s.cloud),
            '$.items[0].type': Equal('GITHUB') | Equal('GITLAB')
        })
    rs_delete_step = Step(
        f'c7n rulesource delete --rule_source_id $.[0].items[0].id', {
            '$.message': Equal('Request is successful. No content returned')
        }, depends_on=(rs_add_step,))

    rule_update_step = Step(f'c7n rule update -rsid $.[0].items[0].id', {
        '$.items[0].status': Equal('Rule update event has been submitted'),
        '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
        '$.items[0].git_project_id': Equal(s.pid),
    }, depends_on=(rs_add_step,))
    wait_rule_update_step = WaitUntil(
        'c7n rulesource describe -rsid $.[0].items[0].id', {
            '$.items[0].id': NotEmpty(),
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].git_project_id': Equal(s.pid),
            '$.items[0].latest_sync.current_status': Equal('SYNCED')
        }, break_if={'$.message': NotEmpty()}, depends_on=(rs_add_step,),
        sleep=5
    )
    rule_describe_step = Step(f'c7n rule describe -l 1 -c {s.cloud}', {
        '$.next_token': NotEmpty(),
        '$.items': Len(operator.eq, 1),
        '$.items[0].name': NotEmpty(),
        '$.items[0].cloud': Equal(s.cloud),
        '$.items[0].description': NotEmpty(),
        '$.items[0].branch': Equal(s.ref),
        '$.items[0].project': Equal(s.pid),
        '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER)
    })
    rule_describe_concrete_step = Step(
        'c7n rule describe -r $.[0].items[0].name', {
            '$.items[0].name': NotEmpty(),
            '$.items[0].cloud': Equal(s.cloud),
            '$.items[0].description': NotEmpty(),
            '$.items[0].branch': Equal(s.ref),
            '$.items[0].project': Equal(s.pid),
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER)
        }, depends_on=(rule_describe_step,))
    rule_delete_concrete_step = Step('c7n rule delete -r $.[0].items[0].name',
                                     {
                                         '$.message': Equal(
                                             'Request is successful. No content returned')
                                     }, depends_on=(rule_describe_step,))
    rule_describe_concrete_not_found_step = Step(
        'c7n rule describe -r $.[0].items[0].name', {
            '$.items': Empty()
        }, depends_on=(rule_describe_step,))

    rule_describe_2_step = Step(f'c7n rule describe -l 2 -c {s.cloud}', {
        '$.next_token': NotEmpty(),
        '$.items': Len(operator.eq, 2),
        '$.items[1].name': NotEmpty(),
        '$.items[1].cloud': Equal(s.cloud),
        '$.items[1].description': NotEmpty(),
        '$.items[1].branch': Equal(s.ref),
        '$.items[1].project': Equal(s.pid),
        '$.items[1].customer': Equal(SMOKE_CAAS_CUSTOMER)
    })

    ruleset_add_step_1 = Step(
        f'c7n ruleset add -n SMOKE -v 1 -c {s.cloud} -pid {s.pid} -gr {s.ref} -act',
        {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].name': Equal('SMOKE'),
            '$.items[0].version': Equal('1.0'),
            '$.items[0].cloud': Equal(s.cloud),
            "$.items[0].rules_number": IsInstance(int),
            '$.items[0].event_driven': False_(),
            '$.items[0].active': True_(),
            '$.items[0].code': Equal('READY_TO_SCAN'),
            '$.items[0].license_keys': Empty(),
            '$.items[0].license_manager_id': Empty(),
            '$.items[0].licensed': False_()
        })
    ruleset_add_step_2 = Step(
        f'c7n ruleset add -n SMOKE -v 2 -c {s.cloud} -act', {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].name': Equal('SMOKE'),
            '$.items[0].version': Equal('2.0'),
            '$.items[0].cloud': Equal(s.cloud),
            "$.items[0].rules_number": IsInstance(int),
            '$.items[0].event_driven': False_(),
            '$.items[0].active': True_(),
            '$.items[0].code': Equal('READY_TO_SCAN'),
            '$.items[0].license_keys': Empty(),
            '$.items[0].license_manager_id': Empty(),
            '$.items[0].licensed': False_()
        })
    ruleset_add_step_3 = Step(
        f'c7n ruleset add -n SMOKE -v 3 -c {s.cloud} -act --standard HIPAA', {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].name': Equal('SMOKE'),
            '$.items[0].version': Equal('3.0'),
            '$.items[0].cloud': Equal(s.cloud),
            "$.items[0].rules_number": IsInstance(int),
            '$.items[0].event_driven': False_(),
            '$.items[0].active': True_(),
            '$.items[0].code': Equal('READY_TO_SCAN'),
            '$.items[0].license_keys': Empty(),
            '$.items[0].license_manager_id': Empty(),
            '$.items[0].licensed': False_()
        })
    ruleset_add_step_4 = Step(
        f'c7n ruleset add -n SMOKE -v 4 -c {s.cloud} -act -ss Compute', {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].name': Equal('SMOKE'),
            '$.items[0].version': Equal('4.0'),
            '$.items[0].cloud': Equal(s.cloud),
            "$.items[0].rules_number": IsInstance(int),
            '$.items[0].event_driven': False_(),
            '$.items[0].active': True_(),
            '$.items[0].code': Equal('READY_TO_SCAN'),
            '$.items[0].license_keys': Empty(),
            '$.items[0].license_manager_id': Empty(),
            '$.items[0].licensed': False_()
        })
    ruleset_add_step_5 = Step(
        f'c7n ruleset add -n SMOKE -v 5 -c {s.cloud} -act -s High', {
            '$.items[0].customer': Equal(SMOKE_CAAS_CUSTOMER),
            '$.items[0].name': Equal('SMOKE'),
            '$.items[0].version': Equal('5.0'),
            '$.items[0].cloud': Equal(s.cloud),
            "$.items[0].rules_number": IsInstance(int),
            '$.items[0].event_driven': False_(),
            '$.items[0].active': True_(),
            '$.items[0].code': Equal('READY_TO_SCAN'),
            '$.items[0].license_keys': Empty(),
            '$.items[0].license_manager_id': Empty(),
            '$.items[0].licensed': False_()
        })
    ruleset_add_the_same_step = Step(
        f'c7n ruleset add -n SMOKE -v 5 -c {s.cloud} -act', {
            '$.message': Equal(
                f'The ruleset \'SMOKE\' version \'5.0\' for in the customer \'{SMOKE_CAAS_CUSTOMER}\' already exists')
        })
    ruleset_add_invalid_step = Step(
        f'c7n ruleset add -n SMOKE -v 6 -c {s.cloud} -act -ss invalid', {
            '$.message': Contains(
                f'Not available service section. Choose from:')
        })
    ruleset_delete_step_1 = Step('c7n ruleset delete -n SMOKE -v 1', {
        '$.message': Equal('Request is successful. No content returned')
    })
    ruleset_delete_step_2 = Step('c7n ruleset delete -n SMOKE -v 2', {
        '$.message': Equal('Request is successful. No content returned')
    })
    ruleset_delete_step_3 = Step('c7n ruleset delete -n SMOKE -v 3', {
        '$.message': Equal('Request is successful. No content returned')
    })
    ruleset_delete_step_4 = Step('c7n ruleset delete -n SMOKE -v 4', {
        '$.message': Equal('Request is successful. No content returned')
    })
    ruleset_delete_step_5 = Step('c7n ruleset delete -n SMOKE -v 5', {
        '$.message': Equal('Request is successful. No content returned')
    })
    rule_describe_empty = Step(f'c7n rule describe -c {s.cloud}', {
        '$.items': Empty()
    })

    return Case(steps=(
        rs_add_step,
        rule_update_step,
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
    ), name=f'Rules management for {source.cloud}')


if __name__ == '__main__':
    aws_source = get_source('AWS')
    azure_source = get_source('AZURE')
    google_source = get_source('GCP')
    cases = [
        authentication_case,
    ]
    if aws_source:
        cases.append(case_for_source(aws_source))
    if azure_source:
        cases.append(case_for_source(azure_source))
    if google_source:
        cases.append(case_for_source(google_source))

    for case in cases:
        case.execute()
    write_cases(cases, name='smoke-rules-management.md')
