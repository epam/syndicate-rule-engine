from helpers.constants import Severity
from services.k8s_workloads import (
    WorkloadFinding,
    build_summary,
    build_tables,
    pod_owner,
    rule_matches,
)
from services.metadata import Metadata, RuleMetadata

RULE = 'ecc-k8s-066-privileged_containers'
HOST_RULE = 'ecc-k8s-070-host_network'


def _pod(name, namespace='team', owners=None, matched=None, rule=RULE):
    data = {'metadata': {'name': name, 'namespace': namespace}}
    if owners is not None:
        data['metadata']['owner_references'] = owners
    if matched is not None:
        data['c7n:MatchedFilters'] = matched
    return WorkloadFinding(
        rule=rule, name=name, namespace=namespace, data=data
    )


def test_pod_owner_prefers_controller():
    pod = {
        'metadata': {
            'ownerReferences': [
                {'kind': 'Node', 'name': 'n1'},
                {'kind': 'ReplicaSet', 'name': 'web-5d', 'controller': True},
            ]
        }
    }
    assert pod_owner(pod) == ('ReplicaSet', 'web-5d')


def test_pod_owner_first_and_missing():
    pod = {'metadata': {'owner_references': [{'kind': 'Job', 'name': 'j'}]}}
    assert pod_owner(pod) == ('Job', 'j')
    assert pod_owner({'metadata': {}}) is None


def test_rule_matches():
    assert rule_matches(RULE, None)
    assert rule_matches(RULE, 'ECC-K8S-066')
    assert rule_matches(RULE, RULE)
    assert not rule_matches(RULE, 'ecc-k8s-06')


def test_build_tables_detailed_and_summary():
    rs = [{'kind': 'ReplicaSet', 'name': 'web-5d', 'controller': True}]
    findings = [
        _pod('web-5d-a', owners=rs, matched=['spec.containers[*]']),
        _pod('web-5d-b', owners=rs, matched=['spec.containers[*]']),
        _pod('web-5d-b', owners=rs, matched=['spec.containers[*]']),
        _pod('lonely', namespace='default'),
        _pod('web-5d-c', owners=rs, rule=HOST_RULE),
    ]
    rule_meta = RuleMetadata(
        source='',
        category='',
        service_section='',
        service='',
        article='',
        impact='',
        remediation='',
        cloud='KUBERNETES',
        severity=Severity.HIGH,
    )
    table, host_table = build_tables(
        findings,
        Metadata(rules={RULE: rule_meta}),
        {RULE: 'Privileged containers'},
    )
    assert table.short_rule_id == 'ecc-k8s-066'
    assert table.title == 'Privileged containers'
    assert table.severity == Severity.HIGH
    assert [p.pod_name for p in table.pods] == [
        'lonely',
        'web-5d-a',
        'web-5d-b',
    ]
    lonely = table.pods[0]
    assert (lonely.owner_kind, lonely.owner_name) == ('Pod', 'lonely')
    assert table.pods[1].matched_filters == ('spec.containers[*]',)

    summary = [
        (w.owner_name, w.owner_kind, w.namespace, w.violations)
        for w in build_summary([table, host_table])
    ]
    assert summary == [
        ('lonely', 'Pod', 'default', ('privileged_containers',)),
        (
            'web-5d',
            'ReplicaSet',
            'team',
            ('privileged_containers', 'host_network'),
        ),
    ]
