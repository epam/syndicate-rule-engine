import pytest

from services.k8s_rbac import (
    CLUSTER_ROLE,
    CLUSTER_ROLE_BINDING,
    ROLE,
    ROLE_BINDING,
    RbacGraph,
    build_summary_rows,
)


def _role(name, rules, namespace=None):
    metadata = {'name': name}
    if namespace:
        metadata['namespace'] = namespace
    return {'metadata': metadata, 'rules': rules}


def _binding(name, role_kind, role_name, subjects, namespace=None):
    metadata = {'name': name}
    if namespace:
        metadata['namespace'] = namespace
    return {
        'metadata': metadata,
        'role_ref': {'kind': role_kind, 'name': role_name},
        'subjects': subjects,
    }


def test_build_summary_rows_aggregates_access_scopes_and_roles():
    graph = RbacGraph.build(
        {
            ROLE: [
                _role(
                    'writer',
                    [{'resources': ['pods'], 'verbs': ['patch']}],
                    'prod',
                ),
                _role(
                    'secret-reader',
                    [{'resources': ['secrets'], 'verbs': ['get']}],
                    'prod',
                ),
                _role(
                    'pod-creator',
                    [{'resources': ['pods'], 'verbs': ['create']}],
                    'prod',
                ),
                _role('empty', [], 'dev'),
            ],
            CLUSTER_ROLE: [
                _role(
                    'reader',
                    [
                        {
                            'resources': ['pods'],
                            'verbs': ['get', 'list', 'watch'],
                        }
                    ],
                ),
                _role('cluster-admin', [{'resources': ['*'], 'verbs': ['*']}]),
            ],
            ROLE_BINDING: [
                _binding(
                    'reader-binding',
                    'ClusterRole',
                    'reader',
                    [{'kind': 'User', 'name': 'alice'}],
                    'dev',
                ),
                _binding(
                    'viewer-binding',
                    'ClusterRole',
                    'reader',
                    [{'kind': 'User', 'name': 'viewers'}],
                    'qa',
                ),
                _binding(
                    'writer-binding',
                    'Role',
                    'writer',
                    [{'kind': 'User', 'name': 'alice'}],
                    'prod',
                ),
                _binding(
                    'secret-binding',
                    'Role',
                    'secret-reader',
                    [{'kind': 'Group', 'name': 'deployers'}],
                    'prod',
                ),
                _binding(
                    'pod-binding',
                    'Role',
                    'pod-creator',
                    [{'kind': 'Group', 'name': 'deployers'}],
                    'prod',
                ),
                _binding(
                    'empty-binding',
                    'Role',
                    'empty',
                    [{'kind': 'ServiceAccount', 'name': 'agent'}],
                    'dev',
                ),
            ],
            CLUSTER_ROLE_BINDING: [
                _binding(
                    'admin-binding',
                    'ClusterRole',
                    'cluster-admin',
                    [{'kind': 'Group', 'name': 'admins'}],
                )
            ],
        }
    )

    rows = {
        (row.subject_kind, row.subject_name): row
        for row in build_summary_rows(graph)
    }

    alice = rows[('User', 'alice')]
    assert alice.access == '✏️'
    assert alice.scope == 'dev, prod'
    assert alice.roles == ('prod/writer', 'reader')

    viewers = rows[('User', 'viewers')]
    assert viewers.access == '👁️'

    deployers = rows[('Group', 'deployers')]
    assert deployers.access == '🔥'
    assert deployers.scope == 'prod'

    admins = rows[('Group', 'admins')]
    assert admins.access == '🔥'
    assert admins.scope == 'cluster-wide'

    service_account = rows[('SA', 'dev/agent')]
    assert service_account.access == 'None'
    assert service_account.scope == 'dev'
    assert service_account.roles == ('dev/empty',)


@pytest.mark.parametrize(
    ('resources', 'verbs'),
    (
        (['*'], ['get']),
        (['rolebindings'], ['create']),
        (['clusterrolebindings'], ['update']),
        (['pods/exec'], ['get']),
        (['nodes/proxy'], ['get']),
        (['users'], ['impersonate']),
        (['roles'], ['bind']),
        (['clusterroles'], ['escalate']),
    ),
)
def test_admin_access_indicators(resources, verbs):
    graph = RbacGraph.build(
        {
            ROLE: [
                _role(
                    'permissions',
                    [{'resources': resources, 'verbs': verbs}],
                    'ns',
                )
            ],
            ROLE_BINDING: [
                _binding(
                    'binding',
                    'Role',
                    'permissions',
                    [{'kind': 'User', 'name': 'auditor'}],
                    'ns',
                )
            ],
        }
    )

    [row] = build_summary_rows(graph)

    assert row.access == '🔥'
