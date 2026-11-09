import io

import openpyxl
import pytest
from xlsxwriter import Workbook

from helpers.constants import Severity
from helpers.k8s_rbac import AGGREGATED_INTO_KEY
from services.k8s_rbac import (
    CLUSTER_ROLE,
    CLUSTER_ROLE_BINDING,
    CLUSTER_WIDE,
    POD,
    REPLICA_SET,
    ROLE,
    ROLE_BINDING,
    RbacFinding,
    RbacGraph,
    RoleRef,
    _is_cluster_role_rule,
    _rule_pair_key,
    build_role_rows,
    build_tables,
    normalize_role_rule,
    slim_resource,
)
from services.k8s_rbac_xlsx import (
    COLUMNS,
    MAX_CELL_LENGTH,
    SEPARATOR,
    RbacXlsxWriter,
    _join,
    unique_sheet_name,
)
from services.metadata import Metadata, RuleMetadata


def meta(name, namespace=None, labels=None, owner=None):
    m = {'name': name, 'labels': labels or {}}
    if namespace:
        m['namespace'] = namespace
    if owner:
        m['owner_references'] = [
            {'kind': owner[0], 'name': owner[1], 'controller': True}
        ]
    return m


def cluster_role(name, rules, labels=None, selectors=None):
    r = {'metadata': meta(name, labels=labels), 'rules': rules}
    if selectors:
        r['aggregation_rule'] = {'cluster_role_selectors': selectors}
    return r


def role(name, ns, rules):
    return {'metadata': meta(name, ns), 'rules': rules}


def binding(name, ref_kind, ref_name, subjects, ns=None):
    return {
        'metadata': meta(name, ns),
        'role_ref': {'kind': ref_kind, 'name': ref_name},
        'subjects': subjects,
    }


def pod(name, ns, sa=None, owner=None):
    spec = {'service_account_name': sa} if sa else {}
    return {'metadata': meta(name, ns, owner=owner), 'spec': spec}


SECRETS_RULE = {
    'api_groups': [''],
    'resources': ['secrets'],
    'verbs': ['get', 'list'],
}
WILDCARD_RULE = {'api_groups': ['*'], 'resources': ['*'], 'verbs': ['*']}


@pytest.fixture
def inventory() -> dict:
    return {
        CLUSTER_ROLE: [
            cluster_role(
                'aggregator',
                [],
                selectors=[{'match_labels': {'agg': 'true'}}],
            ),
            cluster_role('secret-reader', [SECRETS_RULE], {'agg': 'true'}),
            cluster_role('super', [WILDCARD_RULE]),
            cluster_role('unused', [SECRETS_RULE]),
        ],
        ROLE: [role('ns-reader', 'team', [SECRETS_RULE])],
        ROLE_BINDING: [
            binding(
                'rb-team',
                'ClusterRole',
                'secret-reader',
                [{'kind': 'ServiceAccount', 'name': 'app'}],
                ns='team',
            ),
            binding(
                'rb-role',
                'Role',
                'ns-reader',
                [{'kind': 'User', 'name': 'alice'}],
                ns='team',
            ),
        ],
        CLUSTER_ROLE_BINDING: [
            binding(
                'crb-agg',
                'ClusterRole',
                'aggregator',
                [{'kind': 'Group', 'name': 'ops'}],
            ),
            binding(
                'crb-super',
                'ClusterRole',
                'super',
                [
                    {
                        'kind': 'ServiceAccount',
                        'name': 'admin',
                        'namespace': 'kube-system',
                    }
                ],
            ),
        ],
        POD: [
            pod('app-7d9-abc', 'team', 'app', ('ReplicaSet', 'app-7d9')),
            pod('app-7d9-def', 'team', 'app', ('ReplicaSet', 'app-7d9')),
            pod('ds-x', 'team', 'app', ('DaemonSet', 'ds')),
            pod('other', 'team'),  # default service account
        ],
        REPLICA_SET: [
            {'metadata': meta('app-7d9', 'team', owner=('Deployment', 'app'))}
        ],
    }


@pytest.fixture
def graph(inventory) -> RbacGraph:
    return RbacGraph.build(inventory)


class TestRoleRule:
    @pytest.mark.parametrize(
        'key', ('non_resource_urls', 'non_resource_ur_ls', 'nonResourceURLs')
    )
    def test_non_resource_urls_spellings(self, key):
        rule = normalize_role_rule({key: ['/metrics'], 'verbs': ['get']})
        assert rule.non_resource_urls == ('/metrics',)
        assert rule.resource_types() == ('/metrics',)

    def test_camel_case(self):
        rule = normalize_role_rule(
            {
                'apiGroups': ['apps'],
                'resources': ['deployments'],
                'resourceNames': ['x'],
                'verbs': ['get'],
            }
        )
        assert rule.api_groups == ('apps',)
        assert rule.resource_names == ('x',)

    def test_wildcard_resource_types(self):
        rule = normalize_role_rule(
            {'api_groups': ['', 'apps'], 'resources': ['*', 'pods']}
        )
        assert rule.resource_types() == ('core/*', 'apps/*', 'pods')
        assert normalize_role_rule(WILDCARD_RULE).resource_types() == ('*/*',)


class TestGraph:
    def test_effective_namespace(self, graph):
        rows = build_role_rows(graph, RoleRef('Role', 'ns-reader', 'team'))
        assert {r.effective_namespace for r in rows} == {'team'}
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'super'))
        assert {r.effective_namespace for r in rows} == {CLUSTER_WIDE}

    def test_cluster_role_with_role_binding(self, graph):
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'secret-reader'))
        direct = [r for r in rows if not r.aggregated_via]
        assert len(direct) == 1
        assert direct[0].effective_namespace == 'team'
        assert direct[0].subject_namespace == 'team'  # inherited from binding

    def test_aggregation_propagates_subjects(self, graph):
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'secret-reader'))
        via = [r for r in rows if r.aggregated_via]
        assert len(via) == 1
        assert via[0].subject_name == 'ops'
        assert via[0].aggregated_via == 'aggregator'
        assert via[0].effective_namespace == CLUSTER_WIDE
        assert via[0].resource_types == ('secrets',)

    def test_ignores_stale_plugin_annotations(self, inventory):
        for item in inventory[CLUSTER_ROLE]:
            item[AGGREGATED_INTO_KEY] = []
        graph = RbacGraph.build(inventory)
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'secret-reader'))
        assert [r.aggregated_via for r in rows if r.aggregated_via] == [
            'aggregator'
        ]

    def test_slim_snapshot_builds_same_rows(self, inventory):
        slim = {
            rt: [slim_resource(rt, item) for item in items]
            for rt, items in inventory.items()
        }
        ref = RoleRef('ClusterRole', 'secret-reader')
        assert build_role_rows(RbacGraph.build(slim), ref) == build_role_rows(
            RbacGraph.build(inventory), ref
        )

    def test_service_account_workloads(self, graph):
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'secret-reader'))
        row = next(r for r in rows if r.subject_name == 'app')
        assert row.workloads == ('Deployment/app', 'DaemonSet/ds')
        assert row.pods == ('app-7d9-abc', 'app-7d9-def', 'ds-x')

    def test_users_have_no_pods(self, graph):
        rows = build_role_rows(graph, RoleRef('Role', 'ns-reader', 'team'))
        assert rows[0].subject_kind == 'User'
        assert rows[0].pods == ()

    def test_unused_role(self, graph):
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'unused'))
        assert len(rows) == 1
        assert not rows[0].bound
        assert rows[0].subject_name == ''
        assert rows[0].verbs == ('get', 'list')

    def test_one_row_per_rule(self, inventory):
        inventory[CLUSTER_ROLE][2]['rules'] = [WILDCARD_RULE, SECRETS_RULE]
        graph = RbacGraph.build(inventory)
        rows = build_role_rows(graph, RoleRef('ClusterRole', 'super'))
        assert len(rows) == 2


def _rule_meta(severity, standard):
    return RuleMetadata(
        source='',
        category='',
        service_section='',
        service='',
        article='',
        impact='',
        remediation='',
        cloud='KUBERNETES',
        severity=severity,
        standard=standard,
    )


CLUSTER_ADMIN_RB_RULE = (
    'ecc-k8s-083-cluster_admin_role_is_only_used_where_required_'
    'in_rolebindings'
)
CLUSTER_ADMIN_CRB_RULE = (
    'ecc-k8s-084-cluster_admin_role_is_only_used_where_required_'
    'in_cluster_role_bindings'
)


class TestTables:
    @pytest.mark.parametrize(
        ('role_rule', 'cluster_rule'),
        (
            ('ecc-k8s-002-role_wildcard', 'ecc-k8s-001-clusterrole_wildcard'),
            ('ecc-k8s-002-role_wildcard', 'ecc-k8s-001-cluster_role_wildcard'),
            (CLUSTER_ADMIN_RB_RULE, CLUSTER_ADMIN_CRB_RULE),
        ),
    )
    def test_rule_pair_key_normalizes_cluster_role(
        self, role_rule, cluster_rule
    ):
        assert _rule_pair_key(cluster_rule) == _rule_pair_key(role_rule)
        assert _is_cluster_role_rule(cluster_rule)
        assert not _is_cluster_role_rule(role_rule)

    def test_cluster_admin_binding_rules_share_table(self, graph):
        role_rule = CLUSTER_ADMIN_RB_RULE
        cluster_rule = CLUSTER_ADMIN_CRB_RULE
        metadata = Metadata(
            rules={
                role_rule: _rule_meta(Severity.HIGH, {}),
                cluster_rule: _rule_meta(Severity.HIGH, {}),
            }
        )
        [table] = build_tables(
            graph,
            [
                RbacFinding(role_rule, ROLE_BINDING, 'rb-team', 'team'),
                RbacFinding(
                    cluster_rule, CLUSTER_ROLE_BINDING, 'crb-super', None
                ),
            ],
            metadata,
            {
                role_rule: 'Cluster-admin is used only where required in '
                'RoleBindings',
                cluster_rule: 'Cluster-admin is used only where required in '
                'ClusterRoleBindings',
            },
        )
        assert table.rules == ['ecc-k8s-083', 'ecc-k8s-084']
        assert table.title.endswith('in RoleBindings')
        assert table.sheet_name == 'ecc-k8s-083, ecc-k8s-084'
        assert {row.binding_name for row in table.rows} == {
            'rb-team',
            'crb-super',
        }

    def test_related_role_rules_share_table(self, graph):
        cluster_rule = 'ecc-k8s-001-cluster_role_wildcard'
        role_rule = 'ecc-k8s-002-role_wildcard'
        metadata = Metadata(
            rules={
                cluster_rule: _rule_meta(Severity.MEDIUM, {}),
                role_rule: _rule_meta(Severity.HIGH, {}),
            }
        )
        findings = [
            RbacFinding(cluster_rule, CLUSTER_ROLE, 'super', None),
            RbacFinding(role_rule, ROLE, 'ns-reader', 'team'),
        ]
        tables = build_tables(
            graph,
            findings,
            metadata,
            {
                cluster_rule: 'Minimize wildcard use in ClusterRoles',
                role_rule: 'Minimize wildcard use in Roles',
            },
        )
        [table] = tables
        assert table.rules == ['ecc-k8s-001', 'ecc-k8s-002']
        assert table.severity == Severity.HIGH
        assert table.title == 'Minimize wildcard use in Roles'
        assert table.sheet_name == 'ecc-k8s-001, ecc-k8s-002'
        assert len(table.rows) == 2

    def test_same_title_without_matching_rule_names_stays_separate(
        self, graph
    ):
        role_a = 'ecc-k8s-001-role_wildcard'
        role_b = 'ecc-k8s-002-role_secrets'
        metadata = Metadata(
            rules={
                role_a: _rule_meta(Severity.MEDIUM, {}),
                role_b: _rule_meta(Severity.LOW, {}),
            }
        )
        tables = build_tables(
            graph,
            [
                RbacFinding(role_a, ROLE, 'ns-reader', 'team'),
                RbacFinding(role_b, ROLE, 'ns-reader', 'team'),
            ],
            metadata,
            {
                role_a: 'Shared title',
                role_b: 'Shared title',
            },
        )
        assert len(tables) == 2
        assert {table.rules[0] for table in tables} == {
            'ecc-k8s-001',
            'ecc-k8s-002',
        }

    def test_pair_title_uses_catalog_even_when_only_one_rule_finds(
        self, graph
    ):
        cluster_rule = 'ecc-k8s-001-cluster_role_wildcard'
        role_rule = 'ecc-k8s-002-role_wildcard'
        metadata = Metadata(
            rules={cluster_rule: _rule_meta(Severity.HIGH, {})}
        )
        [table] = build_tables(
            graph,
            [RbacFinding(cluster_rule, CLUSTER_ROLE, 'super', None)],
            metadata,
            {
                cluster_rule: 'Minimize wildcard use in ClusterRoles',
                role_rule: 'Minimize wildcard use in Roles',
            },
        )
        assert table.rules == ['ecc-k8s-001']
        assert table.title == 'Minimize wildcard use in Roles'
        assert table.sheet_name == 'ecc-k8s-001'

    def test_pair_title_is_role_rule_description(self, graph):
        cluster_rule = 'ecc-k8s-001-cluster_roles_wildcard'
        role_rule = 'ecc-k8s-002-roles_wildcard'
        metadata = Metadata(
            rules={
                cluster_rule: _rule_meta(Severity.HIGH, {}),
                role_rule: _rule_meta(Severity.LOW, {}),
            }
        )
        [table] = build_tables(
            graph,
            [
                RbacFinding(cluster_rule, CLUSTER_ROLE, 'super', None),
                RbacFinding(role_rule, ROLE, 'ns-reader', 'team'),
            ],
            metadata,
            {
                cluster_rule: 'ClusterRoles must not use wildcards',
                role_rule: 'Avoid wildcards in Roles',
            },
        )
        assert table.title == 'Avoid wildcards in Roles'
        assert table.sheet_name == 'ecc-k8s-001, ecc-k8s-002'
        assert table.rules == ['ecc-k8s-001', 'ecc-k8s-002']

    def test_unpaired_cluster_role_rule_uses_own_name(self, graph):
        cluster_rule = 'ecc-k8s-001-cluster_role_wildcard'
        metadata = Metadata(
            rules={cluster_rule: _rule_meta(Severity.HIGH, {})}
        )
        [table] = build_tables(
            graph,
            [RbacFinding(cluster_rule, CLUSTER_ROLE, 'super', None)],
            metadata,
            {cluster_rule: 'Minimize wildcard use in ClusterRoles'},
        )
        assert table.title == 'Minimize wildcard use in ClusterRoles'
        assert table.sheet_name == 'ecc-k8s-001'


class TestXlsx:
    def test_related_rules_use_rule_ids_as_sheet_name(self, graph):
        cluster_rule = 'ecc-k8s-001-cluster_role_wildcard'
        role_rule = 'ecc-k8s-002-role_wildcard'
        metadata = Metadata(
            rules={
                cluster_rule: _rule_meta(Severity.MEDIUM, {}),
                role_rule: _rule_meta(Severity.HIGH, {}),
            }
        )
        tables = build_tables(
            graph,
            [
                RbacFinding(cluster_rule, CLUSTER_ROLE, 'super', None),
                RbacFinding(role_rule, ROLE, 'ns-reader', 'team'),
            ],
            metadata,
            {
                cluster_rule: 'Minimize wildcard use in ClusterRoles',
                role_rule: 'Minimize wildcard use in Roles',
            },
        )
        buffer = io.BytesIO()
        with Workbook(buffer) as wb:
            RbacXlsxWriter(wb).write_tables(tables)
        buffer.seek(0)
        book = openpyxl.load_workbook(buffer)
        assert book.sheetnames == ['ecc-k8s-001, ecc-k8s-002']
        assert (
            book['ecc-k8s-001, ecc-k8s-002']['B1'].value
            == 'Minimize wildcard use in Roles'
        )

    def test_write(self, graph):
        rule = 'ecc-k8s-003-role_secret_access'
        findings = [RbacFinding(rule, CLUSTER_ROLE, 'secret-reader', None)]
        tables = build_tables(
            graph,
            findings,
            Metadata(rules={rule: _rule_meta(Severity.HIGH, {})}),
            {rule: 'A human-readable role description'},
        )
        buffer = io.BytesIO()
        with Workbook(buffer, {'constant_memory': True}) as wb:
            RbacXlsxWriter(wb).write_tables(tables)
        buffer.seek(0)
        book = openpyxl.load_workbook(buffer)
        assert book.sheetnames == ['ecc-k8s-003']
        sheet = book['ecc-k8s-003']
        assert sheet['B1'].value == 'A human-readable role description'
        assert sheet['A2'].value == 'Rule ID'
        assert sheet['B2'].value == 'ecc-k8s-003'
        header = [c.value for c in sheet[5]]
        assert header == [c.header for c in COLUMNS]
        values = [[c.value for c in row] for row in sheet.iter_rows(min_row=6)]
        assert len(values) == 2
        app = next(v for v in values if v[0] == 'app')
        assert app[2] == 'Deployment/app, DaemonSet/ds'
        assert app[5] == '*'  # no resourceNames
        assert app[7] == 'team'
        # detail columns are hidden and grouped
        detail_col = openpyxl.utils.get_column_letter(
            next(i for i, c in enumerate(COLUMNS, 1) if c.detail)
        )
        dim = sheet.column_dimensions[detail_col]
        assert dim.hidden
        assert dim.outline_level == 1

    def test_empty(self):
        buffer = io.BytesIO()
        with Workbook(buffer) as wb:
            RbacXlsxWriter(wb).write_tables([])
        buffer.seek(0)
        assert openpyxl.load_workbook(buffer).sheetnames == ['RBAC']

    def test_join_caps_cell_length(self):
        values = [f'pod-{i:06d}' for i in range(5000)]
        text = _join(values)
        assert len(text) <= MAX_CELL_LENGTH
        kept = text.split(SEPARATOR)[:-1]
        assert kept == values[: len(kept)]
        assert text.endswith(f'… (+{len(values) - len(kept)} more)')
        assert _join(['x' * (MAX_CELL_LENGTH + 1)]) == '… (+1 more)'
        assert _join(['a', 'b']) == 'a, b'

    def test_unique_sheet_name(self):
        used: set[str] = set()
        assert unique_sheet_name('a/b', used) == 'a_b'
        assert unique_sheet_name('A/B', used) == 'A_B (1)'
        long = 'x' * 40
        assert len(unique_sheet_name(long, used)) == 31
        assert len(unique_sheet_name(long, used)) == 31
