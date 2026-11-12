"""
Kubernetes RBAC "WHO can do WHAT" resolution.

Builds a graph of roles, bindings, subjects and pods from the platform
inventory (SREResources) and expands RBAC findings (violated Roles,
ClusterRoles and their bindings) into rows: one row per
{subject} x {role rule}. Rows are grouped into tables by rule: a Role rule
and its ClusterRole counterpart share one table.

The module is pure (no DB/S3 access) so it can be reused by other
RBAC reports.
"""

import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from helpers.constants import Severity
from helpers.k8s_rbac import (
    compute_aggregation,
    get_any,
    role_name,
)
from services.metadata import Metadata

ROLE = 'k8s.role'
CLUSTER_ROLE = 'k8s.cluster-role'
ROLE_BINDING = 'k8s.role-binding'
CLUSTER_ROLE_BINDING = 'k8s.cluster-role-binding'
POD = 'k8s.pod'
REPLICA_SET = 'k8s.replica-set'

ROLE_TYPES = frozenset((ROLE, CLUSTER_ROLE))
BINDING_TYPES = frozenset((ROLE_BINDING, CLUSTER_ROLE_BINDING))
FINDING_TYPES = ROLE_TYPES | BINDING_TYPES

KIND_ROLE = 'Role'
KIND_CLUSTER_ROLE = 'ClusterRole'
KIND_ROLE_BINDING = 'RoleBinding'
KIND_CLUSTER_ROLE_BINDING = 'ClusterRoleBinding'
KIND_SERVICE_ACCOUNT = 'ServiceAccount'

CLUSTER_WIDE = 'cluster-wide'
DEFAULT_SERVICE_ACCOUNT = 'default'
CORE_API_GROUP = 'core'


def _tuple(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(v) for v in value)


def _metadata(obj: Mapping | None) -> Mapping:
    return (obj or {}).get('metadata') or {}


# role rules


@dataclass(frozen=True)
class RoleRule:
    api_groups: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()
    resource_names: tuple[str, ...] = ()
    non_resource_urls: tuple[str, ...] = ()
    verbs: tuple[str, ...] = ()

    def resource_types(self) -> tuple[str, ...]:
        """
        Human-readable list of what the rule grants access to. A wildcard
        resource is shown together with its api groups (``apps/*``, ``*/*``)
        so it's clear which group it affects. Non-resource URLs are appended
        as is (they start with "/")
        """
        items: list[str] = []
        for resource in self.resources:
            if resource == '*':
                for group in self.api_groups or ('',):
                    items.append(f'{group or CORE_API_GROUP}/*')
            else:
                items.append(resource)
        items.extend(self.non_resource_urls)
        return tuple(dict.fromkeys(items))


def normalize_role_rule(rule: Mapping) -> RoleRule:
    """
    Accepts a policy rule in any of the known spellings: kubernetes client
    ``to_dict()`` (snake_case; nonResourceURLs is ``non_resource_urls`` in
    kubernetes>=36 and ``non_resource_ur_ls`` in older versions) or raw API
    camelCase
    """
    return RoleRule(
        api_groups=_tuple(get_any(rule, 'api_groups', 'apiGroups')),
        resources=_tuple(rule.get('resources')),
        resource_names=_tuple(
            get_any(rule, 'resource_names', 'resourceNames')
        ),
        non_resource_urls=_tuple(
            get_any(
                rule,
                'non_resource_urls',
                'non_resource_ur_ls',
                'nonResourceURLs',
            )
        ),
        verbs=_tuple(rule.get('verbs')),
    )


def role_rules(role: Mapping | None) -> list[RoleRule]:
    if not role:
        return []
    return [normalize_role_rule(r) for r in role.get('rules') or () if r]


# graph


@dataclass(frozen=True)
class RoleRef:
    kind: str  # Role | ClusterRole
    name: str
    namespace: str | None = None  # only for Role


@dataclass(frozen=True)
class Subject:
    kind: str  # ServiceAccount | User | Group
    name: str
    namespace: str | None = None


@dataclass(frozen=True)
class Binding:
    kind: str  # RoleBinding | ClusterRoleBinding
    name: str
    namespace: str | None
    role_ref: RoleRef
    subjects: tuple[Subject, ...]

    @property
    def effective_namespace(self) -> str:
        """
        Role + RoleBinding => binding namespace
        ClusterRole + RoleBinding => binding namespace
        ClusterRoleBinding => cluster-wide
        """
        if self.kind == KIND_ROLE_BINDING:
            return self.namespace or ''
        return CLUSTER_WIDE

    @classmethod
    def from_dict(cls, kind: str, data: Mapping) -> Binding | None:
        meta = _metadata(data)
        ref = get_any(data, 'role_ref', 'roleRef') or {}
        ref_name = ref.get('name')
        if not ref_name:
            return None
        namespace = (
            meta.get('namespace') if kind == KIND_ROLE_BINDING else None
        )
        ref_kind = ref.get('kind') or KIND_CLUSTER_ROLE
        role_ref = RoleRef(
            kind=ref_kind,
            name=ref_name,
            namespace=namespace if ref_kind == KIND_ROLE else None,
        )
        subjects = tuple(
            Subject(
                kind=s.get('kind') or '',
                name=s.get('name') or '',
                namespace=s.get('namespace')
                or (
                    namespace
                    if s.get('kind') == KIND_SERVICE_ACCOUNT
                    else None
                ),
            )
            for s in data.get('subjects') or ()
            if s
        )
        return cls(
            kind=kind,
            name=meta.get('name') or '',
            namespace=namespace,
            role_ref=role_ref,
            subjects=subjects,
        )


@dataclass(frozen=True)
class Grant:
    """
    A binding that grants the role. ``aggregated_via`` is set when the
    binding references an aggregating ClusterRole that includes the role
    """

    binding: Binding
    aggregated_via: str | None = None


@dataclass(frozen=True)
class Workloads:
    workloads: tuple[str, ...] = ()  # Deployment/name, DaemonSet/name, ...
    pods: tuple[str, ...] = ()


_NO_WORKLOADS = Workloads()


def _owner(obj: Mapping) -> tuple[str, str] | None:
    refs = get_any(_metadata(obj), 'owner_references', 'ownerReferences')
    if not refs:
        return None
    ref = next((r for r in refs if r.get('controller')), refs[0])
    return ref.get('kind') or '', ref.get('name') or ''


def _pod_service_account(pod: Mapping) -> str:
    spec = pod.get('spec') or {}
    return (
        get_any(
            spec,
            'service_account_name',
            'serviceAccountName',
            'service_account',
            'serviceAccount',
        )
        or DEFAULT_SERVICE_ACCOUNT
    )


def _slim_metadata(obj: Mapping, labels: bool = False) -> dict:
    meta = _metadata(obj)
    result = {'name': meta.get('name'), 'namespace': meta.get('namespace')}
    if labels:
        result['labels'] = meta.get('labels')
    owner = _owner(obj)
    if owner:
        kind, name = owner
        result['owner_references'] = [
            {'kind': kind, 'name': name, 'controller': True}
        ]
    return result


def slim_resource(resource_type: str, item: Mapping) -> dict:
    """
    Keeps only the fields RbacGraph reads. Used to keep RBAC inventory
    snapshots small: pods and replica sets with full specs and statuses
    are by far the largest part of the inventory
    """
    if resource_type in ROLE_TYPES:
        return {
            'metadata': _slim_metadata(item, labels=True),
            'rules': item.get('rules'),
            'aggregation_rule': get_any(
                item, 'aggregation_rule', 'aggregationRule'
            ),
        }
    if resource_type in BINDING_TYPES:
        return {
            'metadata': _slim_metadata(item),
            'role_ref': get_any(item, 'role_ref', 'roleRef'),
            'subjects': item.get('subjects'),
        }
    if resource_type == POD:
        return {
            'metadata': _slim_metadata(item),
            'spec': {'service_account_name': _pod_service_account(item)},
        }
    if resource_type == REPLICA_SET:
        return {'metadata': _slim_metadata(item)}
    return dict(item)


class RbacGraph:
    """
    In-memory index of RBAC related k8s resources of one platform
    """

    def __init__(self) -> None:
        self.cluster_roles: dict[str, Mapping] = {}
        self.roles: dict[tuple[str, str], Mapping] = {}
        self.bindings: dict[tuple[str, str | None, str], Binding] = {}
        self._bindings_by_role: dict[RoleRef, list[Binding]] = {}
        self._aggregated_into: dict[str, list[str]] = {}
        # (namespace, service account) -> [(pod name, controller owner)]
        self._pods_by_sa: dict[
            tuple[str, str], list[tuple[str, tuple[str, str] | None]]
        ] = {}
        self._replica_set_owner: dict[tuple[str, str], tuple[str, str]] = {}
        self._workloads: dict[tuple[str, str], Workloads] = {}

    @classmethod
    def build(cls, resources: Mapping[str, Iterable[Mapping]]) -> RbacGraph:
        """
        :param resources: {resource type: iterable of resource dicts}
        """
        graph = cls()
        for item in resources.get(CLUSTER_ROLE, ()):
            graph.cluster_roles[role_name(item)] = item
        for item in resources.get(ROLE, ()):
            key = (_metadata(item).get('namespace') or '', role_name(item))
            graph.roles[key] = item
        for rt, kind in (
            (ROLE_BINDING, KIND_ROLE_BINDING),
            (CLUSTER_ROLE_BINDING, KIND_CLUSTER_ROLE_BINDING),
        ):
            for item in resources.get(rt, ()):
                graph.add_binding(Binding.from_dict(kind, item))
        for item in resources.get(POD, ()):
            meta = _metadata(item)
            key = (meta.get('namespace') or '', _pod_service_account(item))
            graph._pods_by_sa.setdefault(key, []).append(
                (meta.get('name') or '', _owner(item))
            )
        for item in resources.get(REPLICA_SET, ()):
            owner = _owner(item)
            if owner:
                meta = _metadata(item)
                key = (meta.get('namespace') or '', meta.get('name') or '')
                graph._replica_set_owner[key] = owner
        graph._aggregated_into, _ = compute_aggregation(
            graph.cluster_roles.values()
        )
        return graph

    def add_binding(self, binding: Binding | None) -> None:
        if binding is None:
            return
        key = (binding.kind, binding.namespace, binding.name)
        if key in self.bindings:
            return
        self.bindings[key] = binding
        self._bindings_by_role.setdefault(binding.role_ref, []).append(binding)

    @property
    def has_bindings(self) -> bool:
        return bool(self.bindings)

    def role(self, ref: RoleRef) -> Mapping | None:
        if ref.kind == KIND_CLUSTER_ROLE:
            return self.cluster_roles.get(ref.name)
        return self.roles.get((ref.namespace or '', ref.name))

    def aggregated_into(self, cluster_role: str) -> list[str]:
        return self._aggregated_into.get(cluster_role, [])

    def iter_grants(self, ref: RoleRef) -> Iterator[Grant]:
        """
        Bindings that grant the given role directly or, for cluster roles,
        through aggregating cluster roles (transitively)
        """
        for binding in self._bindings_by_role.get(ref, ()):
            yield Grant(binding)
        if ref.kind != KIND_CLUSTER_ROLE:
            return
        seen = {ref.name}
        stack = list(self.aggregated_into(ref.name))
        while stack:
            aggregator = stack.pop()
            if aggregator in seen:
                continue
            seen.add(aggregator)
            agg_ref = RoleRef(KIND_CLUSTER_ROLE, aggregator)
            for binding in self._bindings_by_role.get(agg_ref, ()):
                yield Grant(binding, aggregated_via=aggregator)
            stack.extend(self.aggregated_into(aggregator))

    def iter_role_refs(self) -> Iterator[RoleRef]:
        for name in self.cluster_roles:
            yield RoleRef(KIND_CLUSTER_ROLE, name)
        for ns, name in self.roles:
            yield RoleRef(KIND_ROLE, name, ns)

    def workloads(self, subject: Subject) -> Workloads:
        """
        Pods (and their top-level workloads) that run under the given
        service account. Empty for users and groups. Memoized: a subject
        appears in many rows
        """
        if subject.kind != KIND_SERVICE_ACCOUNT:
            return _NO_WORKLOADS
        key = (subject.namespace or '', subject.name)
        result = self._workloads.get(key)
        if result is None:
            result = self._workloads[key] = self._resolve_workloads(key)
        return result

    def _resolve_workloads(self, key: tuple[str, str]) -> Workloads:
        pods = self._pods_by_sa.get(key)
        if not pods:
            return _NO_WORKLOADS
        workloads: dict[str, None] = {}
        for name, owner in pods:
            workloads[self._workload_of(key[0], name, owner)] = None
        return Workloads(
            tuple(workloads), tuple(sorted(name for name, _ in pods))
        )

    def _workload_of(
        self, namespace: str, pod_name: str, owner: tuple[str, str] | None
    ) -> str:
        if not owner:
            return f'Pod/{pod_name}'
        kind, name = owner
        if kind == 'ReplicaSet':
            kind, name = self._replica_set_owner.get(
                (namespace, name), (kind, name)
            )
        return f'{kind}/{name}'


# rows & tables


@dataclass(frozen=True)
class RbacRow:
    subject_name: str
    subject_kind: str
    subject_namespace: str
    workloads: tuple[str, ...]
    pods: tuple[str, ...]
    role_kind: str
    role_name: str
    role_namespace: str
    api_groups: tuple[str, ...]
    resource_types: tuple[str, ...]
    resource_names: tuple[str, ...]
    verbs: tuple[str, ...]
    effective_namespace: str
    binding_kind: str
    binding_name: str
    binding_namespace: str
    aggregated_via: str
    bound: bool = True


def build_role_rows(
    graph: RbacGraph,
    ref: RoleRef,
    role: Mapping | None = None,
    only_binding: Binding | None = None,
) -> list[RbacRow]:
    """
    Expands one role into rows {subject} x {role rule}. If the role is not
    bound to anything, rows without subjects are returned (bound=False)
    """
    role = role if role is not None else graph.role(ref)
    rules = [
        (rule, rule.resource_types())
        for rule in role_rules(role) or [RoleRule()]
    ]
    if only_binding is not None:
        grants = [Grant(only_binding)]
    else:
        grants = list(graph.iter_grants(ref))

    def make(
        entry: tuple[RoleRule, tuple[str, ...]],
        grant: Grant | None,
        subject: Subject | None,
    ):
        rule, resource_types = entry
        binding = grant.binding if grant else None
        workloads = graph.workloads(subject) if subject else _NO_WORKLOADS
        if binding:
            effective_namespace = binding.effective_namespace
        elif ref.kind == KIND_ROLE:
            effective_namespace = ref.namespace or ''
        else:
            effective_namespace = CLUSTER_WIDE
        return RbacRow(
            subject_name=subject.name if subject else '',
            subject_kind=subject.kind if subject else '',
            subject_namespace=(subject.namespace or '') if subject else '',
            workloads=workloads.workloads,
            pods=workloads.pods,
            role_kind=ref.kind,
            role_name=ref.name,
            role_namespace=ref.namespace or '',
            api_groups=rule.api_groups,
            resource_types=resource_types,
            resource_names=rule.resource_names,
            verbs=rule.verbs,
            effective_namespace=effective_namespace,
            binding_kind=binding.kind if binding else '',
            binding_name=binding.name if binding else '',
            binding_namespace=(binding.namespace or '') if binding else '',
            aggregated_via=(grant.aggregated_via or '') if grant else '',
            bound=grant is not None,
        )

    if not grants:
        return [make(rule, None, None) for rule in rules]
    rows = []
    for grant in grants:
        subjects: Sequence[Subject | None] = grant.binding.subjects or (None,)
        for subject in subjects:
            for rule in rules:
                rows.append(make(rule, grant, subject))
    return rows


@dataclass(frozen=True)
class RbacFinding:
    rule: str
    resource_type: str
    name: str
    namespace: str | None
    data: Mapping = field(default_factory=dict, compare=False)


def finding_rows(graph: RbacGraph, finding: RbacFinding) -> list[RbacRow]:
    rt = finding.resource_type
    if rt == CLUSTER_ROLE:
        ref = RoleRef(KIND_CLUSTER_ROLE, finding.name)
        return build_role_rows(graph, ref, graph.role(ref) or finding.data)
    if rt == ROLE:
        ref = RoleRef(KIND_ROLE, finding.name, finding.namespace or '')
        return build_role_rows(graph, ref, graph.role(ref) or finding.data)
    if rt in BINDING_TYPES:
        kind = (
            KIND_ROLE_BINDING
            if rt == ROLE_BINDING
            else KIND_CLUSTER_ROLE_BINDING
        )
        ns = finding.namespace if kind == KIND_ROLE_BINDING else None
        binding = graph.bindings.get((kind, ns, finding.name))
        if binding is None:
            binding = Binding.from_dict(kind, finding.data)
        if binding is None:
            return []
        role = graph.role(binding.role_ref) or finding.data.get('c7n:role-ref')
        return build_role_rows(
            graph, binding.role_ref, role, only_binding=binding
        )
    return []


@dataclass
class RbacTable:
    key: str
    title: str
    sheet: str = ''
    rules: list[str] = field(default_factory=list)
    severity: Severity = Severity.UNKNOWN
    rows: list[RbacRow] = field(default_factory=list)

    @property
    def sheet_name(self) -> str:
        return self.sheet or self.title or self.key


_SEVERITY_ORDER = {
    Severity.UNKNOWN: 0,
    Severity.INFO: 1,
    Severity.LOW: 2,
    Severity.MEDIUM: 3,
    Severity.HIGH: 4,
}


_SHORT_RULE_ID = re.compile(r'^(ecc-k8s-\d+)(?:-|$)', re.IGNORECASE)


def _short_rule_id(rule: str) -> str:
    """
    ecc-k8s-001-cluster_role_wildcard -> ecc-k8s-001
    """
    match = _SHORT_RULE_ID.match(rule)
    return match.group(1) if match else rule


def _rule_title(rule: str) -> str:
    """
    ecc-k8s-001-cluster_role_wildcard -> cluster_role_wildcard
    """
    match = _SHORT_RULE_ID.match(rule)
    return rule[match.end() :] if match else rule


def _compact_rule_title(rule: str) -> str:
    """
    ecc-k8s-084-cluster_admin_role_in_cluster_role_bindings ->
    clusteradminroleinclusterrolebindings
    """
    title = _rule_title(rule)
    title = title.replace('_', '')
    title = title.replace('-', '')
    return title


def _is_cluster_role_rule(rule: str) -> bool:
    return 'clusterrole' in _compact_rule_title(rule)


def _rule_pair_key(rule: str) -> str:
    """
    Role and ClusterRole rules of one pair get the same key:
    ...role_wildcard and ...cluster_role_wildcard -> rolewildcard
    """
    return _compact_rule_title(rule).replace('clusterrole', 'role')


def build_tables(
    graph: RbacGraph,
    findings: Iterable[RbacFinding],
    metadata: Metadata,
    descriptions: Mapping[str, str],
) -> list[RbacTable]:
    """
    Groups findings by rule name after normalizing ClusterRole to Role.
    The table title joins descriptions for rules with the same normalized
    name using '/'. A sheet is named by the short IDs of the rules with
    findings in the table. Identical rows inside a table are merged.
    """
    rules_by_pair: dict[str, list[str]] = {}
    for rule in descriptions:
        rules_by_pair.setdefault(_rule_pair_key(rule), []).append(rule)

    tables: dict[str, RbacTable] = {}
    for finding in findings:
        if finding.resource_type not in FINDING_TYPES:
            continue
        rule_meta = metadata.rule(finding.rule)
        pair_key = _rule_pair_key(finding.rule)
        key = f'pair:{pair_key}'
        table = tables.get(key)
        if table is None:
            rule_descriptions = (
                descriptions[rule]
                for rule in rules_by_pair.get(pair_key, ())
                if rule in descriptions
            )
            title = '/'.join(rule_descriptions) or _short_rule_id(finding.rule)
            table = tables[key] = RbacTable(
                key=key,
                title=title,
            )
        short_rule_id = _short_rule_id(finding.rule)
        if short_rule_id not in table.rules:
            table.rules.append(short_rule_id)
        if _SEVERITY_ORDER.get(rule_meta.severity, 0) > _SEVERITY_ORDER.get(
            table.severity, 0
        ):
            table.severity = rule_meta.severity
        table.rows.extend(finding_rows(graph, finding))

    result = []
    for table in tables.values():
        table.rules.sort()
        table.sheet = ', '.join(table.rules)
        table.rows = sorted(
            dict.fromkeys(table.rows),
            key=lambda r: (
                not r.bound,
                r.subject_name,
                r.subject_kind,
                r.role_name,
                r.effective_namespace,
            ),
        )
        result.append(table)
    result.sort(key=lambda table: (table.title.lower(), table.key))
    return result
