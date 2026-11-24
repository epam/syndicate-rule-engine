"""
Kubernetes workloads rule-level reports.

Groups violated pods (findings of ``k8s.pod`` rules) by rule and builds two
kinds of tables for each rule:

- detailed: one row per pod with its namespace, immediate owner and the
  filters that matched (``c7n:MatchedFilters``);
- summary: one row per unique workload (immediate owner) with the number
  of its violated pods.

The owner is the immediate controller from ``metadata.ownerReferences``
(ReplicaSet for Deployment pods, Job for CronJob pods). A pod without
owner references is its own workload.

The module is pure (no DB/S3 access).
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from helpers.constants import Severity
from helpers.k8s_rbac import get_any
from services.k8s_rbac import _short_rule_id
from services.metadata import Metadata

POD = 'k8s.pod'
KIND_POD = 'Pod'
MATCHED_FILTERS_KEY = 'c7n:MatchedFilters'


@dataclass(frozen=True)
class WorkloadFinding:
    rule: str
    name: str
    namespace: str
    data: Mapping


@dataclass(frozen=True)
class PodRow:
    pod_name: str
    namespace: str
    owner_kind: str
    owner_name: str
    matched_filters: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkloadRow:
    owner_name: str
    owner_kind: str
    namespace: str
    pods: int


@dataclass
class WorkloadTable:
    rule: str
    title: str
    severity: Severity = Severity.UNKNOWN
    pods: list[PodRow] = field(default_factory=list)

    @property
    def short_rule_id(self) -> str:
        return _short_rule_id(self.rule)

    @property
    def workloads(self) -> list[WorkloadRow]:
        counts: dict[tuple[str, str, str], int] = {}
        for pod in self.pods:
            key = (pod.owner_name, pod.owner_kind, pod.namespace)
            counts[key] = counts.get(key, 0) + 1
        rows = [
            WorkloadRow(
                owner_name=name, owner_kind=kind, namespace=ns, pods=count
            )
            for (name, kind, ns), count in counts.items()
        ]
        rows.sort(key=lambda r: (r.namespace, r.owner_kind, r.owner_name))
        return rows


def pod_owner(pod: Mapping) -> tuple[str, str] | None:
    """
    Returns (kind, name) of the pod's immediate controller. Prefers the
    owner reference with controller=true and falls back to the first one
    """
    meta = pod.get('metadata') or {}
    refs = get_any(meta, 'owner_references', 'ownerReferences')
    if not refs:
        return None
    ref = next((r for r in refs if r.get('controller')), refs[0])
    return ref.get('kind') or '', ref.get('name') or ''


def matched_filters(pod: Mapping) -> tuple[str, ...]:
    value = pod.get(MATCHED_FILTERS_KEY)
    if not value:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(dict.fromkeys(str(item) for item in value))


def pod_row(finding: WorkloadFinding) -> PodRow:
    owner = pod_owner(finding.data)
    if owner:
        kind, name = owner
    else:
        kind, name = KIND_POD, finding.name
    return PodRow(
        pod_name=finding.name,
        namespace=finding.namespace or '',
        owner_kind=kind,
        owner_name=name,
        matched_filters=matched_filters(finding.data),
    )


def rule_matches(rule: str, query: str | None) -> bool:
    """
    The query matches either the full rule name or its short id
    (ecc-k8s-066), case-insensitive
    """
    if not query:
        return True
    query = query.strip().lower()
    return query in (rule.lower(), _short_rule_id(rule).lower())


def build_tables(
    findings: Iterable[WorkloadFinding],
    metadata: Metadata,
    descriptions: Mapping[str, str],
) -> list[WorkloadTable]:
    """
    One table per rule. Identical pod rows are merged. Tables are sorted
    by rule short id
    """
    tables: dict[str, WorkloadTable] = {}
    for finding in findings:
        table = tables.get(finding.rule)
        if table is None:
            table = tables[finding.rule] = WorkloadTable(
                rule=finding.rule,
                title=descriptions.get(finding.rule) or finding.rule,
                severity=metadata.rule(finding.rule).severity,
            )
        table.pods.append(pod_row(finding))

    result = []
    for table in tables.values():
        table.pods = sorted(
            dict.fromkeys(table.pods),
            key=lambda r: (
                r.namespace,
                r.owner_kind,
                r.owner_name,
                r.pod_name,
            ),
        )
        result.append(table)
    result.sort(key=lambda t: (t.short_rule_id.lower(), t.rule))
    return result
