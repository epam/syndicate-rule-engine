"""
Pure helpers for Kubernetes RBAC objects. They work with dicts produced by
kubernetes client ``to_dict()`` (snake_case) but also accept camelCase keys
so that raw API/YAML dicts can be used.
"""

from collections.abc import Iterable, Mapping
from typing import Any

AGGREGATED_INTO_KEY = 'c7n:aggregated-into'
AGGREGATED_FROM_KEY = 'c7n:aggregated-from'


def get_any(obj: Mapping | None, *keys: str, default: Any = None) -> Any:
    """
    Returns the value of the first key that is present (and not None) in obj
    """
    if not obj:
        return default
    for key in keys:
        value = obj.get(key)
        if value is not None:
            return value
    return default


def match_label_selector(
    labels: Mapping[str, str] | None, selector: Mapping[str, Any] | None
) -> bool:
    """
    Kubernetes LabelSelector semantics. matchLabels and matchExpressions
    are ANDed. An empty selector ({}) matches everything; a None selector
    matches nothing
    """
    if selector is None:
        return False
    labels = labels or {}
    match_labels = get_any(selector, 'match_labels', 'matchLabels') or {}
    for key, value in match_labels.items():
        if labels.get(key) != value:
            return False
    expressions = (
        get_any(selector, 'match_expressions', 'matchExpressions') or []
    )
    for expression in expressions:
        key = expression.get('key')
        operator = expression.get('operator')
        values = expression.get('values') or []
        match operator:
            case 'In':
                if key not in labels or labels[key] not in values:
                    return False
            case 'NotIn':
                if key in labels and labels[key] in values:
                    return False
            case 'Exists':
                if key not in labels:
                    return False
            case 'DoesNotExist':
                if key in labels:
                    return False
            case _:
                return False
    return True


def cluster_role_selectors(role: Mapping) -> list[Mapping] | None:
    """
    Returns clusterRoleSelectors of the aggregationRule or None if the
    cluster role is not an aggregated one
    """
    rule = get_any(role, 'aggregation_rule', 'aggregationRule')
    if not rule:
        return None
    return (
        get_any(rule, 'cluster_role_selectors', 'clusterRoleSelectors') or []
    )


def role_name(role: Mapping) -> str:
    return (role.get('metadata') or {}).get('name') or ''


def role_labels(role: Mapping) -> Mapping[str, str]:
    return (role.get('metadata') or {}).get('labels') or {}


def compute_aggregation(
    cluster_roles: Iterable[Mapping],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Resolves aggregationRule relations between the given cluster roles.
    Returns two maps:
    - aggregated_into: {role name: [aggregating cluster roles that select it]}
    - aggregated_from: {aggregating role name: [cluster roles it selects]}
    """
    roles = list(cluster_roles)
    aggregated_into: dict[str, list[str]] = {}
    aggregated_from: dict[str, list[str]] = {}
    for aggregator in roles:
        selectors = cluster_role_selectors(aggregator)
        if selectors is None:
            continue
        aggregator_name = role_name(aggregator)
        selected = aggregated_from.setdefault(aggregator_name, [])
        for role in roles:
            name = role_name(role)
            if name == aggregator_name:
                continue
            labels = role_labels(role)
            if any(match_label_selector(labels, s) for s in selectors):
                selected.append(name)
                aggregated_into.setdefault(name, []).append(aggregator_name)
    for names in (*aggregated_into.values(), *aggregated_from.values()):
        names.sort()
    return aggregated_into, aggregated_from


def annotate_aggregation(cluster_roles: list[dict]) -> list[dict]:
    """
    Adds AGGREGATED_INTO_KEY and AGGREGATED_FROM_KEY annotations to the
    given cluster roles in place. Returns the same list
    """
    into, from_ = compute_aggregation(cluster_roles)
    for role in cluster_roles:
        name = role_name(role)
        role[AGGREGATED_INTO_KEY] = into.get(name, [])
        if name in from_:
            role[AGGREGATED_FROM_KEY] = from_[name]
    return cluster_roles
