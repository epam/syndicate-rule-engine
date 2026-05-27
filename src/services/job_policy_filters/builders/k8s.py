from ..types import CustodianFilter
from .base import PolicyFiltersBuilder, PolicyQueryBuilder


def _normalize_uids(resource_uid: str | list[str]) -> list[str]:
    if isinstance(resource_uid, str):
        return [resource_uid] if resource_uid else []
    return [u for u in resource_uid if u]


class ResourceUidFilterBuilder(PolicyFiltersBuilder):
    """
    Narrows the scan to Kubernetes resources with given metadata.uid value(s).

    Filters:
        - type: value
          key: metadata.uid
          op: eq | in
          value: <uid> | [<uid>, ...]

    Args:
        resource_uid: Single UID, or non-empty list of UIDs (e.g. one job, many triggers).
            Empty string or list (or list of empty strings) yields no filters.
    """

    __slots__ = ('_resource_uid',)

    def __init__(self, resource_uid: str | list[str]) -> None:
        self._resource_uid = resource_uid

    def build(self) -> list[CustodianFilter]:
        uids = _normalize_uids(self._resource_uid)
        if not uids:
            return []
        if len(uids) == 1:
            return [
                CustodianFilter(
                    type='value',
                    key='metadata.uid',
                    op='eq',
                    value=uids[0],
                )
            ]
        return [
            CustodianFilter(
                type='value',
                key='metadata.uid',
                op='in',
                value=uids,
            )
        ]


class K8sQueryBuilder(PolicyQueryBuilder):
    """
    Builds a query for Kubernetes resources.
    """

    __slots__ = ('_name', '_namespace')

    def __init__(
        self,
        name: str | None = None,
        namespace: str | None = None,
    ) -> None:
        self._name = name
        self._namespace = namespace

    def build(self) -> list[dict[str, str]]:
        if self._name and self._namespace:
            return [
                {
                    'field_selector': f'metadata.name={self._name},metadata.namespace={self._namespace}'
                }
            ]
        if self._name:
            return [{'field_selector': f'metadata.name={self._name}'}]
        if self._namespace:
            return [
                {'field_selector': f'metadata.namespace={self._namespace}'}
            ]
        return []
