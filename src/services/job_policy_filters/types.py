from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

PolicyName: TypeAlias = str
MergeType: TypeAlias = Literal['append']

APPEND_TYPE: MergeType = 'append'


@dataclass(frozen=True, slots=True)
class CustodianFilter:
    """Cloud Custodian ``value``-style filter entries we generate in code."""

    type: str
    key: str
    op: str
    value: str | list[str] | bool | int | float

    def to_dict(self) -> dict[str, Any]:
        return {
            'type': self.type,
            'key': self.key,
            'op': self.op,
            'value': self.value,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CustodianFilter:
        return cls(
            type=str(data['type']),
            key=str(data['key']),
            op=str(data['op']),
            value=data['value'],
        )


@dataclass(frozen=True, slots=True)
class PolicyScanEntry:
    """One Custodian run for a rule: ``query`` scopes the list API; ``filters`` refine results.

    Multiple entries under the same policy name mean multiple scans (e.g. different namespaces).
    """

    query_merge: MergeType
    query: list[dict[str, str]]
    filters_merge: MergeType
    filters: list[CustodianFilter]

    def to_dict(self) -> dict[str, Any]:
        return {
            'query_merge': self.query_merge,
            'query': [dict(q) for q in self.query],
            'filters_merge': self.filters_merge,
            'filters': [f.to_dict() for f in self.filters],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PolicyScanEntry:
        qm = data.get('query_merge', APPEND_TYPE)
        fm = data.get('filters_merge', APPEND_TYPE)
        if qm != APPEND_TYPE:
            raise ValueError(f'Unsupported query merge type: {qm!r}')
        if fm != APPEND_TYPE:
            raise ValueError(f'Unsupported filters merge type: {fm!r}')
        raw_query = data.get('query') or []
        if not isinstance(raw_query, list):
            raise TypeError('"query" must be a list')
        query: list[dict[str, str]] = []
        for item in raw_query:
            if not isinstance(item, dict):
                raise TypeError('Each query item must be a dict')
            query.append({str(k): str(v) for k, v in item.items()})
        raw_filters = data.get('filters') or []
        if not isinstance(raw_filters, list):
            raise TypeError('"filters" must be a list')
        filters: list[CustodianFilter] = []
        for f in raw_filters:
            if not isinstance(f, Mapping):
                raise TypeError('Each filter must be a JSON object')
            filters.append(CustodianFilter.from_dict(f))
        return cls(
            query_merge=APPEND_TYPE,
            query=query,
            filters_merge=APPEND_TYPE,
            filters=filters,
        )


@dataclass(frozen=True, slots=True)
class BundleFilters:
    """Immutable map of policy name → scan entries (S3 / MinIO bundle payload)."""

    _policies: tuple[tuple[PolicyName, tuple[PolicyScanEntry, ...]], ...]

    def __len__(self) -> int:
        return len(self._policies)

    def __bool__(self) -> bool:
        return len(self._policies) > 0

    @classmethod
    def from_policy_map(
        cls,
        policies: Mapping[
            PolicyName, list[PolicyScanEntry] | tuple[PolicyScanEntry, ...]
        ],
    ) -> BundleFilters:
        return cls(
            _policies=tuple(
                (str(name), tuple(entries))
                for name, entries in policies.items()
            )
        )

    def get(self, policy_name: str) -> tuple[PolicyScanEntry, ...] | None:
        for name, entries in self._policies:
            if name == policy_name:
                return entries
        return None

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        """Serialize for JSON / S3 (MinIO)."""
        return {
            name: [e.to_dict() for e in entries]
            for name, entries in self._policies
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BundleFilters:
        """Deserialize JSON from S3 (MinIO)."""
        if not isinstance(data, Mapping):
            raise TypeError('Bundle root must be a JSON object')
        parsed: dict[str, list[PolicyScanEntry]] = {}
        for policy_name, raw_entries in data.items():
            if not isinstance(raw_entries, list):
                raise TypeError(
                    f'Bundle value for {policy_name!r} must be a list'
                )
            rows: list[PolicyScanEntry] = []
            for x in raw_entries:
                if not isinstance(x, Mapping):
                    raise TypeError(
                        f'Each scan entry for {policy_name!r} must be a JSON object'
                    )
                rows.append(PolicyScanEntry.from_dict(x))
            parsed[str(policy_name)] = rows
        return cls.from_policy_map(parsed)
