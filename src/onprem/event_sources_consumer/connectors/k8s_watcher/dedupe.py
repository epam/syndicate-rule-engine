from __future__ import annotations

from .storage import Storage


# 12 hours
_DEDUPE_TTL_SECONDS = 12 * 60 * 60


class EventUidDeduper:
    """In-process dedupe of watch notifications by (uid, resourceVersion)."""

    __slots__ = ('_storage',)

    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    def is_duplicate(self, uid: str, resource_version: str) -> bool:
        key = f'k8s:watch:dedupe:{uid}:{resource_version}'
        if self._storage.has(key):
            return True
        self._storage.set(key, '1', ttl=_DEDUPE_TTL_SECONDS)
        return False
