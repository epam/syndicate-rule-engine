from __future__ import annotations

from redis import Redis

from .base import Storage


class RedisStorage(Storage):
    def __init__(
        self,
        host: str,
        port: int,
        password: str | None = None,
    ) -> None:
        self._client = Redis(host=host, port=port, password=password)

    def set(self, key: str, value: str, ttl: int) -> None:
        self._client.set(key, value, ex=ttl)

    def has(self, key: str) -> bool:
        return bool(self._client.exists(key))
