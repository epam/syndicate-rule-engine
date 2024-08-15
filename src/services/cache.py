from typing import Callable, Any

from cachetools import TLRUCache, TTLCache, cachedmethod  # noqa

from helpers.constants import CAASEnv


def _expiration(key: Any, value: Any, now: float) -> float:
    """
    Sets expiration for any item. We can write some logic here to set
    specific ttl for specific type of items
    :param key:
    :param value:
    :param now:
    :return:
    """
    return now + float(CAASEnv.INNER_CACHE_TTL_SECONDS.get())


def factory(maxsize=50, ttu: Callable[[Any, Any, float], float] = _expiration
            ) -> TLRUCache:
    return TLRUCache(maxsize=maxsize, ttu=ttu)
