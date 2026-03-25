"""
Environment-based configuration for event sources consumer.
Env prefix: SRE_EVENT_CONSUMER_
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing_extensions import Self


def _env_int(name: str, default: int, min_val: int = 1, max_val: int = 9999) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return max(min(int(val), max_val), min_val)
    except ValueError:
        return default


PREFIX = "SRE_EVENT_CONSUMER_"


@dataclass(frozen=True)
class QueueConfig:
    """Queue (SQS) related configuration."""

    batch_size: int
    wait_seconds: int
    visibility_timeout: int

    # Defaults (overridden by env)
    DEFAULT_BATCH_SIZE = 10
    DEFAULT_WAIT_SECONDS = 2  # SQS long poll; shorter = faster stop_event check
    DEFAULT_VISIBILITY_TIMEOUT = 60  # seconds message is invisible while processing

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            batch_size=_env_int(
                f"{PREFIX}BATCH_SIZE",
                cls.DEFAULT_BATCH_SIZE,
                1,
                10,
            ),
            wait_seconds=_env_int(
                f"{PREFIX}WAIT_SECONDS",
                cls.DEFAULT_WAIT_SECONDS,
            ),
            visibility_timeout=_env_int(
                f"{PREFIX}VISIBILITY_TIMEOUT",
                cls.DEFAULT_VISIBILITY_TIMEOUT,
            ),
        )


queue = QueueConfig.from_env()

BOTO_CONNECT_TIMEOUT = _env_int(
    name=f"{PREFIX}BOTO_CONNECT_TIMEOUT",
    default=20,
    min_val=1,
    max_val=9999,
)
BOTO_READ_TIMEOUT = _env_int(
    name=f"{PREFIX}BOTO_READ_TIMEOUT",
    default=20,
    min_val=1,
    max_val=9999,
)
PORT = _env_int(
    name=f"{PREFIX}PORT",
    default=8081,
)
CONFIG_RELOAD_INTERVAL = 30
CONFIG_LIMIT = 500
WORKER_STOP_TIMEOUT = 15
SHUTDOWN_TIMEOUT = 10
ERROR_RETRY_SECONDS = 5
# Refresh assume_role credentials before expiry (assume_role TTL ~3600s)
CREDENTIALS_REFRESH_INTERVAL = _env_int(
    name=f"{PREFIX}CREDENTIALS_REFRESH_INTERVAL",
    default=45 * 60,
    min_val=60,
    max_val=3600,
)
