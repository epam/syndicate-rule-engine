"""
Fixed limits and SQS :class:`QueueConfig` helpers for the event sources consumer.
Env keys live in :class:`onprem.event_sources_consumer.settings.EventConsumerEnv`.
"""

from __future__ import annotations


from helpers.constants import EnvEnum


PREFIX = 'SRE_EVENT_CONSUMER'


class EventConsumerEnv(EnvEnum):
    # SQS queue (long poll / visibility)
    BATCH_SIZE = (
        f'{PREFIX}_SQS_BATCH_SIZE',
        (),
        '10',
    )
    WAIT_SECONDS = (
        f'{PREFIX}_SQS_WAIT_SECONDS',
        (),
        '20',
    )
    VISIBILITY_TIMEOUT = (
        f'{PREFIX}_SQS_VISIBILITY_TIMEOUT',
        (),
        '60',
    )

    # boto3 client timeouts (seconds)
    BOTO_CONNECT_TIMEOUT = (
        f'{PREFIX}_BOTO_CONNECT_TIMEOUT',
        (),
        '20',
    )
    BOTO_READ_TIMEOUT = (
        f'{PREFIX}_BOTO_READ_TIMEOUT',
        (),
        '20',
    )

    # HTTP (health server)
    PORT = (
        f'{PREFIX}_PORT',
        (),
        '8081',
    )

    # worker / credentials
    CREDENTIALS_REFRESH_INTERVAL = (
        f'{PREFIX}_CREDENTIALS_REFRESH_INTERVAL',
        (),
        str(45 * 60),
    )

    # K8s watch connector batching
    K8S_BATCH_MAX_SIZE = (
        f'{PREFIX}_K8S_BATCH_MAX_SIZE',
        (),
        '25',
    )
    K8S_BATCH_WAIT_SECONDS = (
        f'{PREFIX}_K8S_BATCH_WAIT_SECONDS',
        (),
        '5',
    )

    # Valkey (K8s event dedupe)
    VALKEY_HOST = (
        'VALKEY_DOMAIN',
        ('VALKEY_HOST',),
        'localhost',
    )
    VALKEY_PORT = (
        'VALKEY_PORT',
        (),
        '6379',
    )
    VALKEY_PASSWORD = (
        'VALKEY_PASSWORD',
        ('MODULAR_SDK_MONGO_PASSWORD',),  # NOTE: this is a workaround, needs to be resolved
        '',
    )


# for cache
CREDENTIALS_REFRESH_INTERVAL = (
    EventConsumerEnv.CREDENTIALS_REFRESH_INTERVAL.as_int()
)
CONFIG_RELOAD_INTERVAL = 30
CONFIG_LIMIT = 500
WORKER_STOP_TIMEOUT = 15
SHUTDOWN_TIMEOUT = 10
ERROR_RETRY_SECONDS = 5
