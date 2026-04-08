"""
Main consumer loop: parallel workers per config, supervisor syncs add/remove.
"""

from __future__ import annotations

import signal
import threading
import time
from typing import TYPE_CHECKING

from helpers.constants import SRE_K8S_WATCHER_VENDOR
from helpers.log_helper import get_logger
from onprem.event_sources_consumer.connectors.k8s_watcher.storage.redis import (
    RedisStorage,
)
from services.k8s.credentials_service import K8sCredentialsService

from .config_loader import (
    SOURCE_TYPE_K8S,
    EventSourceConfig,
    load_event_sources,
)
from .connectors import Message, SQSConnector
from .connectors.k8s_watcher import K8sWatchConnector
from .constants import (
    CONFIG_RELOAD_INTERVAL,
    CREDENTIALS_REFRESH_INTERVAL,
    ERROR_RETRY_SECONDS,
    SHUTDOWN_TIMEOUT,
    WORKER_STOP_TIMEOUT,
    EventConsumerEnv,
)
from .credentials_loader import get_credentials
from .message_processor import EventMessageProcessor


if TYPE_CHECKING:
    from services.clients.sts import StsClient
    from services.event_driven import EventIngestService
    from services.platform_service import PlatformService

_LOG = get_logger(__name__)

_shutdown = threading.Event()


def run_consumer_loop(
    application_service,
    ssm,
    event_ingest_service: EventIngestService,
    sts: StsClient | None = None,
    platform_service: PlatformService | None = None,
) -> None:
    """
    Supervisor: periodically reload configs, start workers for new configs,
    stop workers for removed configs. Each config runs in its own thread.
    """
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _LOG.info('Event sources consumer supervisor started')

    workers: dict[str, tuple[threading.Thread, threading.Event]] = {}
    workers_lock = threading.Lock()

    while not _shutdown.is_set():
        try:
            configs = load_event_sources(
                application_service=application_service,
                platform_service=platform_service,
            )
            config_ids = {c.application_id for c in configs}

            if not configs:
                _LOG.info(
                    'No event sources configured, waiting (reload in %ds)',
                    CONFIG_RELOAD_INTERVAL,
                )

            with workers_lock:
                to_remove = [aid for aid in workers if aid not in config_ids]
                for aid in to_remove:
                    thread, stop_ev = workers.pop(aid)
                    stop_ev.set()
                    _LOG.info('Stopping worker for removed config %s', aid)
                    thread.join(timeout=WORKER_STOP_TIMEOUT)

                for aid in list(workers.keys()):
                    if aid in config_ids:
                        thread, _ = workers[aid]
                        if not thread.is_alive():
                            _, stop_ev = workers.pop(aid)
                            stop_ev.set()
                            _LOG.warning('Restarting dead worker for %s', aid)

                for config in configs:
                    if config.application_id not in workers:
                        stop_ev = threading.Event()
                        if config.source_type == SOURCE_TYPE_K8S:
                            target = _run_k8s_worker
                            args = (
                                config,
                                stop_ev,
                                event_ingest_service,
                            )
                        else:
                            target = _run_sqs_worker
                            args = (
                                config,
                                stop_ev,
                                ssm,
                                event_ingest_service,
                                sts,
                            )
                        thread = threading.Thread(
                            target=target, args=args, daemon=True
                        )
                        workers[config.application_id] = (thread, stop_ev)
                        thread.start()
                        _LOG.info(
                            'Started %s worker for config %s',
                            config.source_type,
                            config.application_id,
                        )

        except Exception as e:
            _LOG.exception('Supervisor error: %s', e)

        if not _shutdown.is_set():
            time.sleep(CONFIG_RELOAD_INTERVAL)

    with workers_lock:
        for _, (thread, stop_ev) in list(workers.items()):
            stop_ev.set()
        for _, (thread, _) in list(workers.items()):
            thread.join(timeout=SHUTDOWN_TIMEOUT)

    _LOG.info('Consumer loop stopped')


def _handle_signal(signum: int, frame: object) -> None:
    _LOG.info('Received signal %s, shutting down', signum)
    _shutdown.set()


def _run_sqs_worker(
    config: EventSourceConfig,
    stop_event: threading.Event,
    ssm,
    event_ingest_service: EventIngestService,
    sts,
) -> None:
    """
    Run SQS consumer for one config in a loop until stop_event is set.
    When using role_arn (assume_role), credentials are refreshed periodically.
    """
    _LOG.info(
        'Worker started for %s (customer=%s, queue=%s)',
        config.application_id,
        config.customer_id,
        config.queue_url,
    )
    credentials = get_credentials(
        ssm=ssm, secret_name=config.secret, role_arn=config.role_arn, sts=sts
    )
    connector = SQSConnector(config=config, credentials=credentials)
    processor = EventMessageProcessor(
        event_ingest_service=event_ingest_service
    )
    try:
        connector.connect()

        def callback(msg: Message) -> None:
            processor.process(message=msg)

        last_credentials_refresh = time.monotonic()
        _LOG.info(
            'Polling queue %s (app=%s)',
            config.queue_url,
            config.application_id,
        )
        while not stop_event.is_set():
            if config.role_arn and sts:
                elapsed = time.monotonic() - last_credentials_refresh
                if elapsed >= CREDENTIALS_REFRESH_INTERVAL:
                    fresh = get_credentials(
                        ssm=ssm,
                        secret_name=config.secret,
                        role_arn=config.role_arn,
                        sts=sts,
                    )
                    if fresh:
                        connector.reconnect(fresh)
                        last_credentials_refresh = time.monotonic()
                        _LOG.debug(
                            'Refreshed credentials for %s (role=%s)',
                            config.application_id,
                            config.role_arn,
                        )
                    else:
                        _LOG.warning(
                            'Failed to refresh credentials for %s, retrying later',
                            config.application_id,
                        )

            try:
                connector.consume(callback=callback)
            except Exception as e:
                _LOG.exception(
                    'Error consuming from %s: %s', config.queue_url, e
                )
                if stop_event.wait(timeout=ERROR_RETRY_SECONDS):
                    break
    finally:
        connector.disconnect()
        _LOG.info(
            'Worker stopped for %s (%s)',
            config.application_id,
            config.queue_url,
        )


def _run_k8s_worker(
    config: EventSourceConfig,
    stop_event: threading.Event,
    event_ingest_service: EventIngestService,
) -> None:
    """
    Run K8s watch consumer for one config in a loop until stop_event is set.
    Credentials come from the Platform application secret.
    """
    _LOG.info(
        'K8S worker started for %s (customer=%s, platform=%s)',
        config.application_id,
        config.customer_id,
        config.platform_id,
    )
    connector = K8sWatchConnector(
        source_config=config,
        credentials_service=K8sCredentialsService.build(),
        storage=RedisStorage(
            host=EventConsumerEnv.REDIS_HOST.as_str(),
            port=EventConsumerEnv.REDIS_PORT.as_int(),
            password=EventConsumerEnv.REDIS_PASSWORD.get() or '',
        ),
    )
    processor = EventMessageProcessor(
        event_ingest_service=event_ingest_service
    )
    try:
        connector.connect()

        def callback(msg: Message) -> None:
            processor.process(message=msg, vendor=SRE_K8S_WATCHER_VENDOR)

        _LOG.info(
            'Watching K8s events for platform %s (app=%s)',
            config.platform_id,
            config.application_id,
        )
        while not stop_event.is_set():
            try:
                connector.consume(callback=callback)
            except Exception as e:
                _LOG.exception(
                    'Error watching K8s events for platform %s: %s',
                    config.platform_id,
                    e,
                )
                if stop_event.wait(timeout=ERROR_RETRY_SECONDS):
                    break
    finally:
        connector.disconnect()
        _LOG.info(
            'K8S worker stopped for %s (platform=%s)',
            config.application_id,
            config.platform_id,
        )
