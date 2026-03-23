"""
Main consumer loop: parallel workers per config, supervisor syncs add/remove.
"""

from __future__ import annotations

import signal
import threading
import time
from typing import TYPE_CHECKING

from helpers.log_helper import get_logger

from .config_loader import EventSourceConfig, load_event_sources
from .connectors import Message, SQSConnector
from .credentials_loader import get_credentials
from .message_processor import process_message
from . import settings

if TYPE_CHECKING:
    from services.event_driven import EventIngestService
    from modular_sdk.services.application_service import ApplicationService
    from modular_sdk.services.ssm_service import SSMClientCachingWrapper
    from services.clients.sts import StsClient

_LOG = get_logger(__name__)

_shutdown = threading.Event()


def run_consumer_loop(
    application_service: ApplicationService,
    ssm: SSMClientCachingWrapper,
    event_ingest_service: EventIngestService,
    sts: StsClient | None = None,
) -> None:
    """
    Supervisor: periodically reload configs, start workers for new configs,
    stop workers for removed configs. Each config runs in its own thread.
    """
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _LOG.info("Event sources consumer supervisor started")

    workers: dict[str, tuple[threading.Thread, threading.Event]] = {}
    workers_lock = threading.Lock()

    while not _shutdown.is_set():
        try:
            configs = load_event_sources(application_service)
            config_ids = {c.application_id for c in configs}

            if not configs:
                _LOG.info(
                    "No event sources configured, waiting (reload in %ds)",
                    settings.CONFIG_RELOAD_INTERVAL,
                )

            with workers_lock:
                # Stop workers for removed configs
                to_remove = [aid for aid in workers if aid not in config_ids]
                for aid in to_remove:
                    thread, stop_ev = workers.pop(aid)
                    stop_ev.set()
                    _LOG.info("Stopping worker for removed config %s", aid)
                    thread.join(timeout=settings.WORKER_STOP_TIMEOUT)

                # Restart workers that have died (e.g. exited due to exception)
                for aid in list(workers.keys()):
                    if aid in config_ids:
                        thread, _ = workers[aid]
                        if not thread.is_alive():
                            _, stop_ev = workers.pop(aid)
                            stop_ev.set()
                            _LOG.warning("Restarting dead worker for %s", aid)

                # Start workers for new configs (including restarted)
                for config in configs:
                    if config.application_id not in workers:
                        stop_ev = threading.Event()
                        thread = threading.Thread(
                            target=_run_worker,
                            args=(
                                config,
                                stop_ev,
                                ssm,
                                event_ingest_service,
                                sts,
                            ),
                            daemon=True,
                        )
                        workers[config.application_id] = (thread, stop_ev)
                        thread.start()
                        _LOG.info(
                            "Started worker for new config %s",
                            config.application_id,
                        )

        except Exception as e:
            _LOG.exception("Supervisor error: %s", e)

        if not _shutdown.is_set():
            time.sleep(settings.CONFIG_RELOAD_INTERVAL)

    # Signal all workers to stop
    with workers_lock:
        for _, (thread, stop_ev) in list(workers.items()):
            stop_ev.set()
        for _, (thread, _) in list(workers.items()):
            thread.join(timeout=settings.SHUTDOWN_TIMEOUT)

    _LOG.info("Consumer loop stopped")


def _handle_signal(signum: int, frame: object) -> None:
    _LOG.info("Received signal %s, shutting down", signum)
    _shutdown.set()


def _run_worker(
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
        "Worker started for %s (customer=%s, queue=%s)",
        config.application_id,
        config.customer_id,
        config.queue_url,
    )
    credentials = get_credentials(
        ssm=ssm,
        secret_name=config.secret,
        role_arn=config.role_arn,
        sts=sts,
    )
    connector = SQSConnector(config=config, credentials=credentials)
    try:
        connector.connect()

        def callback(msg: Message) -> None:
            process_message(
                message=msg,
                event_ingest_service=event_ingest_service,
            )

        last_credentials_refresh = time.monotonic()
        _LOG.info("Polling queue %s (app=%s)", config.queue_url, config.application_id)
        while not stop_event.is_set():
            # Refresh assume_role credentials before they expire
            if config.role_arn and sts:
                elapsed = time.monotonic() - last_credentials_refresh
                if elapsed >= settings.CREDENTIALS_REFRESH_INTERVAL:
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
                            "Refreshed credentials for %s (role=%s)",
                            config.application_id,
                            config.role_arn,
                        )
                    else:
                        _LOG.warning(
                            "Failed to refresh credentials for %s, retrying later",
                            config.application_id,
                        )

            try:
                connector.consume(callback=callback)
            except Exception as e:
                _LOG.exception(
                    "Error consuming from %s: %s",
                    config.queue_url,
                    e,
                )
                if stop_event.wait(timeout=settings.ERROR_RETRY_SECONDS):
                    break
    finally:
        connector.disconnect()
        _LOG.info(
            "Worker stopped for %s (%s)",
            config.application_id,
            config.queue_url,
        )
