"""
K8s watch connector: streams events from /api/v1/events?watch=true.
Credentials are resolved from the Platform application secret (kubeconfig).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from kubernetes import client, config, watch
from kubernetes.client import ApiClient

from helpers.log_helper import get_logger
from onprem.event_sources_consumer.connectors.base import (
    BaseConnector,
    Message,
)
from onprem.event_sources_consumer.constants import EventConsumerEnv
from services.platform_service import K8STokenKubeconfig

from .buffer import K8sWatchIngestBuffer
from .dedupe import EventUidDeduper
from .event_normalizer import WatchEventNormalizer


if TYPE_CHECKING:
    from onprem.event_sources_consumer.config_loader import EventSourceConfig
    from services.k8s.credentials_service import K8sCredentialsService

    from .storage import Storage

_LOG = get_logger(__name__)


class K8sWatchConnector(BaseConnector):
    """Streams K8s events via the watch API; batches ingest and persists watch cursor."""

    def __init__(
        self,
        source_config: EventSourceConfig,
        credentials_service: K8sCredentialsService,
        storage: Storage,
    ) -> None:
        self._config = source_config
        self._credentials_service = credentials_service

        # k8s client
        self._api_client: ApiClient | None = None
        self._core_v1: client.CoreV1Api | None = None
        self._active_watch: watch.Watch | None = None

        # internal tools
        self._buffer: K8sWatchIngestBuffer | None = None
        self._deduper = EventUidDeduper(storage=storage)
        self._normalizer: WatchEventNormalizer | None = None

    def connect(self) -> None:
        self.disconnect()

        platform_id = self._config.platform_id
        if not platform_id:
            _LOG.warning(
                'K8S event source %s has no platform_id',
                self._config.application_id,
            )
            return

        kubeconfig = self._credentials_service.get_kubeconfig(
            platform_id=platform_id
        )
        if not kubeconfig:
            _LOG.warning('No kubeconfig found for platform %s', platform_id)
            return

        if isinstance(kubeconfig, K8STokenKubeconfig):
            raw_kubeconfig = kubeconfig.build_config()
        else:
            raw_kubeconfig = kubeconfig.raw

        # TODO: remove after after testing
        # try:
        #     import os

        #     cluster = raw_kubeconfig['clusters'][0]['cluster']
        #     cluster['server'] = 'https://127.0.0.1:32771'
        #     cluster['certificate-authority-data'] = os.getenv('CA')

        #     user = raw_kubeconfig['users'][0]['user']
        #     token = os.getenv('TOKEN')
        #     user['token'] = token
            
        #     import json; _LOG.info(json.dumps(raw_kubeconfig, indent=2))
        # except (KeyError, IndexError, TypeError) as e:
        #     _LOG.warning('Could not patch kubeconfig server URL: %s', e)

        self._api_client = config.new_client_from_config_dict(raw_kubeconfig)
        self._core_v1 = client.CoreV1Api(self._api_client)

        self._normalizer = WatchEventNormalizer(platform_id, self._deduper)
        self._buffer = K8sWatchIngestBuffer(
            batch_max_size=EventConsumerEnv.K8S_BATCH_MAX_SIZE.as_int(),
            batch_wait_seconds=EventConsumerEnv.K8S_BATCH_WAIT_SECONDS.as_float(),
        )

        _LOG.info(
            'K8s watcher connected for platform %s (app=%s)',
            platform_id,
            self._config.application_id,
        )

    def consume(
        self,
        callback: Callable[[Message], None],
    ) -> None:
        core_v1 = self._core_v1
        buf = self._buffer
        if core_v1 is None or buf is None:
            raise RuntimeError('K8sWatchConnector not connected')
        if self._normalizer is None:
            raise RuntimeError('K8sWatchConnector normalizer not initialized')

        buf.start(callback)
        self._watch_stream_once(core_v1, buf, callback)

    def disconnect(self) -> None:
        if self._active_watch is not None:
            self._active_watch.stop()
            self._active_watch = None
        if self._buffer is not None:
            self._buffer.shutdown()
            self._buffer = None
        if self._api_client is not None:
            self._api_client.close()
        self._api_client = None
        self._core_v1 = None
        self._normalizer = None

    def _watch_stream_once(
        self,
        core_v1: client.CoreV1Api,
        buf: K8sWatchIngestBuffer,
        flush_callback: Callable[[Message], None],
    ) -> None:
        w = watch.Watch()
        self._active_watch = w

        try:
            for event in w.stream(
                core_v1.list_event_for_all_namespaces,
            ):
                if event is None or not isinstance(event, dict):
                    continue
                self._handle_watch_event(event, buf)
        finally:
            self._active_watch = None
            w.stop()
            buf.flush_all_pending(flush_callback=flush_callback)

    def _handle_watch_event(
        self,
        event: dict,
        buf: K8sWatchIngestBuffer,
    ) -> None:
        etype = event.get('type')
        if etype not in ('ADDED', 'MODIFIED'):
            _LOG.warning('Skipping watch event type %s', etype)
            return
        raw = event.get('raw_object')
        if raw is None:
            obj = event.get('object')
            if obj is not None and hasattr(obj, 'to_dict'):
                raw = obj.to_dict()
            elif isinstance(obj, dict):
                raw = obj
        if not isinstance(raw, dict):
            _LOG.warning('Skipping non-dict raw object: %s', raw)
            return
        if self._normalizer is None:
            _LOG.error('K8sWatchConnector normalizer not initialized')
            return
        norm = self._normalizer.try_normalize(raw)
        if not norm:
            _LOG.debug('K8sWatchConnector normalizer returned None')
            return
        buf.add(norm)
