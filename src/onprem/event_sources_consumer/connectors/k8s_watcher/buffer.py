from __future__ import annotations

import threading
import uuid
from collections import deque
from collections.abc import Callable

from helpers.log_helper import get_logger
from onprem.event_sources_consumer.connectors.base import Message


_LOG = get_logger(__name__)


class K8sWatchIngestBuffer:
    """
    Batches normalized events and invokes the ingest callback from a background thread.
    Persists cursor (resourceVersion + last event time) only after a successful callback.
    """

    __slots__ = (
        '_batch_max',
        '_batch_wait',
        '_cursor_key',
        '_cursor_storage',
        '_queue',
        '_lock',
        '_wake',
        '_stop',
        '_thread',
        '_flush_callback',
    )

    def __init__(
        self,
        batch_max_size: int,
        batch_wait_seconds: float,
    ) -> None:
        self._batch_max = batch_max_size
        self._batch_wait = batch_wait_seconds
        self._queue: deque[dict] = deque()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = False
        self._thread: threading.Thread | None = None
        self._flush_callback: Callable[[Message], None] | None = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None

    def start(
        self,
        flush_callback: Callable[[Message], None],
    ) -> None:
        if self._thread is not None:
            return
        self._flush_callback = flush_callback
        self._stop = False
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
        )
        self._thread.start()

    def add(
        self,
        event: dict,
    ) -> None:
        wake = False
        with self._lock:
            self._queue.append(event)
            if len(self._queue) >= self._batch_max:
                wake = True
        if wake:
            self._wake.set()

    def shutdown(
        self,
        timeout: float = 10.0,
    ) -> None:
        self._stop = True
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        self._flush_callback = None

    def flush_all_pending(
        self,
        flush_callback: Callable[[Message], None],
    ) -> None:
        """Synchronous drain (e.g. after 410 catch-up)."""
        while True:
            batch = self._take_batch_for_flush()
            if not batch:
                break
            return self._invoke_batch(
                batch,
                flush_callback,
            )

    def _run(self) -> None:
        flush_callback = self._flush_callback
        if flush_callback is None:
            return
        while True:
            self._wake.wait(timeout=self._batch_wait)
            self._wake.clear()
            if self._stop and not self._queue_has_items():
                break
            batch = self._take_batch_for_flush()
            if batch:
                self._invoke_batch(batch, flush_callback)
                if self._queue_has_items():
                    self._wake.set()
            elif self._stop and not self._queue_has_items():
                break

    def _queue_has_items(self) -> bool:
        with self._lock:
            return bool(self._queue)

    def _take_batch_for_flush(self) -> list[dict]:
        with self._lock:
            if not self._queue:
                return []
            n = min(len(self._queue), self._batch_max)
            return [self._queue.popleft() for _ in range(n)]

    def _invoke_batch(
        self,
        batch: list[dict],
        flush_callback: Callable[[Message], None],
    ) -> None:
        if not batch:
            return
        try:
            flush_callback(
                Message(
                    message_id=str(uuid.uuid4()),
                    body=batch,
                )
            )
        except Exception:
            with self._lock:
                self._queue.extendleft(reversed(batch))
            raise
