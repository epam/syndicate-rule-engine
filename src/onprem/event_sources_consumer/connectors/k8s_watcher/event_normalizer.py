from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.log_helper import get_logger


if TYPE_CHECKING:
    from .dedupe import EventUidDeduper


_LOG = get_logger(__name__)


class WatchEventNormalizer:
    """Maps CoreV1 Event dicts to SRE_K8S_WATCHER ingest shape."""

    __slots__ = ('_platform_id', '_deduper')

    def __init__(self, platform_id: str, deduper: EventUidDeduper) -> None:
        self._platform_id = platform_id
        self._deduper = deduper

    def try_normalize(self, obj: dict) -> dict | None:
        if not isinstance(obj, dict):
            _LOG.warning('Skipping non-dict event: %s', obj)
            return None
        md = obj.get('metadata')
        if not isinstance(md, dict):
            _LOG.warning('Skipping non-dict metadata: %s', md)
            return None
        uid = md.get('uid')
        rv = md.get('resourceVersion')
        if not uid or not rv:
            _LOG.warning(
                'Skipping event with missing uid or resourceVersion: %s', obj
            )
            return None
        if self._deduper.is_duplicate(str(uid), str(rv)):
            _LOG.warning('Skipping duplicate event %s', uid)
            return None
        involved = obj.get('involvedObject')
        kind = (
            involved.get('kind') if isinstance(involved, dict) else None
        ) or 'Unknown'
        reason = obj.get('reason') or ''
        return {
            'type': kind,
            'reason': reason,
            'platformId': self._platform_id,
        }
