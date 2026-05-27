from typing import Any, Literal, TypedDict

from c7n.utils import jmespath_search


class SREEventDrivenEvent(TypedDict):
    event: str
    source: str
    ids: str


class SREEventDrivenMode(TypedDict):
    mode: Literal['sre-aws-event-driven']
    events: list[SREEventDrivenEvent]


class SREEvents:
    """Extract resource ids from SRE DB payloads: ``{cloud, region_name, source_name, event_name, metadata}``."""

    @classmethod
    def get_sre_ids(
        cls,
        event: dict[str, Any],
        mode: SREEventDrivenMode,
    ) -> set[str]:
        """Extract resource ids from an SRE row (see policy mode ``events`` subscriptions)."""
        resource_ids = set()
        for e in mode.get('events', []):
            if event.get('event_name') != e.get('event'):
                continue
            if event.get('source_name') != e.get('source'):
                continue
            jmespath = f"metadata.{e.get('ids')}"
            ids = jmespath_search(jmespath, event)
            if ids:
                if not isinstance(ids, (list, tuple, set)):
                    ids = [ids]
                resource_ids.update(ids)
        return resource_ids
