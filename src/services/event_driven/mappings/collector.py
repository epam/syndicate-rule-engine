from __future__ import annotations

from typing import TYPE_CHECKING

from helpers import Version
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.event_driven.domain import (
    ESourceENameRulesMap,
    K8sServiceRulesMap,
)
from services.event_driven.mappings.provider import S3EventMappingProvider

if TYPE_CHECKING:
    from services.metadata import Metadata, RuleMetadata


_LOG = get_logger(__name__)


class EventMappingCollector(S3EventMappingProvider):
    """
    Collector for event mappings.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        super().__init__(s3_client, environment_service)
        self._aws_events: ESourceENameRulesMap = {}
        self._azure_events: ESourceENameRulesMap = {}
        self._google_events: ESourceENameRulesMap = {}
        self._k8s_events: K8sServiceRulesMap = {}

    def on_refresh(
        self,
        metadata: Metadata,
        license_key: str,
        version: Version,
    ) -> None:
        _LOG.info(
            f'Refreshing event mappings from metadata '
            f'(license_key={license_key!r}, version={version.to_str()})'
        )
        for rule_name, rule_meta in metadata.rules.items():
            self._add_meta(rule_name, rule_meta)

        if self._aws_events:
            self.set_to_s3(
                license_key=license_key,
                version=version,
                cloud=Cloud.AWS,
                data=self._aws_events,
            )
        if self._azure_events:
            self.set_to_s3(
                license_key=license_key,
                version=version,
                cloud=Cloud.AZURE,
                data=self._azure_events,
            )
        if self._google_events:
            self.set_to_s3(
                license_key=license_key,
                version=version,
                cloud=Cloud.GOOGLE,
                data=self._google_events,
            )
        if self._k8s_events:
            self.set_k8s_mapping_to_s3(
                license_key=license_key,
                version=version,
                data=self._k8s_events,
            )
        _LOG.info(
            f'Event mappings saved to S3: '
            f'AWS={len(self._aws_events)} sources, '
            f'Azure={len(self._azure_events)} sources, '
            f'Google={len(self._google_events)} sources, '
            f'K8s={len(self._k8s_events)} services.',
        )
        self.reset()

    def reset(self) -> None:
        self._aws_events.clear()
        self._azure_events.clear()
        self._google_events.clear()
        self._k8s_events.clear()
        _LOG.info('Event mappings reset')

    def _add_meta(
        self,
        rule_name: str,
        meta: RuleMetadata,
    ) -> None:
        cloud = meta.cloud
        parsed = cloud if isinstance(cloud, Cloud) else Cloud.parse(cloud)
        if parsed == Cloud.KUBERNETES:
            rules = self._k8s_events.setdefault(meta.service, [])
            if rule_name not in rules:
                rules.append(rule_name)
            return

        if not meta.events:
            _LOG.warning(
                f'No events found for {meta.cloud!r} with source {meta.source!r}. '
                'May be license is not activated for event-driven mode usage.'
            )
            return

        event_map = self._event_map(cloud=meta.cloud)
        if event_map is None:
            _LOG.warning(
                f'Unknown cloud {meta.cloud!r} for rule {rule_name!r}. '
                'Skipping event mapping.'
            )
            return

        for source, event_names in meta.events.items():
            if not isinstance(event_names, list):
                _LOG.warning(
                    f'Invalid event names format for rule {rule_name!r}, '
                    f'source {source!r}. Expected list, got {type(event_names).__name__}.'
                )
                continue
            event_map.setdefault(source, {})
            for event_name in event_names:
                rules_for_event = event_map[source].setdefault(event_name, [])
                if rule_name not in rules_for_event:
                    rules_for_event.append(rule_name)

    def _event_map(self, cloud: Cloud | str) -> ESourceENameRulesMap | None:
        if not isinstance(cloud, Cloud):
            parsed_cloud = Cloud.parse(cloud)
            if parsed_cloud is None:
                return None
            cloud = parsed_cloud

        if cloud == Cloud.AWS:
            return self._aws_events
        if cloud == Cloud.AZURE:
            return self._azure_events
        if cloud == Cloud.GOOGLE:
            return self._google_events
        return None
