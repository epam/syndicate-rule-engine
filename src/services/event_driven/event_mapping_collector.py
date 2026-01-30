from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, cast

from helpers.constants import Cloud
from helpers.log_helper import get_logger
from helpers import Version, urljoin
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService


if TYPE_CHECKING:
    from services.metadata import RuleMetadata, Metadata


_LOG = get_logger(__name__)

T = TypeVar("T")


EventName = str
Source = str
RuleName = str
RuleNames = list[RuleName]

EventMapping = dict[Source, dict[EventName, RuleNames]]
"""Mapping of event names to rule names."""


class EventMappingBucketKeys:
    """
    Keys for event mapping bucket in S3.
    """

    prefix = "mappings/"
    events = "events/"
    suffix = ".json.gz"

    @classmethod
    def event_mapping_key(
        cls,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> str:
        """
        Builds event mapping key in S3.

        Example:
        mappings/2341-f34d-4567-8901-234567890123/1.0.0/events/aws.json.gz
        """
        cloud_name = cloud.value if isinstance(cloud, Cloud) else cloud
        cloud_name = cloud_name.lower()
        file_name = cloud_name + cls.suffix
        version_str = (
            version.to_str()
            if isinstance(version, Version)
            else version
        )
        return urljoin(
            cls.prefix,
            license_key,
            version_str,
            cls.events,
            file_name,
        )


class S3EventMappingProvider:
    """
    Provider for event mappings in S3.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        self._s3 = s3_client
        self._env = environment_service

        self._cache: dict[str, EventMapping] = {}

    @property
    def bucket_name(self) -> str:
        return self._env.get_rulesets_bucket_name()

    def get_from_s3(
        self,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> EventMapping | None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        if license_key in self._cache:
            return self._cache[key]
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if data is None:
            return None
        self._cache[key] = cast(EventMapping, data)
        return self._cache[key]

    def set_to_s3(
        self,
        license_key: str,
        version: Version,
        cloud: Cloud | str,
        data: EventMapping,
    ) -> None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        self._cache[key] = data
        self._s3.gz_put_json(
            bucket=self.bucket_name,
            key=key,
            obj=data,
        )


class EventMappingCollector(S3EventMappingProvider):
    """
    Collector for event mappings.
    Implements MetadataRefreshHook to automatically update mappings when metadata is refreshed.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        super().__init__(s3_client, environment_service)

        self._aws_events: EventMapping = {}
        self._azure_events: EventMapping = {}
        self._google_events: EventMapping = {}

    def on_refresh(
        self,
        metadata: Metadata,
        license_key: str,
        version: Version,
    ) -> None:
        """
        Called when metadata is refreshed from LM.
        Collects mappings from metadata rules.

        Args:
            metadata: Metadata object that was refreshed from LM.
            license_key: License key that was used to save metadata.
            version: Version of metadata that was saved.
        """
        _LOG.info(
            f"Refreshing event mappings from metadata "
            f"(license_key={license_key!r}, version={version.to_str()})"
        )

        # Collect mappings from all rules
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
        _LOG.info(
            f"Event mappings saved to S3: "
            f"AWS={len(self._aws_events)} sources, "
            f"Azure={len(self._azure_events)} sources, "
            f"Google={len(self._google_events)} sources.",
        )
        self.reset()

    def reset(self) -> None:
        """Reset all event mappings."""
        self._aws_events.clear()
        self._azure_events.clear()
        self._google_events.clear()
        _LOG.info("Event mappings reset")

    def _add_meta(
        self,
        rule_name: str,
        meta: RuleMetadata,
    ) -> None:
        if not meta.events:
            _LOG.warning(
                f"No events found for {meta.cloud!r} with source {meta.source!r}. "
                "May be license is not activated for event-driven mode usage."
            )
            return

        _map = self._event_map(cloud=meta.cloud)
        if _map is None:
            _LOG.warning(
                f"Unknown cloud {meta.cloud!r} for rule {rule_name!r}. "
                "Skipping event mapping."
            )
            return

        for source, event_names in meta.events.items():
            if not isinstance(event_names, list):
                _LOG.warning(
                    f"Invalid event names format for rule {rule_name!r}, "
                    f"source {source!r}. Expected list, got {type(event_names).__name__}."
                )
                continue
            _map.setdefault(source, {})
            for event_name in event_names:
                _map[source].setdefault(event_name, []).append(rule_name)

    def _event_map(self, cloud: str | Cloud) -> EventMapping | None:
        """Return mutable event mapping for the given cloud and license key."""
        if not isinstance(cloud, Cloud):
            try:
                parsed_cloud = Cloud.parse(cloud)
                if parsed_cloud is None:
                    return None
                cloud = parsed_cloud
            except ValueError:
                return None

        if cloud == Cloud.AWS:
            return self._aws_events
        elif cloud == Cloud.AZURE:
            return self._azure_events
        elif cloud == Cloud.GOOGLE:
            return self._google_events
        else:
            return None
