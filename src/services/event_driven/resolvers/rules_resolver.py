from __future__ import annotations

from typing import TYPE_CHECKING

from helpers.constants import Cloud
from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from helpers import Version
    from services.event_driven.mappings.provider import S3EventMappingProvider

    from ..domain.types import EventGenericRecord


_LOG = get_logger(__name__)


class RulesResolver:
    def __init__(self, event_mapping_provider: S3EventMappingProvider) -> None:
        self._event_mapping_provider = event_mapping_provider

    def cloud_mapping(
        self,
        cloud: Cloud,
        license_key: str,
        version: Version,
    ) -> dict:
        data = self._event_mapping_provider.get_from_s3(
            version=version,
            license_key=license_key,
            cloud=cloud,
        )
        if data is None:
            _LOG.warning(
                f"No event mapping found for {cloud.value} in S3 "
                f"for license key: {license_key} and version: {version.to_str()} "
                f"May be metadata is not synced for this license key and version"
            )
            return {}
        return data

    def get_rules(
        self,
        event: EventGenericRecord,
        license_key: str,
        version: Version,
    ) -> set[str]:
        cloud_enum = Cloud(event.cloud)
        event_source = event.source_name
        event_name = event.event_name

        if cloud_enum == Cloud.KUBERNETES:
            k8s_map = self._event_mapping_provider.get_k8s_mapping_from_s3(
                license_key=license_key,
                version=version,
            )
            if not k8s_map:
                return set()
            return set(k8s_map.get(event_source, []))

        cloud_map = self.cloud_mapping(cloud_enum, license_key, version)
        return set[str](cloud_map.get(event_source, {}).get(event_name, []))
