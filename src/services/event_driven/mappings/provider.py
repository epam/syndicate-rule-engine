from __future__ import annotations

import time
from typing import cast

from helpers import Version, urljoin
from helpers.constants import Cloud
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.event_driven.domain import ESourceENameRulesMap, K8sServiceRulesMap


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
        cloud_name = cloud.value if isinstance(cloud, Cloud) else cloud
        cloud_name = cloud_name.lower()
        file_name = cloud_name + cls.suffix
        version_str = version.to_str() if isinstance(version, Version) else version
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
        self._nested_cache: dict[str, tuple[ESourceENameRulesMap, float]] = {}
        self._k8s_cache: dict[str, tuple[K8sServiceRulesMap, float]] = {}

    @property
    def bucket_name(self) -> str:
        return self._env.get_rulesets_bucket_name()

    def get_from_s3(
        self,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> ESourceENameRulesMap | None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        cached = self._get_cached_nested(key)
        if cached is not None:
            return cached
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if data is None:
            return None
        nested = cast(ESourceENameRulesMap, data)
        self._nested_cache[key] = (nested, time.monotonic())
        return nested

    def get_k8s_mapping_from_s3(
        self,
        license_key: str,
        version: Version | str,
    ) -> K8sServiceRulesMap | None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=Cloud.KUBERNETES,
        )
        cached = self._get_cached_k8s(key)
        if cached is not None:
            return cached
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if data is None:
            return None
        k8s_map = cast(K8sServiceRulesMap, data)
        self._k8s_cache[key] = (k8s_map, time.monotonic())
        return k8s_map

    def set_to_s3(
        self,
        license_key: str,
        version: Version,
        cloud: Cloud | str,
        data: ESourceENameRulesMap,
    ) -> None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        self._set_cached_nested(key, data)
        self._s3.gz_put_json(
            bucket=self.bucket_name,
            key=key,
            obj=data,
        )

    def set_k8s_mapping_to_s3(
        self,
        license_key: str,
        version: Version,
        data: K8sServiceRulesMap,
    ) -> None:
        key = EventMappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=Cloud.KUBERNETES,
        )
        self._set_cached_k8s(key, data)
        self._s3.gz_put_json(
            bucket=self.bucket_name,
            key=key,
            obj=data,
        )

    def _cache_entry_fresh(self, loaded_at_monotonic: float) -> bool:
        ttl = self._env.event_mapping_cache_ttl_seconds()
        if ttl <= 0:
            return True
        return (time.monotonic() - loaded_at_monotonic) < ttl

    def _get_cached_nested(self, key: str) -> ESourceENameRulesMap | None:
        entry = self._nested_cache.get(key)
        if entry is None:
            return None
        data, loaded_at = entry
        if self._cache_entry_fresh(loaded_at):
            return data
        del self._nested_cache[key]
        return None

    def _get_cached_k8s(self, key: str) -> K8sServiceRulesMap | None:
        entry = self._k8s_cache.get(key)
        if entry is None:
            return None
        data, loaded_at = entry
        if self._cache_entry_fresh(loaded_at):
            return data
        del self._k8s_cache[key]
        return None

    def _set_cached_nested(self, key: str, data: ESourceENameRulesMap) -> None:
        self._nested_cache[key] = (data, time.monotonic())
    
    def _set_cached_k8s(self, key: str, data: K8sServiceRulesMap) -> None:
        self._k8s_cache[key] = (data, time.monotonic())
