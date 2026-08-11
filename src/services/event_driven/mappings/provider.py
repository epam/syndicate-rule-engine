from __future__ import annotations

import time
from enum import Enum
from typing import cast, Any

from helpers import Version, urljoin
from helpers.constants import Cloud
from services.clients.s3 import S3Client
from services.environment_service import EnvironmentService
from services.event_driven.domain import ESourceENameRulesMap, K8sServiceRulesMap


class MappingPaths(str, Enum):
    EVENTS = "events/"
    PERIODIC = "periodic/"


class MappingBucketKeys:
    """
    Keys for mapping folder in `rulesets` S3 bucket
    """

    prefix = "mappings/"
    suffix = ".json.gz"

    @classmethod
    def mapping_key(
        cls,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
        mapping_path: MappingPaths | str,
    ) -> str:
        cloud_name = cloud.value if isinstance(cloud, Cloud) else cloud
        cloud_name = cloud_name.lower()
        file_name = cloud_name + cls.suffix
        version_str = version.to_str() if isinstance(version, Version) else version
        mapping_path = (
            mapping_path.value
            if isinstance(mapping_path, MappingPaths)
            else mapping_path
        )
        return urljoin(
            cls.prefix,
            license_key,
            version_str,
            mapping_path,
            file_name,
        )

    @classmethod
    def event_mapping_key(
        cls,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> str:
        return cls.mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
            mapping_path=MappingPaths.EVENTS,
        )

    @classmethod
    def periodic_mapping_key(
        cls,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> str:
        return cls.mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
            mapping_path=MappingPaths.PERIODIC,
        )


class S3MappingProvider:
    """
    Base class for S3 mapping providers.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        self._s3 = s3_client
        self._env = environment_service
        self._nested_cache: dict[str, tuple[Any, float]] = {}

    @property
    def bucket_name(self) -> str:
        return self._env.get_rulesets_bucket_name()

    def get_from_s3_by_key(
        self,
        key: str,
    ) -> Any | None:

        cached = self._get_cached_nested(key)
        if cached is not None:
            return cached
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if not data:
            return None
        self._set_cached_nested(key, data)
        return data

    def set_to_s3_by_key(
        self,
        key: str,
        data: Any,
    ) -> None:
        self._set_cached_nested(key, data)
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

    def _get_cached_nested(self, key: str) -> Any | None:
        entry = self._nested_cache.get(key)
        if entry is None:
            return None
        data, loaded_at = entry
        if self._cache_entry_fresh(loaded_at):
            return data
        del self._nested_cache[key]
        return None

    def _set_cached_nested(self, key: str, data: Any) -> None:
        if not data:
            return
        self._nested_cache[key] = (data, time.monotonic())


class S3EventMappingProvider(S3MappingProvider):
    """
    Provider for event mappings in S3.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        super().__init__(s3_client, environment_service)
        self._nested_cache: dict[str, tuple[ESourceENameRulesMap, float]] = {}
        self._k8s_cache: dict[str, tuple[K8sServiceRulesMap, float]] = {}

    def get_from_s3(
        self,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> ESourceENameRulesMap | None:
        key = MappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        return self.get_from_s3_by_key(key)

    def get_k8s_mapping_from_s3(
        self,
        license_key: str,
        version: Version | str,
    ) -> K8sServiceRulesMap | None:
        key = MappingBucketKeys.event_mapping_key(
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
        if not data:
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
        key = MappingBucketKeys.event_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        self.set_to_s3_by_key(key, data)

    def set_k8s_mapping_to_s3(
        self,
        license_key: str,
        version: Version,
        data: K8sServiceRulesMap,
    ) -> None:
        key = MappingBucketKeys.event_mapping_key(
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

    def _get_cached_k8s(self, key: str) -> K8sServiceRulesMap | None:
        entry = self._k8s_cache.get(key)
        if entry is None:
            return None
        data, loaded_at = entry
        if self._cache_entry_fresh(loaded_at):
            return data
        del self._k8s_cache[key]
        return None

    def _set_cached_k8s(self, key: str, data: K8sServiceRulesMap) -> None:
        if not data:
            return
        self._k8s_cache[key] = (data, time.monotonic())


class S3PeriodicMappingProvider(S3MappingProvider):
    """
    Provider for periodic mappings in S3.
    """

    def __init__(
        self,
        s3_client: S3Client,
        environment_service: EnvironmentService,
    ) -> None:
        super().__init__(s3_client, environment_service)

    def get_from_s3(
        self,
        license_key: str,
        version: Version | str,
        cloud: Cloud | str,
    ) -> list | None:
        key = MappingBucketKeys.periodic_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        return self.get_from_s3_by_key(key)

    def set_to_s3(
        self,
        license_key: str,
        version: Version,
        cloud: Cloud | str,
        data: list,
    ) -> None:
        key = MappingBucketKeys.periodic_mapping_key(
            license_key=license_key,
            version=version,
            cloud=cloud,
        )
        self.set_to_s3_by_key(key, data)
