from __future__ import annotations

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
        self._nested_cache: dict[str, ESourceENameRulesMap] = {}
        self._k8s_cache: dict[str, K8sServiceRulesMap] = {}

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
        if key in self._nested_cache:
            return self._nested_cache[key]
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if data is None:
            return None
        self._nested_cache[key] = cast(ESourceENameRulesMap, data)
        return self._nested_cache[key]

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
        if key in self._k8s_cache:
            return self._k8s_cache[key]
        data = self._s3.gz_get_json(
            bucket=self.bucket_name,
            key=key,
        )
        if data is None:
            return None
        self._k8s_cache[key] = cast(K8sServiceRulesMap, data)
        return self._k8s_cache[key]

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
        self._nested_cache[key] = data
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
        self._k8s_cache[key] = data
        self._s3.gz_put_json(
            bucket=self.bucket_name,
            key=key,
            obj=data,
        )
