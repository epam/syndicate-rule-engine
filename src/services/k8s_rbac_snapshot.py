from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from helpers.constants import (
    K8S_RBAC_OPTIONAL_RESOURCE_TYPES,
    K8S_RBAC_REQUIRED_RESOURCE_TYPES,
    K8S_RBAC_RESOURCE_TYPES,
)
from helpers.log_helper import get_logger
from services.k8s_rbac import slim_resource
from services.reports_bucket import PlatformReportsBucketKeysBuilder

if TYPE_CHECKING:
    from models.job import Job
    from services.clients.s3 import S3Client
    from services.platform_service import Platform

_LOG = get_logger(__name__)


class RbacSnapshotWriter:
    """
    Writes the RBAC inventory snapshot of one job: a slim gzipped object per
    resource type and a manifest that refers to them. Feed it with
    collected resources via write() and call finish() at the end
    """

    def __init__(
        self,
        s3_client: S3Client,
        bucket: str,
        platform: Platform,
        job: Job,
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._platform = platform
        self._job = job
        self._keys = PlatformReportsBucketKeysBuilder(platform)
        self._written: set[str] = set()

    @property
    def manifest_key(self) -> str:
        return self._keys.rbac_inventory_snapshot_manifest_key(self._job)

    @property
    def missing_optional_types(self) -> list[str]:
        return [
            rt
            for rt in K8S_RBAC_OPTIONAL_RESOURCE_TYPES
            if rt not in self._written
        ]

    @property
    def complete(self) -> bool:
        # pods and replica sets only enrich the report
        return set(K8S_RBAC_REQUIRED_RESOURCE_TYPES) <= self._written

    def _resource_key(self, resource_type: str) -> str:
        return self._keys.rbac_inventory_snapshot_resource_key(
            self._job, resource_type
        )

    def write(self, resource_type: str, resources: list[dict]) -> None:
        if resource_type not in K8S_RBAC_RESOURCE_TYPES:
            return
        try:
            self._s3.gz_put_json(
                bucket=self._bucket,
                key=self._resource_key(resource_type),
                obj=[slim_resource(resource_type, r) for r in resources],
            )
        except Exception:  # noqa: BLE001
            _LOG.exception(
                f'Failed to write RBAC snapshot for {resource_type}'
            )
            return
        self._written.add(resource_type)

    def finish(self) -> dict[str, Any]:
        """
        Writes and returns the manifest of the snapshot
        """
        manifest = {
            'schema_version': 1,
            'platform_id': self._platform.id,
            'job_id': self._job.id,
            'complete': self.complete,
            'captured_at': time.time(),
            'resource_types': {
                rt: {
                    'key': (
                        self._resource_key(rt) if rt in self._written else None
                    ),
                    'complete': rt in self._written,
                }
                for rt in K8S_RBAC_RESOURCE_TYPES
            },
        }
        self._s3.gz_put_json(
            bucket=self._bucket, key=self.manifest_key, obj=manifest
        )
        return manifest
