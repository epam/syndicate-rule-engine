from __future__ import annotations

from pathlib import PurePosixPath
from typing import Iterable

from helpers import batches
from helpers.constants import Cloud
from helpers.log_helper import get_logger
from executor.job.scan.codec import decode_failed_policies, encode_failed_policies
from executor.job.scan.types import FailedPoliciesMap
from services.clients.s3 import S3Client
from services.sharding import (
    RuleMeta,
    ShardPart,
    ShardsCollection,
    ShardsCollectionFactory,
    ShardsS3IO,
)

_LOG = get_logger(__name__)


class ScanPartialStore:
    """S3 persistence for in-progress scan shards + ``failed.json.gz`` sidecar."""

    __slots__ = ("_s3",)

    def __init__(self, s3: S3Client) -> None:
        self._s3 = s3

    @staticmethod
    def _failed_sidecar_key(partial_key: str) -> str:
        return str(PurePosixPath(partial_key) / "failed.json")

    def load_partial_collection(
        self,
        cloud: Cloud,
        bucket: str,
        partial_key: str,
    ) -> ShardsCollection:
        coll = ShardsCollectionFactory.from_cloud(cloud)
        coll.io = ShardsS3IO(bucket=bucket, key=partial_key, client=self._s3)
        coll.fetch_all()
        coll.fetch_meta()
        return coll

    @staticmethod
    def merge_delta_into_partial(
        partial: ShardsCollection,
        parts: Iterable[ShardPart],
        meta: dict[str, RuleMeta],
    ) -> None:
        partial.put_parts(parts)
        partial.update_meta(meta)

    def write_failed_policies_sidecar(
        self,
        bucket: str,
        partial_key: str,
        failed: FailedPoliciesMap,
    ) -> None:
        self._s3.gz_put_object(
            bucket=bucket,
            key=self._failed_sidecar_key(partial_key),
            body=encode_failed_policies(failed),
        )

    def load_failed_policies_sidecar(
        self,
        bucket: str,
        partial_key: str,
    ) -> FailedPoliciesMap:
        buf = self._s3.gz_get_object(
            bucket=bucket, key=self._failed_sidecar_key(partial_key)
        )
        if not buf:
            return {}
        return decode_failed_policies(buf.getvalue())

    def delete_partial(self, bucket: str, partial_key: str) -> None:
        """Remove all objects under the scan partial prefix (shards, meta, failed sidecar)."""
        prefix = partial_key if partial_key.endswith('/') else partial_key + '/'
        keys = list(self._s3.list_dir(bucket_name=bucket, key=prefix))
        if not keys:
            return
        for chunk in batches(keys, 100):
            resp = self._s3.client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [{'Key': k} for k in chunk], 'Quiet': True},
            )
            for err in resp.get('Errors') or ():
                _LOG.error(
                    'S3 delete_objects error for %s: %s — %s',
                    err.get('Key'),
                    err.get('Code'),
                    err.get('Message'),
                )
