"""Persist and load per-rule event payloads for reactive jobs."""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import TYPE_CHECKING, Any, overload

from typing_extensions import Self

from helpers.constants import Env
from helpers.log_helper import get_logger
from models.event import EventRecordAttribute
from services.event_driven.events.keys import JobEventsKeysBuilder
from services.metadata import DEFAULT_VERSION, Metadata
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant

    from models.job import Job
    from services.clients.s3 import S3Client
    from services.license_service import License
    from services.platform_service import Platform


_LOG = get_logger(__name__)


def _event_record_to_dict(
    event_record: EventRecordAttribute,
) -> dict:
    return event_record.as_dict()


class JobEventsService:
    def __init__(self, s3_client: S3Client) -> None:
        self._s3_client = s3_client

    @classmethod
    def build(cls) -> Self:
        from services import SP

        return cls(s3_client=SP.s3)

    @overload
    def save(
        self,
        *,
        tenant: Tenant,
        job: Job,
        events_by_rule: dict[str, list[EventRecordAttribute]],
        license: License,
        metadata: Metadata | None = None,
    ) -> None: ...

    @overload
    def save(
        self,
        *,
        platform: Platform,
        job: Job,
        events_by_rule: dict[str, list[EventRecordAttribute]],
        license: License,
        metadata: Metadata | None = None,
    ) -> None: ...

    def save(
        self,
        *,
        tenant: Tenant | None = None,
        platform: Platform | None = None,
        job: Job,
        events_by_rule: dict[str, list[EventRecordAttribute]],
        license: License,
        metadata: Metadata | None = None,
    ) -> None:
        if not events_by_rule:
            return
        if metadata is None:
            from services import SP

            metadata = SP.metadata_provider.get(
                license, version=DEFAULT_VERSION
            )

        rules_to_scan = set(job.rules_to_scan)
        scoped: dict[str, list[EventRecordAttribute]] = {}
        for rule_name, records in events_by_rule.items():
            if rule_name not in rules_to_scan or not records:
                continue
            scoped[rule_name] = _dedupe_records(records)
        if not scoped:
            _LOG.info(
                'No job events to persist for job %s (empty events_by_rule '
                'or no overlap with rules_to_scan)',
                job.id,
            )
            return

        keys = self._keys_builder(tenant, platform)
        bucket = Env.REPORTS_BUCKET_NAME.as_str()
        manifest: dict[str, str] = {}
        for rule_name, records in scoped.items():
            rule_key = keys.job_events_rule(job, rule_name)
            _LOG.debug(
                f'Event records for rule {rule_name} for'
                f' job {job.id} saved to {rule_key}',
                extra={
                    'job_id': job.id,
                    'rule_name': rule_name,
                    'key': rule_key,
                },
            )
            self._s3_client.gz_put_json(
                bucket=bucket,
                key=rule_key,
                obj=[_event_record_to_dict(r) for r in records],
            )
            manifest[rule_name] = rule_key

        key = keys.job_events_manifest(job)
        _LOG.info(
            f'Events manifest saved for job {job.id} to {key}',
            extra={
                'job_id': job.id,
                'key': key,
            },
        )
        self._s3_client.gz_put_json(
            bucket=bucket,
            key=key,
            obj=manifest,
        )

    @overload
    def load_all_rule_events(
        self,
        *,
        tenant: Tenant,
        job: Job,
    ) -> dict[str, list[dict]]: ...

    @overload
    def load_all_rule_events(
        self,
        *,
        platform: Platform,
        job: Job,
    ) -> dict[str, list[dict]]: ...

    def load_all_rule_events(
        self,
        *,
        tenant: Tenant | None = None,
        platform: Platform | None = None,
        job: Job,
    ) -> dict[str, list[dict]]:
        keys = self._keys_builder(tenant, platform)
        bucket = Env.REPORTS_BUCKET_NAME.as_str()
        manifest = self._s3_client.gz_get_json(
            bucket=bucket,
            key=keys.job_events_manifest(job),
        )
        if not isinstance(manifest, dict):
            return {}

        result: dict[str, list[dict]] = {}
        for rule_name, rule_key in manifest.items():
            if not isinstance(rule_name, str) or not isinstance(rule_key, str):
                continue
            data = self._s3_client.gz_get_json(bucket=bucket, key=rule_key)
            if isinstance(data, list):
                result[rule_name] = data
        return result

    def _keys_builder(
        self,
        tenant: Tenant | None,
        platform: Platform | None,
    ) -> JobEventsKeysBuilder:
        if tenant is not None:
            return JobEventsKeysBuilder(TenantReportsBucketKeysBuilder(tenant))
        if platform is not None:
            return JobEventsKeysBuilder(
                PlatformReportsBucketKeysBuilder(platform)
            )
        raise ValueError('Either tenant or platform must be provided')


def _event_record_digest(event_record: EventRecordAttribute) -> str:
    payload = _to_json_compatible(event_record)
    digest_source = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(digest_source.encode('utf-8')).hexdigest()


def _to_json_compatible(value: Any) -> Any:
    if isinstance(value, EventRecordAttribute):
        return _to_json_compatible(value.as_dict())
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(v) for v in value]
    return value


def _dedupe_records(
    records: list[EventRecordAttribute],
) -> list[EventRecordAttribute]:
    seen: set[str] = set()
    out: list[EventRecordAttribute] = []
    for rec in records:
        digest = _event_record_digest(rec)
        if digest in seen:
            continue
        seen.add(digest)
        out.append(rec)
    return out
