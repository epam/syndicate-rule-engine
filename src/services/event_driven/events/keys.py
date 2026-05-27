"""S3 object keys for per-job reactive event payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.job import Job

    from services.reports_bucket import ReportsBucketKeysBuilder


class JobEventsKeysBuilder:
    """Keys under ``{base_job}/events/``."""

    EVENTS_DIR = 'events/'
    MANIFEST_FILENAME = 'manifest.json'

    def __init__(self, builder: ReportsBucketKeysBuilder) -> None:
        self._builder = builder

    def job_events(self, job: Job) -> str:
        return self._builder.urljoin(
            self._builder.base_job(job),
            self.EVENTS_DIR,
        )

    def job_events_manifest(self, job: Job) -> str:
        return self._builder.urljoin(
            self.job_events(job).rstrip('/'),
            self.MANIFEST_FILENAME,
        ).rstrip('/')

    def job_events_rule(self, job: Job, rule_name: str) -> str:
        return self._builder.urljoin(
            self.job_events(job).rstrip('/'),
            f'{rule_name}.json.gz',
        ).rstrip('/')
