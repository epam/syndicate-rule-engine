"""S3 object keys for policy filter bundles under a job prefix."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.job import Job

    from services.reports_bucket import ReportsBucketKeysBuilder


class JobPolicyFiltersKeysBuilder:
    """
    Builds S3 object keys for the per-job policy-filters bundle (gzipped JSON).

    raw/EPAM Systems/AWS/31231231231/jobs/standard/2023-12-10-14/b00649c9-2657-4ade-bd6b-f0f5924f6a50/filters/  # noqa
    """

    FILTERS_DIR = 'filters/'
    BUNDLE_FILENAME = 'bundle.json'

    def __init__(self, builder: ReportsBucketKeysBuilder) -> None:
        self._builder = builder

    def job_filters(self, job: Job) -> str:
        base_job = self._builder.base_job(job)
        return self._builder.urljoin(base_job, self.FILTERS_DIR)

    def job_filters_bundle(self, job: Job) -> str:
        return self._builder.urljoin(
            self.job_filters(job).rstrip('/'),
            self.BUNDLE_FILENAME,
        ).rstrip('/')
