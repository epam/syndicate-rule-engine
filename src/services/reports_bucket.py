from __future__ import annotations

import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Optional

from helpers import Version, urljoin
from helpers.constants import COMPOUND_KEYS_SEPARATOR, Cloud, JobType
from helpers.time_helper import utc_datetime, week_number
from models.job import Job
from models.metrics import ReportMetrics
from services import SP
from services.clients.s3 import S3Client


if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant

    from services.platform_service import Platform


class ReportsBucketKeysBuilder(ABC):
    """
    Paths must look like this:
    raw/EPAM Systems/AWS/31231231231/latest/0.json.gz
    raw/EPAM Systems/AWS/31231231231/latest/1.json.gz

    raw/EPAM Systems/AWS/31231231231/snapshots/2023-12-10-14/

    raw/EPAM Systems/AWS/31231231231/jobs/standard/2023-12-10-14/b00649c9-2657-4ade-bd6b-f0f5924f6a50/result/  #  noqa

    raw/EPAM Systems/AWS/31231231231/jobs/event-driven/2023-12-10-14/b00649c9-2657-4ade-bd6b-f0f5924f6a50/result/  # noqa
    raw/EPAM Systems/AWS/31231231231/jobs/event-driven/2023-12-10-14/b00649c9-2657-4ade-bd6b-f0f5924f6a50/difference/  # noqa
    """

    date_delimiter = '-'

    prefix = 'raw/'
    on_demand = 'on-demand/'  # any on-flight generated reports
    snapshots = 'snapshots/'
    latest = 'latest/'
    jobs = 'jobs/'
    standard = 'standard/'
    ed = 'event-driven/'
    result = 'result/'
    difference = 'difference/'

    @staticmethod
    def urljoin(*args) -> str:
        return urljoin(*args) + '/'  # delimiter

    @staticmethod
    def datetime(_from: datetime | None = None) -> str:
        """
        Builds datetime part of a path with 1 hour precision in UTC.
        By default, uses the current datetime.
        '2023-11-02-09/'
        :return:
        """
        _from = _from or utc_datetime()
        _from = _from.astimezone(timezone.utc)  # just in case
        return _from.strftime(
            ReportsBucketKeysBuilder.date_delimiter.join(
                ('%Y', '%m', '%d', '%H')
            )
            + '/'
        )

    @property
    @abstractmethod
    def cloud(self) -> Cloud:
        """
        :return:
        """

    @abstractmethod
    def job_result(self, job: 'Job') -> str:
        """
        Builds s3 key for a concrete job
        :param job:
        :return:
        """

    @abstractmethod
    def latest_key(self) -> str:
        """
        Builds s3 key to the latest state
        :return:
        """

    @abstractmethod
    def snapshots_folder(self) -> str:
        """
        Returns a path to a folder with snapshots
        """

    def snapshot_key(self, date: datetime) -> str:
        """
        Returns a path to snapshot for the given date. You can definitely
        use this key to write files. But if you want to read the data you
        should get a key with the nearest older date
        :param date:
        :return:
        """
        return self.urljoin(self.snapshots_folder(), self.datetime(date))

    def nearest_snapshot_key(self, date: datetime) -> str | None:
        """
        Returns the nearest to given date existing snapshot key
        """
        # todo can be cached
        prefixes = SP.s3.common_prefixes(
            bucket=SP.environment_service.default_reports_bucket_name(),
            delimiter='/',
            prefix=self.snapshots_folder(),
        )
        to_check = self.urljoin(self.snapshots_folder(), self.datetime(date))
        lower = None
        for prefix in prefixes:
            if prefix <= to_check:
                lower = prefix
            elif not lower:
                lower = prefix
                break
            else:
                break
        return lower

    @staticmethod
    def _random_filename() -> str:
        """
        Each time returns a random name for a file
        :return:
        """
        with tempfile.NamedTemporaryFile() as file:
            return PurePosixPath(file.name).name

    @classmethod
    def one_time_on_demand(cls) -> str:
        """
        Generates random one time
        :return:
        """
        return cls.on_demand + cls._random_filename()


class TenantReportsBucketKeysBuilder(ReportsBucketKeysBuilder):
    def __init__(self, tenant: 'Tenant'):
        self._tenant = tenant

    @property
    def cloud(self) -> Cloud:
        """
        Only AWS|AZURE|GOOGLE currently
        :return:
        """
        return Cloud[self._tenant.cloud.upper()]

    def job_result(self, job: Job) -> str:
        if job.tenant_name != self._tenant.name:
            raise ValueError(
                f"Job tenant must be {self._tenant.name!r}, "
                f"got {job.tenant_name!r}"
            )

        if job.job_type == JobType.REACTIVE:
            prefix = self.ed
        else:
            prefix = self.standard

        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.jobs,
            prefix,
            self.datetime(utc_datetime(job.submitted_at)),
            job.id,
            self.result,
        )

    def latest_key(self) -> str:
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.latest,
        )

    def snapshots_folder(self) -> str:
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.snapshots,
        )


class PlatformReportsBucketKeysBuilder(ReportsBucketKeysBuilder):
    def __init__(self, platform: 'Platform'):
        self._platform = platform

    @property
    def cloud(self) -> Cloud:
        """
        Currently, the only platform that we support is KUBERNETES
        """
        return Cloud.KUBERNETES

    def job_result(self, job: 'Job') -> str:
        assert job.platform_id == self._platform.id, (
            f'Job platform must be {self._platform.id}'
        )

        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.jobs,
            self.standard,
            self.datetime(utc_datetime(job.submitted_at)),
            job.id,
        )

    def latest_key(self) -> str:
        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.latest,
        )

    def snapshots_folder(self) -> str:
        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.snapshots,
        )


class StatisticsBucketKeysBuilder:
    _statistics = 'job-statistics/'
    _standard = 'standard/'
    _ed = 'event-driven/'
    _statistics_file = 'statistics.json'
    _diagnostic_report_file = 'diagnostic_report.json'
    _report_statistics = 'report-statistics/'
    _tenant_statistics = 'tenant-statistics/'
    _rules = 'rules/'
    _diagnostic = 'diagnostic/'

    @classmethod
    def job_statistics(cls, job: Job) -> str:
        if job.job_type == JobType.REACTIVE:
            return urljoin(cls._statistics, cls._ed, job.id, cls._statistics_file)
        return urljoin(cls._statistics, cls._standard, job.id, cls._statistics_file)

    @classmethod
    def report_statistics(cls, now: datetime, customer: str) -> str:
        return urljoin(
            cls._report_statistics,
            cls._diagnostic,
            customer,
            now.strftime(
                ReportsBucketKeysBuilder.date_delimiter.join(('%Y', '%m'))
                + '/'
            ),
            cls._diagnostic_report_file,
        )

    @classmethod
    def tenant_statistics(
        cls,
        now: datetime,
        tenant: Optional['Tenant'] = None,
        customer: Optional[str] = None,
    ) -> str:
        if customer:
            return urljoin(
                cls._tenant_statistics,
                cls._rules,
                customer,
                now.strftime(
                    ReportsBucketKeysBuilder.date_delimiter.join(('%Y', '%m'))
                    + '/'
                ),
            )
        elif tenant:
            return urljoin(
                cls._tenant_statistics,
                cls._rules,
                tenant.customer_name,
                now.strftime(
                    ReportsBucketKeysBuilder.date_delimiter.join(('%Y', '%m'))
                    + '/'
                ),
                tenant.cloud,
                tenant.project,
                str(week_number(now)) + '.json',
            )
        return urljoin(cls._tenant_statistics, cls._rules)

    @classmethod
    def xray_log(cls, job_id: str) -> str:
        now = utc_datetime()
        return urljoin(
            'xray', 'executor', now.year, now.month, now.day, f'{job_id}.log'
        )


class ReportMetricsBucketKeysBuilder:
    __slots__ = ()

    date_delimiter = '-'
    prefix = 'metrics/'
    data = 'data'

    @staticmethod
    def datetime(end: datetime) -> str:
        """
        Builds datetime part of a path
        :return:
        """
        end = end.astimezone(timezone.utc)  # just in case
        return end.strftime(
            ReportMetricsBucketKeysBuilder.date_delimiter.join(
                ('%Y', '%m', '%d', '%H', '%M', '%S', '%f')
            )
        )

    @classmethod
    def metrics_key(cls, item: ReportMetrics) -> str:
        # first two are always type and customer and always exist
        type_, customer, *other = item.key.split(COMPOUND_KEYS_SEPARATOR)

        return S3Client.safe_key(
            urljoin(
                cls.prefix,
                customer,
                type_,
                *filter(None, other),
                cls.datetime(utc_datetime(item.end)),
                cls.data,
            )
        )


class ReportMetaBucketsKeys:
    __slots__ = ()

    prefix = 'meta/'
    data = 'data.gz'

    @classmethod
    def meta_key(cls, license_key: str, version: Version) -> str:
        return urljoin(cls.prefix, license_key, version.to_str(), cls.data)


class RulesetsBucketKeys:
    __slots__ = ()
    licensed = 'licensed/'
    standard = 'standard/'
    events = 'events/'
    json_suffix = '.json.gz'
    data = 'data.gz'

    @classmethod
    def licensed_ruleset_key(cls, name: str, version: str) -> str:
        return S3Client.safe_key(
            urljoin(cls.licensed, name, version, cls.data)
        )

    @classmethod
    def licensed_event_mapping_key(
        cls,
        name: str,
        version: str,
        cloud: Cloud | str,
    ) -> str:
        if isinstance(cloud, Cloud):
                cloud = cloud.value
        file_name = cloud + cls.json_suffix
        return S3Client.safe_key(
            urljoin(
                cls.licensed,
                name, 
                version,
                cls.events,
                file_name,
            )
        )

    @classmethod
    def standard_ruleset_key(
        cls, customer: str, name: str, version: str
    ) -> str:
        return S3Client.safe_key(
            urljoin(cls.standard, customer, name, version, cls.data)
        )
