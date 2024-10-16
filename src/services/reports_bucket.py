import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone, date
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Optional
from dateutil.relativedelta import relativedelta

from helpers import urljoin
from helpers.constants import Cloud
from helpers.time_helper import utc_datetime, week_number
from models.batch_results import BatchResults
from models.job import Job
from services import SP

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
    def datetime(_from: Optional[datetime] = None) -> str:
        """
        Builds datetime part of a path with 1 hour precision in UTC.
        By default, uses the current datetime.
        '2023-11-02-09/'
        :return:
        """
        _from = _from or utc_datetime()
        _from = _from.astimezone(timezone.utc)  # just in case
        return _from.strftime(ReportsBucketKeysBuilder.date_delimiter.join(
            ['%Y', '%m', '%d', '%H']
        ) + '/')

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
    def ed_job_result(self, br: 'BatchResults') -> str:
        """
        Builds s3 key for a concrete ed job
        :param br:
        :return:
        """

    @abstractmethod
    def ed_job_difference(self, br: 'BatchResults') -> str:
        """
        Builds s3 key for a concrete ed job difference
        :param br:
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
        return self.urljoin(
            self.snapshots_folder(),
            self.datetime(date)
        )

    def nearest_snapshot_key(self, date: datetime) -> str | None:
        """
        Returns the nearest to given date existing snapshot key
        """
        prefixes = SP.s3.common_prefixes(
            bucket=SP.environment_service.default_reports_bucket_name(),
            delimiter='/',
            prefix=self.snapshots_folder()
        )
        to_check = self.urljoin(
            self.snapshots_folder(),
            self.datetime(date)
        )
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

    def job_result(self, job: 'Job') -> str:
        assert job.tenant_name == self._tenant.name, \
            f'Job tenant must be {self._tenant.name}'
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.jobs,
            self.standard,
            self.datetime(utc_datetime(job.submitted_at)),
            job.id,
            self.result,
        )

    def ed_job_result(self, br: 'BatchResults') -> str:
        assert br.tenant_name == self._tenant.name, \
            f'Job tenant must be {self._tenant.name}'
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.jobs,
            self.ed,
            self.datetime(utc_datetime(br.submitted_at)),
            br.id,
            self.result,
        )

    def ed_job_difference(self, br: 'BatchResults') -> str:
        assert br.tenant_name == self._tenant.name, \
            f'Job tenant must be {self._tenant.name}'
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.jobs,
            self.ed,
            self.datetime(utc_datetime(br.submitted_at)),
            br.id,
            self.difference,
        )

    def latest_key(self) -> str:
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.latest
        )

    def snapshots_folder(self) -> str:
        return self.urljoin(
            self.prefix,
            self._tenant.customer_name,
            self.cloud.value,
            self._tenant.project,
            self.snapshots
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
        assert job.platform_id == self._platform.id, \
            f'Job platform must be {self._platform.id}'

        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.jobs,
            self.standard,
            self.datetime(utc_datetime(job.submitted_at)),
            job.id
        )

    def ed_job_result(self, br: 'BatchResults') -> str:
        raise NotImplementedError('Event-driven is not available for platform')

    def ed_job_difference(self, br: 'BatchResults') -> str:
        raise NotImplementedError('Event-driven is not available for platform')

    def latest_key(self) -> str:
        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.latest
        )

    def snapshots_folder(self) -> str:
        return self.urljoin(
            self.prefix,
            self._platform.customer,
            self.cloud.value,
            self._platform.platform_id,
            self.snapshots
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
    def job_statistics(cls, job: Job | BatchResults) -> str:
        if isinstance(job, Job):
            return urljoin(
                cls._statistics,
                cls._standard,
                job.id,
                cls._statistics_file
            )
        return urljoin(
            cls._statistics,
            cls._ed,
            job.id,
            cls._statistics_file
        )

    @classmethod
    def report_statistics(cls, now: date, customer: str) -> str:
        return urljoin(
            cls._report_statistics,
            cls._diagnostic,
            customer,
            now.strftime(ReportsBucketKeysBuilder.date_delimiter.join(
                ['%Y', '%m']) + '/'),
            cls._diagnostic_report_file
        )

    @classmethod
    def tenant_statistics(cls, now: date, tenant: Optional['Tenant'] = None,
                          customer: Optional[str] = None) -> str:
        if customer:
            return urljoin(
                cls._tenant_statistics,
                cls._rules,
                customer,
                now.strftime(ReportsBucketKeysBuilder.date_delimiter.join(
                    ['%Y', '%m']) + '/')
            )
        elif tenant:
            return urljoin(
                cls._tenant_statistics,
                cls._rules,
                tenant.customer_name,
                now.strftime(ReportsBucketKeysBuilder.date_delimiter.join(
                    ['%Y', '%m']) + '/'),
                tenant.cloud,
                tenant.project,
                str(week_number(now)) + '.json'
            )
        return urljoin(
            cls._tenant_statistics,
            cls._rules
        )

    @classmethod
    def xray_log(cls, job_id: str) -> str:
        now = utc_datetime()
        return urljoin(
            'xray',
            'executor',
            now.year,
            now.month,
            now.day,
            f'{job_id}.log'
        )


class MetricsBucketKeysBuilder:
    __slots__ = '_tenant',
    _accounts = 'accounts/'
    _tenants = 'tenants/'
    _monthly = 'monthly/'

    def __init__(self, tenant: 'Tenant'):
        self._tenant = tenant

    def account_metrics(self, dt: datetime) -> str:
        return urljoin(
            self._tenant.customer_name,
            self._accounts,
            dt.date().isoformat(),
            f'{self._tenant.project}.json'
        )

    def tenant_metrics(self, dt: datetime) -> str:
        return urljoin(
            self._tenant.customer_name,
            self._tenants,
            dt.date().isoformat(),
            f'{self._tenant.display_name_to_lower.lower()}.json'
        )

    @classmethod
    def list_customer_accounts_metrics_prefix(cls, customer_name: str,
                                              dt: datetime) -> str:
        return urljoin(
            customer_name,
            cls._accounts,
            dt.date().isoformat()
        ) + '/'

    def account_monthly_metrics(self, dt: datetime) -> str:
        """
        Key contains the first day of the next month
        """
        next_m = dt + relativedelta(months=+1, day=1)
        return urljoin(
            self._tenant.customer_name,
            self._accounts,
            self._monthly,
            next_m.date().isoformat(),
            f'{self._tenant.project}.json'
        )

    def tenant_monthly_metrics(self, dt: datetime) -> str:
        """
        Key contains the first day of the next month
        """
        next_m = dt + relativedelta(months=+1, day=1)
        return urljoin(
            self._tenant.customer_name,
            self._tenants,
            self._monthly,
            next_m.date().isoformat(),
            f'{self._tenant.display_name_to_lower.lower()}.json'
        )
