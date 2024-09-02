import statistics
from datetime import datetime
from itertools import chain
from typing import TypedDict, Generator, BinaryIO, cast

import msgspec
from modular_sdk.models.tenant import Tenant

from helpers.constants import Cloud, ReportFormat, PolicyErrorType
from helpers.log_helper import get_logger
from models.batch_results import BatchResults
from models.job import Job
from services import cache
from services.ambiguous_job_service import AmbiguousJob
from services.clients.s3 import S3Client, Json
from services.environment_service import EnvironmentService
from services.mappings_collector import LazyLoadedMappingsCollector
from services.platform_service import Platform
from services.reports_bucket import (TenantReportsBucketKeysBuilder, \
                                     PlatformReportsBucketKeysBuilder,
                                     StatisticsBucketKeysBuilder,
                                     ReportsBucketKeysBuilder)
from services.sharding import ShardsCollection, ShardsCollectionFactory, \
    ShardsS3IO

_LOG = get_logger(__name__)


class StatisticsItem(TypedDict, total=False):
    policy: str
    region: str
    tenant_name: str
    customer_name: str
    start_time: float
    end_time: float
    api_calls: dict

    scanned_resources: int | None
    failed_resources: int | None
    reason: str | None
    traceback: list[str]
    error_type: PolicyErrorType | None


class AverageStatisticsItem(TypedDict, total=False):
    policy: str
    invocations: int
    succeeded_invocations: int
    failed_invocations: int
    total_api_calls: dict
    min_exec: float
    max_exec: float
    total_exec: float
    average_exec: float
    resources_failed: int
    resources_scanned: int
    average_resources_scanned: int
    average_resources_failed: int


class ReportResponse:
    __slots__ = ('entity', 'content', 'fmt', 'dictionary_url')

    def __init__(self, entity: AmbiguousJob | Tenant | Platform,
                 content: Json | None = None,
                 dictionary_url: str | None = None,
                 fmt: ReportFormat = ReportFormat.JSON):
        self.entity = entity
        self.content = content or {}
        self.dictionary_url = dictionary_url
        self.fmt = fmt

    def dict(self) -> dict:
        res = {'format': self.fmt, 'obfuscated': bool(self.dictionary_url)}
        if self.dictionary_url:
            res['dictionary_url'] = self.dictionary_url
        if isinstance(self.content, str):
            res['url'] = self.content
        else:
            res['content'] = self.content
        if isinstance(self.entity, AmbiguousJob):
            res['job_id'] = self.entity.id
            res['job_type'] = self.entity.type
            res['tenant_name'] = self.entity.tenant_name
            res['customer_name'] = self.entity.customer_name
        elif isinstance(self.entity, Platform):
            res['platform_id'] = self.entity.id
            res['tenant_name'] = self.entity.tenant_name
            res['customer_name'] = self.entity.customer
        else:
            res['tenant_name'] = self.entity.name
            res['customer_name'] = self.entity.customer_name
        return res


class ReportService:
    def __init__(self, s3_client: S3Client,
                 environment_service: EnvironmentService,
                 mappings_collector: LazyLoadedMappingsCollector):
        self.s3_client = s3_client
        self.environment_service = environment_service
        self.mappings_collector = mappings_collector

        self._ipv4_cache = cache.TTLCache(maxsize=2, ttl=300)

    def job_collection(self, tenant: Tenant, job: Job) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).job_result(job),
            client=self.s3_client
        )
        return collection

    def ed_job_collection(self, tenant: Tenant, br: BatchResults
                          ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).ed_job_result(br),
            client=self.s3_client
        )
        return collection

    def ambiguous_job_collection(self, tenant: Tenant, job: AmbiguousJob
                                 ) -> ShardsCollection:
        if not job.is_ed_job:
            return self.job_collection(tenant, job.job)
        return self.ed_job_collection(tenant, job.job)

    def ed_job_difference_collection(self, tenant: Tenant, br: BatchResults
                                     ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).ed_job_difference(br),
            client=self.s3_client
        )
        return collection

    def tenant_latest_collection(self, tenant: Tenant) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).latest_key(),
            client=self.s3_client
        )
        return collection

    def platform_latest_collection(self, platform: Platform
                                   ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=PlatformReportsBucketKeysBuilder(platform).latest_key(),
            client=self.s3_client
        )
        return collection

    def platform_job_collection(self, platform: Platform, job: Job
                                ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=PlatformReportsBucketKeysBuilder(platform).job_result(job),
            client=self.s3_client
        )
        return collection

    def tenant_snapshot_collection(self, tenant: Tenant,
                                   date: datetime) -> ShardsCollection | None:
        key = TenantReportsBucketKeysBuilder(tenant).nearest_snapshot_key(date)
        if not key:
            return
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            client=self.s3_client
        )
        return collection

    def platform_snapshot_collection(self, platform: Platform, date: datetime
                                     ) -> ShardsCollection | None:
        key = PlatformReportsBucketKeysBuilder(platform).nearest_snapshot_key(
            date
        )
        if not key:
            return
        collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            client=self.s3_client
        )
        return collection

    def fetch_meta(self, tp: Tenant | Platform) -> dict:
        if isinstance(tp, Tenant):
            collection = self.tenant_latest_collection(tp)
        else:
            collection = self.platform_latest_collection(tp)
        collection.fetch_meta()
        return collection.meta or {}

    def job_statistics(self, job: Job | BatchResults) -> list[StatisticsItem]:
        data = self.s3_client.gz_get_json(
            bucket=self.environment_service.get_statistics_bucket_name(),
            key=StatisticsBucketKeysBuilder.job_statistics(job)
        )
        if not data:
            return []
        return data

    @staticmethod
    def average_statistics(*iterables: list[StatisticsItem]
                           ) -> Generator[dict, None, None]:
        remapped = {}  # (policy region) to items
        for i in chain(*iterables):
            remapped.setdefault((i['policy'], i['region']), []).append(i)
        for key, items in remapped.items():
            total_api_calls = {}
            executions = []
            failed_invocations = 0
            scanned, failed = [], []
            for item in items:
                for k, v in (item.get('api_calls') or {}).items():
                    if k not in total_api_calls:
                        total_api_calls[k] = v
                    else:
                        total_api_calls[k] += v
                executions.append(item['end_time'] - item['start_time'])
                if item.get('scanned_resources'):
                    scanned.append(item['scanned_resources'])
                if item.get('failed_resources'):
                    failed.append(item['failed_resources'])
                if item.get('error_type'):
                    failed_invocations += 1
            scanned = scanned or [0]
            failed = failed or [0]
            yield {
                'policy': key[0],
                'region': key[1],
                'invocations': len(items),
                'succeeded_invocations': len(items) - failed_invocations,
                'failed_invocations': failed_invocations,
                'total_api_calls': total_api_calls,
                'min_exec': min(executions),
                'max_exec': max(executions),
                'total_exec': sum(executions),
                'average_exec': statistics.mean(executions),
                'resources_failed': sum(failed),
                'resources_scanned': sum(scanned),
                'average_resources_scanned': statistics.mean(scanned),
                'average_resources_failed': statistics.mean(failed),
            }

    @staticmethod
    def sum_average_statistics(iterables: list[AverageStatisticsItem]
                               ) -> list[AverageStatisticsItem]:
        result = {}
        for item in iterables:
            for k, v in item.items():
                if k == 'policy':
                    result.setdefault(item['policy'], {})
                elif k == 'region':
                    continue
                elif k == 'total_api_calls':
                    for k_api, v_api in v.items():
                        result[item['policy']].setdefault(k, {}).setdefault(
                            k_api, 0)
                        result[item['policy']][k][k_api] += v_api
                else:
                    result[item['policy']].setdefault(k, 0)
                    result[item['policy']][k] += v

        return [{'policy': k, **v} for k, v in result.items()]

    @staticmethod
    def format_statistic(item: StatisticsItem) -> dict:
        item = cast(dict, item)
        item.pop('tenant_name', None)
        item.pop('customer_name', None)
        item.pop('traceback', None)
        item.pop('reason', None)
        item['succeeded'] = not bool(item.get('error_type'))
        item['execution_time'] = item['end_time'] - item['start_time']
        item.pop('start_time', None)
        item.pop('end_time', None)
        return item

    @staticmethod
    def format_failed(item: StatisticsItem) -> StatisticsItem:
        """
        Changes the given item. Does not create new one
        :param item:
        :return:
        """
        item.pop('tenant_name', None)
        item.pop('customer_name', None)
        item.pop('start_time', None)
        item.pop('end_time', None)
        item.pop('api_calls', None)
        item.pop('scanned_resources', None)
        item.pop('failed_resources', None)
        item.pop('traceback', None)
        return item

    @staticmethod
    def only_failed(statistic: list[StatisticsItem],
                    error_type: PolicyErrorType = None) -> filter:
        """
        Keeps only failed statistics items
        :param statistic:
        :param error_type:
        :return:
        """

        def check(item):
            et = item.get('error_type')
            if not et:
                return False
            if error_type and et != error_type:
                return False
            return True

        return filter(check, statistic)

    def one_time_url(self, buffer: BinaryIO, filename: str) -> str:
        """
        Can be used to generate one time presigned urls. Such files will be
        placed to a temp directory in S3 where they will be removed by
        lifecycle policies. These files a basically temp files
        :param buffer:
        :param filename: desired filename for the file that will be downloaded
        :return:
        """
        key = ReportsBucketKeysBuilder.one_time_on_demand()
        self.s3_client.gz_put_object(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            body=buffer
        )
        return self.s3_client.prepare_presigned_url(self.s3_client.gz_download_url(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            filename=filename
        ))

    def one_time_url_json(self, obj: Json, filename: str) -> str:
        """
        The same as the one above, but returns one time url for json object
        :param obj:
        :param filename:
        :return:
        """
        key = ReportsBucketKeysBuilder.one_time_on_demand()
        self.s3_client.gz_put_object(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            body=msgspec.json.encode(obj),
            content_type='application/json'
        )
        return self.s3_client.prepare_presigned_url(self.s3_client.gz_download_url(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            filename=filename
        ))
