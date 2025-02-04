import statistics
import msgspec
from datetime import datetime
from itertools import chain
from typing import BinaryIO, Generator, TypedDict, Iterable

from modular_sdk.models.tenant import Tenant

from helpers.log_helper import get_logger
from helpers.constants import Cloud, PolicyErrorType, ReportFormat
from helpers.reports import Standard
from models.batch_results import BatchResults
from models.job import Job
from services.ambiguous_job_service import AmbiguousJob
from services.clients.s3 import Json, S3Client
from services.environment_service import EnvironmentService
from services.metadata import Metadata
from services.platform_service import Platform
from services.coverage_service import (
    StandardCoverageCalculator,
    calculate_controls_coverages,
)
from services.reports_bucket import (
    PlatformReportsBucketKeysBuilder,
    ReportsBucketKeysBuilder,
    StatisticsBucketKeysBuilder,
    TenantReportsBucketKeysBuilder,
)
from services.sharding import (
    ShardsCollection,
    ShardsCollectionFactory,
    ShardsS3IO,
    BaseShardPart,
)

_LOG = get_logger(__name__)


class StatisticsItem(msgspec.Struct, kw_only=True, eq=False):
    policy: str
    region: str
    tenant_name: str
    customer_name: str
    start_time: float
    end_time: float
    api_calls: dict = msgspec.field(default_factory=dict)

    scanned_resources: int | None = None
    failed_resources: int | None = None
    reason: str | None = None
    traceback: list[str] = msgspec.field(default_factory=list)
    error_type: PolicyErrorType | None = None

    def is_successful(self) -> bool:
        return self.error_type is None

class AverageStatisticsItem(msgspec.Struct, kw_only=True, eq=False):
    policy: str
    region: str
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


# class AverageStatisticsItem(TypedDict, total=False):
#     policy: str
#     invocations: int
#     succeeded_invocations: int
#     failed_invocations: int
#     total_api_calls: dict
#     min_exec: float
#     max_exec: float
#     total_exec: float
#     average_exec: float
#     resources_failed: int
#     resources_scanned: int
#     average_resources_scanned: int
#     average_resources_failed: int


class ReportResponse:
    __slots__ = ('entity', 'content', 'fmt', 'dictionary_url')

    def __init__(
        self,
        entity: AmbiguousJob | Tenant | Platform,
        content: Json | None = None,
        dictionary_url: str | None = None,
        fmt: ReportFormat = ReportFormat.JSON,
    ):
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
    _job_statistics_decoder = msgspec.json.Decoder(type=list[StatisticsItem])

    def __init__(
        self, s3_client: S3Client, environment_service: EnvironmentService
    ):
        self.s3_client = s3_client
        self.environment_service = environment_service

    def job_collection(self, tenant: Tenant, job: Job) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).job_result(job),
            client=self.s3_client,
        )
        return collection

    def ed_job_collection(
        self, tenant: Tenant, br: BatchResults
    ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).ed_job_result(br),
            client=self.s3_client,
        )
        return collection

    def ambiguous_job_collection(
        self, tenant: Tenant, job: AmbiguousJob
    ) -> ShardsCollection:
        if not job.is_ed_job:
            return self.job_collection(tenant, job.job)
        return self.ed_job_collection(tenant, job.job)

    def ed_job_difference_collection(
        self, tenant: Tenant, br: BatchResults
    ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).ed_job_difference(br),
            client=self.s3_client,
        )
        return collection

    def tenant_latest_collection(self, tenant: Tenant) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=TenantReportsBucketKeysBuilder(tenant).latest_key(),
            client=self.s3_client,
        )
        return collection

    def platform_latest_collection(
        self, platform: Platform
    ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=PlatformReportsBucketKeysBuilder(platform).latest_key(),
            client=self.s3_client,
        )
        return collection

    def platform_job_collection(
        self, platform: Platform, job: Job
    ) -> ShardsCollection:
        collection = ShardsCollectionFactory.from_cloud(Cloud.KUBERNETES)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=PlatformReportsBucketKeysBuilder(platform).job_result(job),
            client=self.s3_client,
        )
        return collection

    def tenant_snapshot_collection(
        self, tenant: Tenant, date: datetime
    ) -> ShardsCollection | None:
        key = TenantReportsBucketKeysBuilder(tenant).nearest_snapshot_key(date)
        if not key:
            return
        collection = ShardsCollectionFactory.from_tenant(tenant)
        collection.io = ShardsS3IO(
            bucket=self.environment_service.default_reports_bucket_name(),
            key=key,
            client=self.s3_client,
        )
        return collection

    def platform_snapshot_collection(
        self, platform: Platform, date: datetime
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
            client=self.s3_client,
        )
        return collection

    def fetch_meta(self, tp: Tenant | Platform) -> dict:
        if isinstance(tp, Tenant):
            collection = self.tenant_latest_collection(tp)
        else:
            collection = self.platform_latest_collection(tp)
        collection.fetch_meta()
        return collection.meta or {}

    def job_statistics(
        self, job: Job | BatchResults | AmbiguousJob
    ) -> list[StatisticsItem]:
        if isinstance(job, AmbiguousJob):
            job = job.job
        data = self.s3_client.gz_get_object(
            bucket=self.environment_service.get_statistics_bucket_name(),
            key=StatisticsBucketKeysBuilder.job_statistics(job),
        )
        if data is None:
            # must never happen for a succeeded job
            return []
        return self._job_statistics_decoder.decode(data.getvalue())

    @staticmethod
    def average_statistics(
        *iterables: Iterable[StatisticsItem],
    ) -> Generator[AverageStatisticsItem, None, None]:
        remapped = {}  # (policy region) to items
        for i in chain(*iterables):
            remapped.setdefault((i.policy, i.region), []).append(i)
        for key, items in remapped.items():
            items: list[StatisticsItem]
            total_api_calls = {}
            executions = []
            failed_invocations = 0
            scanned, failed = [], []
            for item in items:
                for k, v in item.api_calls.items():
                    if k not in total_api_calls:
                        total_api_calls[k] = v
                    else:
                        total_api_calls[k] += v
                executions.append(item.end_time - item.start_time)
                if item.scanned_resources is not None:
                    scanned.append(item.scanned_resources)
                if item.failed_resources is not None:
                    failed.append(item.failed_resources)
                if item.error_type:
                    failed_invocations += 1
            scanned = scanned or [0]
            failed = failed or [0]
            yield AverageStatisticsItem(
                policy=key[0],
                region=key[1],
                invocations=len(items),
                succeeded_invocations=len(items) - failed_invocations,
                failed_invocations=failed_invocations,
                total_api_calls=total_api_calls,
                min_exec=min(executions),
                max_exec=max(executions),
                total_exec=sum(executions),
                average_exec=statistics.mean(executions),
                resources_failed=sum(failed),
                resources_scanned=sum(scanned),
                average_resources_scanned=statistics.mean(scanned),
                average_resources_failed=statistics.mean(failed)
            )

    @staticmethod
    def format_statistics_failed(item: StatisticsItem) -> StatisticsItem:
        """
        Changes the given item. Does not create new one.
        Returns the same one for convenience
        :param item:
        :return:
        """
        item.tenant_name = msgspec.UNSET
        item.customer_name = msgspec.UNSET
        item.start_time = msgspec.UNSET
        item.end_time = msgspec.UNSET
        item.api_calls = msgspec.UNSET
        item.scanned_resources = msgspec.UNSET
        item.failed_resources = msgspec.UNSET
        item.traceback = msgspec.UNSET
        return item

    @staticmethod
    def only_failed(
        statistic: Iterable[StatisticsItem],
        error_type: PolicyErrorType | None = None,
    ) -> Generator[StatisticsItem, None, None]:
        """
        Keeps only failed statistics items
        :param statistic:
        :param error_type:
        :return:
        """
        for item in statistic:
            if not item.error_type:
                continue
            # definitelly failed
            if error_type and item.error_type != error_type:
                continue
            yield item

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
            body=buffer,
        )
        return self.s3_client.prepare_presigned_url(
            self.s3_client.gz_download_url(
                bucket=self.environment_service.default_reports_bucket_name(),
                key=key,
                filename=filename,
            )
        )

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
            content_type='application/json',
        )
        return self.s3_client.prepare_presigned_url(
            self.s3_client.gz_download_url(
                bucket=self.environment_service.default_reports_bucket_name(),
                key=key,
                filename=filename,
            )
        )

    @staticmethod
    def iter_successful_parts(
        col: ShardsCollection,
    ) -> Generator[BaseShardPart, None, None]:
        unsuccessful = set()
        for part in col.iter_parts():
            # TODO: filters here
            if part.resources:
                unsuccessful.add(part.policy)
        for part in col.iter_parts():
            if part.policy not in unsuccessful:
                yield part

    @staticmethod
    def group_parts_iterator_by_location(
        it: Iterable[BaseShardPart],
    ) -> dict[str, list[BaseShardPart]]:
        res = {}
        for item in it:
            res.setdefault(item.location, []).append(item)
        return res

    @staticmethod
    def get_standard_to_controls_to_rules(
        it: Iterable[BaseShardPart], metadata: Metadata
    ) -> dict[Standard, dict[str, int]]:
        res = {}
        checked = set()
        for part in it:
            if part.policy in checked:
                continue
            checked.add(part.policy)

            meta = metadata.rule(part.policy)
            for name in meta.standard:
                for version, controls in meta.standard[name].items():
                    controls_rules = res.setdefault(
                        Standard(name, version), {}
                    )
                    for c in controls:
                        controls_rules.setdefault(c, 0)
                        controls_rules[c] += 1
        return res

    @staticmethod
    def calculate_coverages(
        successful: dict[Standard, dict[str, int]],
        full: dict[Standard, dict[str, int]],
    ) -> dict[Standard, float]:
        res = {}
        for st in successful:
            total_controls = full.get(st)
            if not total_controls:
                _LOG.warning(
                    f'Metadata does not contain controls-rules mapping for {st}'
                )
                continue
            res[st] = (
                StandardCoverageCalculator()
                .update(
                    calculate_controls_coverages(
                        successful[st], total_controls
                    )
                )
                .produce()
            )
        return res

    def calculate_tenant_full_coverage(
        self, col: ShardsCollection, metadata: Metadata, cloud: Cloud
    ) -> dict[Standard, float]:
        return self.calculate_coverages(
            successful=self.get_standard_to_controls_to_rules(
                it=self.iter_successful_parts(col), metadata=metadata
            ),
            full=metadata.domain(cloud).full_cov,
        )

    def calculate_tenant_tech_coverage(
        self, col: ShardsCollection, metadata: Metadata, cloud: Cloud
    ) -> dict[Standard, float]:
        return self.calculate_coverages(
            successful=self.get_standard_to_controls_to_rules(
                it=self.iter_successful_parts(col), metadata=metadata
            ),
            full=metadata.domain(cloud).tech_cov,
        )
