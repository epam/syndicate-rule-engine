from http import HTTPStatus
from typing import Any

from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    Cloud,
    Endpoint,
    HTTPMethod,
    JobState,
    ReportFormat,
)
from helpers.lambda_response import LambdaOutput, build_response
from models.job import Job
from services import SP, modular_helpers
from services.job_service import JobService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.modular_helpers import tenant_cloud
from services.platform_service import PlatformService
from services.report_convertors import ShardsCollectionDigestConvertor
from services.report_service import ReportResponse, ReportService
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    JobDigestReportGetModel,
    TenantJobsDigestsReportGetModel,
)
from validators.utils import validate_kwargs


class DigestReportHandler(AbstractHandler):
    def __init__(
        self,
        job_service: JobService,
        report_service: ReportService,
        tenant_service: TenantService,
        platform_service: PlatformService,
        license_service: LicenseService,
    ) -> None:
        self._js = job_service
        self._rs = report_service
        self._ts = tenant_service
        self._platform_service = platform_service
        self._ls = license_service

    @classmethod
    def build(cls) -> "DigestReportHandler":
        return cls(
            job_service=SP.job_service,
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
            platform_service=SP.platform_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_DIGESTS_JOBS_JOB_ID: {HTTPMethod.GET: self.get_by_job},
            Endpoint.REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_by_tenant_jobs
            },
        }

    @validate_kwargs
    def get_by_job(self, event: JobDigestReportGetModel, job_id: str):
        job = next(
            self._js.get_by_job_types(
                job_id=job_id,
                job_types=event.job_types,
                customer_name=event.customer,
            ),
            None,
        )
        if not job:
            return build_response(
                content="The request job not found", code=HTTPStatus.NOT_FOUND
            )

        if job.is_platform_job:
            platform = self._platform_service.get_nullable(job.platform_id)
            if not platform:
                return build_response(
                    content="Job platform not found", code=HTTPStatus.NOT_FOUND
                )
            collection = self._rs.platform_job_collection(platform, job)
            collection.meta = self._rs.fetch_meta(platform)
            cloud = Cloud.KUBERNETES
        else:
            tenant = self._ts.get(job.tenant_name)
            tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)
            collection = self._rs.job_collection(tenant, job)
            collection.meta = self._rs.fetch_meta(tenant)
            cloud = tenant_cloud(tenant)
        metadata = self._ls.get_customer_metadata(job.customer_name)
        return build_response(
            content=self._collection_response(job, collection, metadata, cloud)
        )

    @staticmethod
    def _collection_response(
        job: Job, collection: ShardsCollection, metadata: Metadata, cloud: Cloud
    ) -> dict[str, Any]:
        """
        Builds response for the given collection
        """
        collection.fetch_all()

        report = ShardsCollectionDigestConvertor(cloud, metadata).convert(collection)
        return ReportResponse(entity=job, content=report, fmt=ReportFormat.JSON).dict()

    @validate_kwargs
    def get_by_tenant_jobs(
        self, event: TenantJobsDigestsReportGetModel, tenant_name: str
    ) -> LambdaOutput:
        tenant = self._ts.get(tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)
        cloud = tenant_cloud(tenant)

        jobs = self._js.get_by_tenant_name(
            tenant_name=tenant_name,
            job_types=event.job_types,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        jobs = filter(lambda x: not x.is_platform_job, jobs)
        metadata = self._ls.get_customer_metadata(tenant.customer_name)
        meta = self._rs.fetch_meta(tenant)

        job_collection: list[tuple[Job, ShardsCollection]] = []
        for job in jobs:
            col = self._rs.job_collection(tenant, job)
            col.meta = meta
            job_collection.append((job, col))
        return build_response(
            content=map(
                lambda pair: self._collection_response(*(pair + (metadata,)), cloud),
                job_collection,
            )
        )
