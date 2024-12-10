from http import HTTPStatus

from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import HTTPMethod, JobState, ReportFormat, \
    CustodianEndpoint
from helpers.lambda_response import build_response
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJobService, AmbiguousJob
from services.platform_service import PlatformService
from services.report_convertors import ShardsCollectionDigestConvertor
from services.report_service import ReportService, ReportResponse
from services.sharding import ShardsCollection
from services.metadata import Metadata
from services.license_service import LicenseService
from validators.swagger_request_models import JobDigestReportGetModel, \
    TenantJobsDigestsReportGetModel
from validators.utils import validate_kwargs


class DigestReportHandler(AbstractHandler):
    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 report_service: ReportService,
                 tenant_service: TenantService,
                 platform_service: PlatformService,
                 license_service: LicenseService):
        self._ambiguous_job_service = ambiguous_job_service
        self._rs = report_service
        self._ts = tenant_service
        self._platform_service = platform_service
        self._ls = license_service

    @classmethod
    def build(cls) -> 'DigestReportHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
            platform_service=SP.platform_service,
            license_service=SP.license_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_DIGESTS_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            },
            CustodianEndpoint.REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_by_tenant_jobs
            }
        }

    @validate_kwargs
    def get_by_job(self, event: JobDigestReportGetModel, job_id: str):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id,
            typ=event.job_type,
            customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found',
                code=HTTPStatus.NOT_FOUND
            )

        if job.is_platform_job:
            platform = self._platform_service.get_nullable(job.platform_id)
            if not platform:
                return build_response(
                    content='Job platform not found',
                    code=HTTPStatus.NOT_FOUND
                )
            collection = self._rs.platform_job_collection(platform, job.job)
        else:
            tenant = self._ts.get(job.tenant_name)
            tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)
            collection = self._rs.ambiguous_job_collection(tenant, job)
        metadata = self._ls.get_customer_metadata(job.customer_name)
        return build_response(
            content=self._collection_response(job, collection, metadata)
        )

    @staticmethod
    def _collection_response(job: AmbiguousJob, collection: ShardsCollection,
                             metadata: Metadata
                             ) -> dict:
        """
        Builds response for the given collection
        """
        collection.fetch_all()

        report = ShardsCollectionDigestConvertor(metadata).convert(collection)
        return ReportResponse(job, report, fmt=ReportFormat.JSON).dict()

    @validate_kwargs
    def get_by_tenant_jobs(self, event: TenantJobsDigestsReportGetModel,
                           tenant_name: str):
        tenant = self._ts.get(tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        jobs = self._ambiguous_job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            job_type=event.job_type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso
        )
        jobs = filter(lambda x: not x.is_platform_job,
                      self._ambiguous_job_service.to_ambiguous(jobs))
        metadata = self._ls.get_customer_metadata(tenant.customer_name)

        job_collection = []
        for job in jobs:
            col = self._rs.ambiguous_job_collection(tenant, job)
            job_collection.append((job, col))
        # TODO _collection_response to threads?
        return build_response(content=map(
            lambda pair: self._collection_response(*(pair + (metadata, ))),
            job_collection
        ))
