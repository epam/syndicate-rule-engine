from http import HTTPStatus

from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod, JobState, \
    ReportFormat, Cloud
from helpers.lambda_response import build_response
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJob, AmbiguousJobService
from services.platform_service import PlatformService
from services.report_convertors import ShardsCollectionDetailsConvertor
from services.report_service import ReportResponse, ReportService
from services.modular_helpers import tenant_cloud
from services import obfuscation
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    JobDetailsReportGetModel,
    TenantJobsDetailsReportGetModel,
)
from validators.utils import validate_kwargs


class DetailedReportHandler(AbstractHandler):
    def __init__(self, ambiguous_job_service: AmbiguousJobService,
                 report_service: ReportService,
                 tenant_service: TenantService,
                 platform_service: PlatformService):
        self._ambiguous_job_service = ambiguous_job_service
        self._rs = report_service
        self._ts = tenant_service
        self._platform_service = platform_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
            platform_service=SP.platform_service
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_DETAILS_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            },
            Endpoint.REPORTS_DETAILS_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_by_tenant_jobs
            }
        }

    @validate_kwargs
    def get_by_job(self, event: JobDetailsReportGetModel, job_id: str):
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
            collection.meta = self._rs.fetch_meta(platform)
            cloud = Cloud.KUBERNETES
        else:
            tenant = self._ts.get(job.tenant_name)
            modular_helpers.assert_tenant_valid(tenant, event.customer)
            collection = self._rs.ambiguous_job_collection(tenant, job)
            collection.meta = self._rs.fetch_meta(tenant)
            cloud = tenant_cloud(tenant)

        return build_response(
            content=self._collection_response(job, collection, cloud, event.href,
                                              event.obfuscated)
        )

    def _collection_response(self, job: AmbiguousJob,
                             collection: ShardsCollection,
                             cloud: Cloud,
                             href: bool = False,
                             obfuscated: bool = False
                             ) -> dict:
        """
        Builds response for the given collection
        :param collection:
        :param job:
        :param href:
        :return:
        """
        collection.fetch_all()

        dictionary_url = None
        if obfuscated:
            dct = obfuscation.get_obfuscation_dictionary(collection)
            dictionary_url = self._rs.one_time_url_json(dct,
                                                        'dictionary.json')
        report = ShardsCollectionDetailsConvertor(cloud).convert(collection)
        if href:
            return ReportResponse(
                job,
                self._rs.one_time_url_json(report, f'{job.id}.json'),
                dictionary_url,
                ReportFormat.JSON
            ).dict()

        else:
            return ReportResponse(
                job,
                report,
                dictionary_url,
                ReportFormat.JSON
            ).dict()

    @validate_kwargs
    def get_by_tenant_jobs(self, event: TenantJobsDetailsReportGetModel,
                           tenant_name: str):
        tenant = self._ts.get(tenant_name)
        modular_helpers.assert_tenant_valid(tenant, event.customer)

        jobs = self._ambiguous_job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            job_type=event.job_type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso
        )
        jobs = filter(lambda x: not x.is_platform_job,
                      self._ambiguous_job_service.to_ambiguous(jobs))

        meta = self._rs.fetch_meta(tenant)
        job_collection = []
        for job in jobs:
            col = self._rs.ambiguous_job_collection(tenant, job)
            col.meta = meta
            job_collection.append((job, col))
        return build_response(content=map(
            lambda pair: self._collection_response(*pair, tenant_cloud(tenant),
                                                   href=event.href,
                                                   obfuscated=event.obfuscated),
            job_collection
        ))
