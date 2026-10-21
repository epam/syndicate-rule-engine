from http import HTTPStatus
from io import BytesIO
from typing import Any, Literal

from modular_sdk.commons.constants import ParentType
from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    Cloud,
    Endpoint,
    HTTPMethod,
    JobState,
    ReportFormat,
)
from helpers.lambda_response import (
    LambdaOutput,
    ResponseFactory,
    build_response,
)
from models.job import Job
from services import SP, modular_helpers, obfuscation
from services.control_plane_report import (
    ControlPlaneReportError,
    ControlPlaneReportService,
)
from services.job_service import JobService
from services.modular_helpers import tenant_cloud
from services.platform_service import Platform, PlatformService
from services.report_convertors import ShardsCollectionFindingsConvertor
from services.report_service import ReportResponse, ReportService
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    ControlPlaneJobReportGetModel,
    JobFindingsReportGetModel,
    TenantJobsFindingsReportGetModel,
)
from validators.utils import validate_kwargs


class FindingsReportHandler(AbstractHandler):
    def __init__(
        self,
        job_service: JobService,
        report_service: ReportService,
        tenant_service: TenantService,
        platform_service: PlatformService,
        control_plane_report_service: ControlPlaneReportService,
    ) -> None:
        self._job_service = job_service
        self._rs = report_service
        self._ts = tenant_service
        self._platform_service = platform_service
        self._control_plane_report_service = control_plane_report_service

    @classmethod
    def build(cls) -> Self:
        return cls(
            job_service=SP.job_service,
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
            platform_service=SP.platform_service,
            control_plane_report_service=SP.control_plane_report_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_FINDINGS_JOBS_JOB_ID: {HTTPMethod.GET: self.get_by_job},
            Endpoint.REPORTS_FINDINGS_TENANTS_TENANT_NAME_JOBS: {
                HTTPMethod.GET: self.get_by_tenant_jobs
            },
            Endpoint.REPORTS_FINDINGS_JOBS_JOB_ID_CONTROL_PLANE: {
                HTTPMethod.GET: self.get_control_plane_by_job
            },
        }

    @validate_kwargs
    def get_control_plane_by_job(
        self, event: ControlPlaneJobReportGetModel, job_id: str
    ):
        job = self._job_service.get_nullable(job_id)
        if not job:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Job {job_id!r} not found'
            ).exc()

        platform_id = job.platform_id
        if not platform_id:
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                'Job is not a Kubernetes platform job'
            ).exc()

        platform = self._platform_service.get_nullable(platform_id)
        if not platform:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Kubernetes platform {platform_id!r} not found'
            ).exc()

        tenant = self._ts.get(platform.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        if platform.customer != tenant.customer_name:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                f'Kubernetes platform {platform_id!r} not found'
            ).exc()

        collection = self._rs.platform_job_collection(platform, job)
        collection.meta = self._rs.fetch_meta(platform)
        collection.fetch_all()

        report = self._make_control_plane_report(
            job_id=job.id,
            platform=platform,
            tenant_name=tenant.name,
            customer_name=tenant.customer_name,
            collection=collection,
            fmt=event.format,
            obfuscated=event.obfuscated,
            href=event.href,
        )
        return build_response(content=report)

    def _make_control_plane_report(
        self,
        job_id: str,
        platform: Platform,
        tenant_name: str,
        customer_name: str,
        collection: ShardsCollection,
        fmt: Literal['md', 'json'],
        obfuscated: bool,
        href: bool = False,
    ) -> dict[str, Any]:
        dictionary_url = None
        if obfuscated:
            dictionary = obfuscation.get_obfuscation_dictionary(collection)
            if dictionary:
                dictionary_url = self._rs.one_time_url_json(
                    dictionary, filename='dictionary.json'
                )

        try:
            content = self._control_plane_report_service.generate_report(
                collection, customer_name, fmt
            )
        except ControlPlaneReportError as error:
            raise (
                ResponseFactory(HTTPStatus.BAD_REQUEST)
                .message(str(error))
                .exc()
            ) from error
        report = {
            'job_id': job_id,
            'platform_id': platform.id,
            'tenant_name': tenant_name,
            'customer_name': customer_name,
            'format': fmt,
            'obfuscated': obfuscated,
        }
        if href or fmt == 'md':
            filename = f'{job_id}-control-plane-findings.{fmt}'
            if isinstance(content, str):
                report['url'] = self._rs.one_time_url(
                    BytesIO(content.encode('utf-8')), filename
                )
            else:
                report['url'] = self._rs.one_time_url_json(content, filename)
        else:
            report['content'] = content
        if dictionary_url:
            report['dictionary_url'] = dictionary_url
        return report

    @validate_kwargs
    def get_by_job(self, event: JobFindingsReportGetModel, job_id: str):
        job = self._job_service.get_nullable(job_id)
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
        return build_response(
            content=self._collection_response(
                job=job,
                collection=collection,
                cloud=cloud,
                href=event.href,
                obfuscated=event.obfuscated,
            )
        )

    def _collection_response(
        self,
        job: Job,
        collection: ShardsCollection,
        cloud: Cloud,
        href: bool = False,
        obfuscated: bool = False,
    ) -> dict[str, Any]:
        collection.fetch_all()

        dictionary_url = None
        if obfuscated:
            dct = obfuscation.get_obfuscation_dictionary(collection)
            dictionary_url = self._rs.one_time_url_json(dct, filename="dictionary.json")
        report = ShardsCollectionFindingsConvertor(cloud).convert(collection)
        if href:
            return ReportResponse(
                job,
                self._rs.one_time_url_json(report, f"{job.id}-findings.json"),
                dictionary_url,
                ReportFormat.JSON,
            ).dict()
        else:
            return ReportResponse(job, report, dictionary_url, ReportFormat.JSON).dict()

    @validate_kwargs
    def get_by_tenant_jobs(
        self, event: TenantJobsFindingsReportGetModel, tenant_name: str
    ) -> LambdaOutput:
        tenant = self._ts.get(tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)

        jobs = self._job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            job_types=event.job_types,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        jobs = filter(lambda x: not x.is_platform_job, jobs)

        meta = self._rs.fetch_meta(tenant)
        job_collection: list[tuple[Job, ShardsCollection]] = []
        for job in jobs:
            col = self._rs.job_collection(tenant, job)
            col.meta = meta
            job_collection.append((job, col))
        return build_response(
            content=map(
                lambda pair: self._collection_response(
                    *pair,
                    cloud=tenant_cloud(tenant),
                    href=event.href,
                    obfuscated=event.obfuscated,
                ),
                job_collection,
            )
        )
