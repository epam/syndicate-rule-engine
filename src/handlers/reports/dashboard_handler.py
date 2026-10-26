from http import HTTPStatus
from typing import Self

from modular_sdk.models.tenant import Tenant
from modular_sdk.services.tenant_service import TenantService

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod, ReportFormat
from helpers.lambda_response import ResponseFactory, build_response
from models.job import Job
from services import SP, modular_helpers
from services.dashboard_report_service import (
    DashboardReportError,
    DashboardReportService,
)
from services.job_service import JobService
from services.report_service import ReportResponse, ReportService
from validators.swagger_request_models import DashboardReportGetModel
from validators.utils import validate_kwargs


class DashboardReportHandler(AbstractHandler):
    def __init__(
        self,
        dashboard_service: DashboardReportService,
        tenant_service: TenantService,
        job_service: JobService,
        report_service: ReportService,
    ):
        self._dashboard_service = dashboard_service
        self._tenant_service = tenant_service
        self._job_service = job_service
        self._report_service = report_service

    @classmethod
    def build(cls) -> Self:
        return cls(
            dashboard_service=SP.dashboard_report_service,
            tenant_service=SP.modular_client.tenant_service(),
            job_service=SP.job_service,
            report_service=SP.report_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_DASHBOARD_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            },
            Endpoint.REPORTS_DASHBOARD_TENANTS_TENANT_NAME: {
                HTTPMethod.GET: self.get_by_tenant
            },
        }

    def _build_dashboard(self, job: Job, tenant: Tenant):
        try:
            dashboard = self._dashboard_service.generate(job, tenant)

            file_url = self._report_service.one_time_url(
                dashboard, f'{tenant.name.lower()}_dashboard_{job.id}.xlsx'
            )
            response = ReportResponse(job, file_url, fmt=ReportFormat.XLSX)

            return build_response(content=response.dict())

        except DashboardReportError as error:
            raise (
                ResponseFactory(HTTPStatus(error.status_code))
                .message(str(error))
                .exc()
            )

    @validate_kwargs
    def get_by_job(
        self,
        event: DashboardReportGetModel,
        job_id: str,
        job_obj: Job,
    ):
        tenant = modular_helpers.assert_tenant_valid(
            self._tenant_service.get(job_obj.tenant_name),
            event.customer_id,
        )
        return self._build_dashboard(job_obj, tenant)

    @validate_kwargs
    def get_by_tenant(
        self,
        event: DashboardReportGetModel,
        tenant_name: str,
    ):
        tenant = modular_helpers.assert_tenant_valid(
            self._tenant_service.get(tenant_name),
            event.customer_id,
        )
        job = self._job_service.get_latest_succeeded_job(
            tenant.name, tenant.customer_name
        )
        if not job:
            raise (
                ResponseFactory(HTTPStatus.NOT_FOUND)
                .message(
                    f'No successfully completed scan job exists for tenant '
                    f'{tenant.name!r}'
                )
                .exc()
            )
        return self._build_dashboard(job, tenant)
