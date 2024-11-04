import io
from functools import cached_property
from http import HTTPStatus
from itertools import chain

from modular_sdk.services.tenant_service import TenantService
from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod, ReportFormat
from helpers.lambda_response import build_response
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJobService
from services.coverage_service import CoverageService
from services.environment_service import EnvironmentService
from services.report_service import ReportResponse, ReportService
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter
from validators.swagger_request_models import (
    JobComplianceReportGetModel,
    TenantComplianceReportGetModel,
)
from validators.utils import validate_kwargs


class ComplianceReportXlsxWriter:
    def __init__(self, coverages: dict[str, dict[str, float]]):
        self._coverages = coverages

    def write(self, wsh: Worksheet, wb: Workbook):
        standards = sorted(set(chain.from_iterable(
            v.keys() for v in self._coverages.values()
        )))
        bold = wb.add_format({'bold': True})
        percent = wb.add_format({'num_format': '0.00%'})
        table = Table()
        table.new_row()
        table.add_cells(CellContent('Regions', bold))
        for st in standards:
            table.add_cells(CellContent(st, bold))
        for region, region_data in self._coverages.items():
            table.new_row()
            table.add_cells(CellContent(region))
            for st in standards:
                table.add_cells(CellContent(region_data.get(st), percent))
        writer = XlsxRowsWriter()
        writer.write(wsh, table)


class ComplianceReportHandler(AbstractHandler):
    def __init__(self, tenant_service: TenantService,
                 coverage_service: CoverageService,
                 environment_service: EnvironmentService,
                 ambiguous_job_service: AmbiguousJobService,
                 report_service: ReportService):
        self._tenant_service = tenant_service
        self._coverage_service = coverage_service
        self._environment_service = environment_service
        self._ambiguous_job_service = ambiguous_job_service
        self._report_service = report_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            tenant_service=SP.modular_client.tenant_service(),
            coverage_service=SP.coverage_service,
            environment_service=SP.environment_service,
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_COMPLIANCE_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            },
            CustodianEndpoint.REPORTS_COMPLIANCE_TENANTS_TENANT_NAME: {
                HTTPMethod.GET: self.get_by_tenant
            }
        }

    @validate_kwargs
    def get_by_job(self, event: JobComplianceReportGetModel, job_id: str):
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
        tenant = self._tenant_service.get(job.tenant_name)
        if not tenant:
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content='Job tenant not found'
            )
        # TODO api implement for platform
        if not job.is_ed_job:
            collection = self._report_service.job_collection(tenant, job.job)
        else:
            collection = self._report_service.ed_job_collection(tenant,
                                                                job.job)
        collection.fetch_all()
        coverages = self._coverage_service.coverage_from_collection(
            collection, modular_helpers.tenant_cloud(tenant)
        )
        response = ReportResponse(job, coverages, fmt=event.format)
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        coverages, f'{tenant.name}-compliance.json'
                    )
                    response.content = url
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer) as wb:
                    ComplianceReportXlsxWriter(coverages).write(
                        wb=wb,
                        wsh=wb.add_worksheet('Compliance')
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{job.id}-compliance.xlsx'
                )
                response.content = url
        return build_response(content=response.dict())

    @validate_kwargs
    def get_by_tenant(self, event: TenantComplianceReportGetModel, 
                      tenant_name: str):
        tenant = self._tenant_service.get(tenant_name)
        modular_helpers.assert_tenant_valid(tenant, event.customer)
        cloud = modular_helpers.tenant_cloud(tenant)
        if not cloud:
            return build_response(
                content=f'Not allowed cloud: {cloud.value}',
                code=HTTPStatus.BAD_REQUEST
            )
        collection = self._report_service.tenant_latest_collection(tenant)
        collection.fetch_all()
        coverages = self._coverage_service.coverage_from_collection(
            collection, cloud
        )
        response = ReportResponse(tenant, coverages, fmt=event.format)
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        coverages, f'{tenant_name}-compliance.json'
                    )
                    response.content = url
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer) as wb:
                    ComplianceReportXlsxWriter(coverages).write(
                        wb=wb,
                        wsh=wb.add_worksheet('Compliance')
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{tenant_name}-compliance.xlsx'
                )
                response.content = url
        return build_response(content=response.dict())
