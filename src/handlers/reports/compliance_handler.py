import io
from http import HTTPStatus
from itertools import chain

from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self
from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    GLOBAL_REGION,
    Cloud,
    Endpoint,
    HTTPMethod,
    ReportFormat,
)
from helpers.lambda_response import build_response
from helpers.system_customer import SystemCustomer
from services import SP, modular_helpers
from services.environment_service import EnvironmentService
from services.job_service import JobService
from services.license_service import LicenseService
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
        standards = sorted(
            set(chain.from_iterable(v.keys() for v in self._coverages.values()))
        )
        bold = wb.add_format({"bold": True})
        percent = wb.add_format({"num_format": "0.00%"})
        table = Table()
        table.new_row()
        table.add_cells(CellContent("Regions", bold))
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
    def __init__(
        self,
        tenant_service: TenantService,
        environment_service: EnvironmentService,
        job_service: JobService,
        report_service: ReportService,
        license_service: LicenseService,
    ):
        self._tenant_service = tenant_service
        self._environment_service = environment_service
        self._job_service = job_service
        self._report_service = report_service
        self._license_service = license_service

    @classmethod
    def build(cls) -> Self:
        return cls(
            tenant_service=SP.modular_client.tenant_service(),
            environment_service=SP.environment_service,
            job_service=SP.job_service,
            report_service=SP.report_service,
            license_service=SP.license_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            Endpoint.REPORTS_COMPLIANCE_JOBS_JOB_ID: {HTTPMethod.GET: self.get_by_job},
            Endpoint.REPORTS_COMPLIANCE_TENANTS_TENANT_NAME: {
                HTTPMethod.GET: self.get_by_tenant
            },
        }

    @validate_kwargs
    def get_by_job(self, event: JobComplianceReportGetModel, job_id: str):
        customer_name = event.customer or SystemCustomer.get_name()
        job = self._job_service.get_by_customer_name(
            customer_name=customer_name,
            job_id=job_id,
            job_types=event.job_types,
        )
        job = next(job, None)
        if not job:
            return build_response(
                content="The request job not found",
                code=HTTPStatus.NOT_FOUND,
            )
        tenant = self._tenant_service.get(job.tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)
        if not tenant:
            return build_response(
                code=HTTPStatus.NOT_FOUND, content="Job tenant not found"
            )
        cloud = modular_helpers.tenant_cloud(tenant)
        if not cloud:
            return build_response(
                content=f"Not allowed cloud: {cloud.value}",
                code=HTTPStatus.BAD_REQUEST,
            )
        # TODO: implement for platform
        collection = self._report_service.job_collection(tenant, job)
        collection.fetch_all()

        if cloud is Cloud.AWS:
            mapping = self._report_service.group_parts_iterator_by_location(
                self._report_service.iter_successful_parts(collection)
            )
        else:
            mapping = {
                GLOBAL_REGION: list(
                    self._report_service.iter_successful_parts(collection)
                )
            }
        region_coverages = {}
        metadata = self._license_service.get_customer_metadata(tenant.customer_name)
        for location, parts in mapping.items():
            coverages = self._report_service.calculate_coverages(
                successful=self._report_service.get_standard_to_controls_to_rules(
                    it=parts, metadata=metadata
                ),
                full=metadata.domain(tenant.cloud).full_cov,
            )
            region_coverages[location] = {
                st.full_name: cov for st, cov in coverages.items()
            }

        response = ReportResponse(job, region_coverages, fmt=event.format)
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        region_coverages, f"{job.id}-compliance.json"
                    )
                    response.content = url
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer) as wb:
                    ComplianceReportXlsxWriter(region_coverages).write(
                        wb=wb, wsh=wb.add_worksheet("Compliance")
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f"{job.id}-compliance.xlsx"
                )
                response.content = url
        return build_response(content=response.dict())

    @validate_kwargs
    def get_by_tenant(self, event: TenantComplianceReportGetModel, tenant_name: str):
        tenant = self._tenant_service.get(tenant_name)
        tenant = modular_helpers.assert_tenant_valid(tenant, event.customer)
        cloud = modular_helpers.tenant_cloud(tenant)
        if not cloud:
            return build_response(
                content=f"Not allowed cloud: {cloud.value}",
                code=HTTPStatus.BAD_REQUEST,
            )
        collection = self._report_service.tenant_latest_collection(tenant)
        collection.fetch_all()

        if cloud is Cloud.AWS:
            mapping = self._report_service.group_parts_iterator_by_location(
                self._report_service.iter_successful_parts(collection)
            )
        else:
            mapping = {
                GLOBAL_REGION: list(
                    self._report_service.iter_successful_parts(collection)
                )
            }
        region_coverages = {}
        metadata = self._license_service.get_customer_metadata(tenant.customer_name)
        for location, parts in mapping.items():
            coverages = self._report_service.calculate_coverages(
                successful=self._report_service.get_standard_to_controls_to_rules(
                    it=parts, metadata=metadata
                ),
                full=metadata.domain(tenant.cloud).full_cov,
            )
            region_coverages[location] = {
                st.full_name: cov for st, cov in coverages.items()
            }

        response = ReportResponse(tenant, region_coverages, fmt=event.format)
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        region_coverages, f"{tenant_name}-compliance.json"
                    )
                    response.content = url
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer) as wb:
                    ComplianceReportXlsxWriter(region_coverages).write(
                        wb=wb, wsh=wb.add_worksheet("Compliance")
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f"{tenant_name}-compliance.xlsx"
                )
                response.content = url
        return build_response(content=response.dict())
