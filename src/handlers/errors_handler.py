import io
from http import HTTPStatus
from typing import Iterable

from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers.constants import CustodianEndpoint, HTTPMethod, ReportFormat
from helpers.lambda_response import build_response
from services import SP
from services.ambiguous_job_service import AmbiguousJobService
from services.report_service import (
    ReportResponse,
    ReportService,
    StatisticsItem,
)
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter
from validators.swagger_request_models import JobErrorReportGetModel
from validators.utils import validate_kwargs


class ResourceReportXlsxWriter:
    head = ('Rule', 'Region', 'Type', 'Reason')

    def __init__(self, it: Iterable[StatisticsItem]):
        self._it = it

    def write(self, wsh: Worksheet, wb: Workbook):
        bold = wb.add_format({'bold': True})
        remapped = {}
        for item in self._it:
            remapped.setdefault(item.policy, []).append(item)
        table = Table()
        table.new_row()
        for h in self.head:
            table.add_cells(CellContent(h, bold))
        for rule, items in remapped.items():
            table.new_row()
            table.add_cells(CellContent(rule))
            table.add_cells(*[CellContent(item.region) for item in items])
            table.add_cells(
                *[CellContent(item.error_type.value) for item in items]
            )
            table.add_cells(*[CellContent(item.reason) for item in items])
        XlsxRowsWriter().write(wsh, table)


class ErrorsReportHandler(AbstractHandler):
    def __init__(
        self,
        ambiguous_job_service: AmbiguousJobService,
        report_service: ReportService,
    ):
        self._ambiguous_job_service = ambiguous_job_service
        self._report_service = report_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_ERRORS_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            }
        }

    @validate_kwargs
    def get_by_job(self, event: JobErrorReportGetModel, job_id: str):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id, typ=event.job_type, customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found', code=HTTPStatus.NOT_FOUND
            )
        statistics = self._report_service.job_statistics(job.job)
        data = map(
            self._report_service.format_statistics_failed,
            self._report_service.only_failed(
                statistic=statistics, error_type=event.error_type
            ),
        )
        content = []
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        list(data), f'{job.id}-errors.json'
                    )
                    content = ReportResponse(job, url).dict()
                else:
                    content = list(data)
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer) as wb:
                    ResourceReportXlsxWriter(data).write(
                        wb=wb, wsh=wb.add_worksheet('Errors')
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{job.id}-errors.xlsx'
                )
                content = ReportResponse(
                    job, url, fmt=ReportFormat.XLSX
                ).dict()
        return build_response(content=content)
