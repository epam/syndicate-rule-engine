import io
from http import HTTPStatus
from typing import Iterator

from modular_sdk.services.tenant_service import TenantService
from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers.constants import (
    CustodianEndpoint,
    HTTPMethod,
    JobState,
    ReportFormat,
)
from helpers.lambda_response import build_response
from services import SP
from services import modular_helpers
from services.ambiguous_job_service import AmbiguousJobService
from services.report_service import (
    ReportResponse,
    ReportService,
    StatisticsItem,
)
from services.xlsx_writer import CellContent, Table, XlsxRowsWriter
from validators.swagger_request_models import (
    JobRuleReportGetModel,
    TenantRuleReportGetModel,
)
from validators.utils import validate_kwargs


class RulesReportXlsxWriter:
    head = (
        'Rule',
        'Region',
        'Executed successfully',
        'Execution time (seconds)',
        'Failed Resources',
    )

    def __init__(self, it: Iterator[dict]):
        self._it = it

    @staticmethod
    def status_empty(st: bool) -> str:
        """
        What value will be should in 'Failed Resources' field in case rule
        was executed successfully or unsuccessfully
        :param st:
        :return:
        """
        if st:
            return '0'
        return ''

    def write(self, wsh: Worksheet, wb: Workbook):
        bold = wb.add_format({'bold': True})
        red = wb.add_format({'bg_color': '#da9694'})
        green = wb.add_format({'bg_color': '#92d051'})

        def status_color(st: bool):
            if st:
                return green
            return red

        remapped = {}
        for item in self._it:
            remapped.setdefault(item['policy'], []).append(item)
        # sorts buy the total number of failed resources per rule
        # across all the regions
        data = dict(
            sorted(
                remapped.items(),
                key=lambda p: sum(
                    i.get('failed_resources') or 0 for i in p[1]
                ),
                reverse=True,
            )
        )

        table = Table()
        table.new_row()
        for h in self.head:
            table.add_cells(CellContent(h, bold))
        for rule, items in data.items():
            table.new_row()
            table.add_cells(CellContent(rule))
            items = sorted(
                items,
                key=lambda i: i.get('failed_resources') or 0,
                reverse=True,
            )
            table.add_cells(*[CellContent(item['region']) for item in items])
            table.add_cells(
                *[
                    CellContent(
                        str(item['succeeded']).lower(),
                        status_color(item['succeeded']),
                    )
                    for item in items
                ]
            )
            table.add_cells(
                *[CellContent(item['execution_time']) for item in items]
            )
            table.add_cells(
                *[
                    CellContent(
                        item.get('failed_resources')
                        or self.status_empty(item['succeeded'])
                    )
                    for item in items
                ]
            )
        writer = XlsxRowsWriter()
        writer.write(wsh, table)


class JobsRulesHandler(AbstractHandler):
    def __init__(
        self,
        ambiguous_job_service: AmbiguousJobService,
        report_service: ReportService,
        tenant_service: TenantService,
    ):
        self._ambiguous_job_service = ambiguous_job_service
        self._report_service = report_service
        self._tenant_service = tenant_service

    @classmethod
    def build(cls) -> 'AbstractHandler':
        return cls(
            ambiguous_job_service=SP.ambiguous_job_service,
            report_service=SP.report_service,
            tenant_service=SP.modular_client.tenant_service(),
        )

    @property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.REPORTS_RULES_JOBS_JOB_ID: {
                HTTPMethod.GET: self.get_by_job
            },
            CustodianEndpoint.REPORTS_RULES_TENANTS_TENANT_NAME: {
                HTTPMethod.GET: self.get_by_tenant_accumulated
            },
        }

    @staticmethod
    def _format_statistics_item(item: StatisticsItem) -> dict:
        return {
            'policy': item.policy,
            'region': item.region,
            'api_calls': item.api_calls,
            'execution_time': item.end_time - item.start_time,
            'succeeded': item.is_successful(),
            'scanned_resources': item.scanned_resources,
            'failed_resources': item.failed_resources,
            'error_type': item.error_type
        }

    @validate_kwargs
    def get_by_job(self, event: JobRuleReportGetModel, job_id: str):
        job = self._ambiguous_job_service.get_job(
            job_id=job_id, typ=event.job_type, customer=event.customer
        )
        if not job:
            return build_response(
                content='The request job not found', code=HTTPStatus.NOT_FOUND
            )
        statistics = self._report_service.job_statistics(job.job)
        data = map(
            self._format_statistics_item, statistics
        )
        content = []
        match event.format:
            case ReportFormat.JSON:
                if event.href:
                    url = self._report_service.one_time_url_json(
                        list(data), f'{job.id}-rules.json'
                    )
                    content = ReportResponse(job, url).dict()
                else:
                    content = list(data)
            case ReportFormat.XLSX:
                buffer = io.BytesIO()
                with Workbook(buffer, {'strings_to_numbers': True}) as wb:
                    RulesReportXlsxWriter(data).write(
                        wb=wb, wsh=wb.add_worksheet('Rules')
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f'{job.id}-rules.xlsx'
                )
                content = ReportResponse(
                    job, url, fmt=ReportFormat.XLSX
                ).dict()
        return build_response(content=content)

    @validate_kwargs
    def get_by_tenant_accumulated(
        self, event: TenantRuleReportGetModel, tenant_name: str
    ):
        tenant = self._tenant_service.get(tenant_name)
        modular_helpers.assert_tenant_valid(tenant)

        jobs = self._ambiguous_job_service.get_by_tenant_name(
            tenant_name=tenant_name,
            job_type=event.job_type,
            status=JobState.SUCCEEDED,
            start=event.start_iso,
            end=event.end_iso,
        )
        average = self._report_service.average_statistics(
            *map(self._report_service.job_statistics, jobs)
        )
        return build_response(content=average)
