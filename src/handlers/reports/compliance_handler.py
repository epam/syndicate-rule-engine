import io
from http import HTTPStatus
from itertools import chain
from typing import Any

from modular_sdk.services.tenant_service import TenantService
from typing_extensions import Self
from xlsxwriter import Workbook
from xlsxwriter.format import Format
from xlsxwriter.worksheet import Worksheet

from handlers import AbstractHandler, Mapping
from helpers import deep_get
from helpers.constants import (
    GLOBAL_REGION,
    Cloud,
    Endpoint,
    HTTPMethod,
    ReportFormat,
    Severity,
)
from helpers.lambda_response import build_response
from services import SP, modular_helpers
from services.environment_service import EnvironmentService
from services.job_service import JobService
from services.license_service import LicenseService
from services.metadata import Metadata
from services.report_service import ReportResponse, ReportService
from services.sharding import ShardsCollection
from validators.swagger_request_models import (
    JobComplianceReportGetModel,
    TenantComplianceReportGetModel,
)
from validators.utils import validate_kwargs


class ComplianceReportXlsxWriter:
    _DETAILED_HEADERS = (
        'Region',
        'Standard',
        'Standard Coverage',
        'Control',
        'Severity',
        'Status',
        'Control Coverage',
        'Successful Rules',
        'Failed Rules',
        'Not Evaluated Rules',
        'Total Rules',
        'Missing From Metadata',
    )

    def __init__(
        self,
        coverages: dict[str, dict[str, Any]],
        detailed: bool = False,
    ):
        self._coverages = coverages
        self._detailed = detailed

    def write(self, wsh: Worksheet, wb: Workbook):
        if self._detailed:
            self._write_detailed(wsh, wb)
            return

        self._write_summary(wsh, wb)

    def _write_summary(self, wsh: Worksheet, wb: Workbook):
        standards = sorted(
            set(chain.from_iterable(v.keys() for v in self._coverages.values()))
        )
        bold = wb.add_format({"bold": True})
        percent = wb.add_format({"num_format": "0.00%"})
        wsh.write(0, 0, 'Regions', bold)
        for column, standard in enumerate(standards, start=1):
            wsh.write(0, column, standard, bold)
        for row, (region, region_data) in enumerate(
            self._coverages.items(), start=1
        ):
            wsh.write(row, 0, region)
            for column, standard in enumerate(standards, start=1):
                self._write_cell(
                    wsh,
                    row,
                    column,
                    region_data.get(standard),
                    percent,
                )

    @staticmethod
    def _status(
        successful: list[str],
        failed: list[str],
        not_evaluated: list[str],
    ) -> str:
        if failed:
            return 'Fail'
        if not_evaluated or not successful:
            return 'Not Evaluated'
        return 'Pass'

    @staticmethod
    def _write_cell(
        wsh: Worksheet,
        row: int,
        column: int,
        value: object,
        cell_format: Format,
    ) -> None:
        if value is None:
            wsh.write_blank(row, column, None, cell_format)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            wsh.write_number(row, column, value, cell_format)
        else:
            wsh.write(row, column, value, cell_format)

    def _write_detailed(self, wsh: Worksheet, wb: Workbook):
        """Write one row for every control in a detailed compliance report."""
        header = wb.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': '#FFFFFF',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
        })
        text = wb.add_format({'border': 1, 'valign': 'top'})
        wrapped = wb.add_format({
            'border': 1,
            'valign': 'top',
            'text_wrap': True,
        })
        percent = wb.add_format({
            'border': 1,
            'num_format': '0.00%',
            'valign': 'top',
        })
        total = wb.add_format({'border': 1, 'valign': 'top'})
        severity_formats = {
            'High': wb.add_format({
                'border': 1,
                'bg_color': '#FF3300',
                'valign': 'top',
            }),
            'Medium': wb.add_format({
                'border': 1,
                'bg_color': '#FFCC00',
                'valign': 'top',
            }),
            'Low': wb.add_format({
                'border': 1,
                'bg_color': '#FFFF99',
                'valign': 'top',
            }),
            'Info': wb.add_format({
                'border': 1,
                'bg_color': '#A6C9EC',
                'valign': 'top',
            }),
        }
        status_formats = {
            'Pass': wb.add_format({
                'border': 1,
                'bg_color': '#C6EFCE',
                'font_color': '#006100',
                'valign': 'top',
            }),
            'Fail': wb.add_format({
                'border': 1,
                'bg_color': '#FFC7CE',
                'font_color': '#9C0006',
                'valign': 'top',
            }),
            'Not Evaluated': wb.add_format({
                'border': 1,
                'bg_color': '#BFBFBF',
                'font_color': '#404040',
                'valign': 'top',
            }),
        }

        for column, value in enumerate(self._DETAILED_HEADERS):
            wsh.write(0, column, value, header)
        wsh.set_row(0, 30)

        row = 1
        for region, region_data in self._coverages.items():
            if not isinstance(region_data, dict):
                continue
            for standard, standard_data in region_data.items():
                if not isinstance(standard_data, dict):
                    continue
                standard_coverage = standard_data.get('total')
                controls = standard_data.get('controls') or {}
                if not isinstance(controls, dict):
                    continue
                for control, control_data in controls.items():
                    if not isinstance(control_data, dict):
                        continue
                    rules = control_data.get('rules') or {}
                    if not isinstance(rules, dict):
                        rules = {}
                    successful = sorted(rules['successful'])
                    failed = sorted(rules['failed'])
                    not_evaluated = sorted(rules['not_evaluated'])
                    severity = control_data.get('severity', Severity.UNKNOWN)
                    status = self._status(
                        successful, failed, not_evaluated
                    )
                    values = (
                        region,
                        standard,
                        standard_coverage,
                        control,
                        severity,
                        status,
                        control_data.get('total'),
                        ', '.join(successful),
                        ', '.join(failed),
                        ', '.join(not_evaluated),
                        rules.get('total'),
                        rules.get('missing_from_meta'),
                    )
                    formats = (
                        text,
                        text,
                        percent,
                        text,
                        text,
                        text,
                        percent,
                        wrapped,
                        wrapped,
                        wrapped,
                        total,
                        total,
                    )
                    for column, (value, cell_format) in enumerate(
                        zip(values, formats)
                    ):
                        self._write_cell(
                            wsh, row, column, value, cell_format
                        )
                    row += 1

        last_column = len(self._DETAILED_HEADERS) - 1
        last_data_row = max(row - 1, 0)
        wsh.autofilter(0, 0, last_data_row, last_column)
        if row > 1:
            for value, cell_format in severity_formats.items():
                wsh.conditional_format(
                    1,
                    4,
                    last_data_row,
                    4,
                    {
                        'type': 'formula',
                        'criteria': f'=$E2="{value}"',
                        'format': cell_format,
                    },
                )
            for value, cell_format in status_formats.items():
                wsh.conditional_format(
                    1,
                    5,
                    last_data_row,
                    5,
                    {
                        'type': 'formula',
                        'criteria': f'=$F2="{value}"',
                        'format': cell_format,
                    },
                )
        wsh.freeze_panes(1, 0)
        wsh.set_column(0, 0, 16)
        wsh.set_column(1, 1, 36)
        wsh.set_column(2, 2, 18)
        wsh.set_column(3, 3, 14)
        wsh.set_column(4, 5, 16)
        wsh.set_column(6, 6, 18)
        wsh.set_column(7, 9, 42)
        wsh.set_column(10, 11, 18)


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
        job = self._job_service.get_nullable(job_id)
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
        standard_control_to_rule_names = {}
        if event.detailed:
            standard_control_to_rule_names = self._build_standard_control_to_rule_names(
                metadata=metadata, cloud=cloud
            )
        for location, parts in mapping.items():
            coverages = self._report_service.calculate_coverages(
                successful=self._report_service.get_standard_to_controls_to_rules(
                    it=parts, metadata=metadata
                ),
                full=metadata.domain(tenant.cloud).full_cov,
                detailed=event.detailed
            )
            if event.detailed:
                self._extend_with_control_coverages(
                    collection=collection,
                    coverages=coverages,
                    full_coverage=metadata.domain(tenant.cloud).full_cov,
                    standard_control_to_rule_names=standard_control_to_rule_names,
                    location=location,
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
                    ComplianceReportXlsxWriter(
                        region_coverages, detailed=event.detailed
                    ).write(
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
        standard_control_to_rule_names = {}
        if event.detailed:
            standard_control_to_rule_names = self._build_standard_control_to_rule_names(
                metadata=metadata, cloud=cloud
            )
        for location, parts in mapping.items():
            coverages = self._report_service.calculate_coverages(
                successful=self._report_service.get_standard_to_controls_to_rules(
                    it=parts, metadata=metadata
                ),
                full=metadata.domain(tenant.cloud).full_cov,
                detailed=event.detailed
            )
            if event.detailed:
                self._extend_with_control_coverages(
                    collection=collection,
                    coverages=coverages,
                    full_coverage=metadata.domain(tenant.cloud).full_cov,
                    standard_control_to_rule_names=standard_control_to_rule_names,
                    location=location,
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
                    ComplianceReportXlsxWriter(
                        region_coverages, detailed=event.detailed
                    ).write(
                        wb=wb, wsh=wb.add_worksheet("Compliance")
                    )
                buffer.seek(0)
                url = self._report_service.one_time_url(
                    buffer, f"{tenant_name}-compliance.xlsx"
                )
                response.content = url
        return build_response(content=response.dict())

    @staticmethod
    def _build_standard_control_to_rule_names(
        metadata: Metadata, cloud: Cloud
    ) -> dict:
        result = {}

        def _sev_rank(severity: Severity) -> int:
            return list(Severity).index(severity)

        for rule_name, rule_metadata in metadata.rules.items():
            if rule_metadata.cloud != cloud:
                continue
            rule_sev = rule_metadata.severity
            for standard, versions in rule_metadata.standard.items():
                std_map = result.setdefault(standard, {})
                for version, controls in versions.items():
                    ver_map = std_map.setdefault(version, {})
                    for control in controls:
                        ctrl_data = ver_map.setdefault(
                            control, {
                                'rules': [],
                                'severity': None
                            }
                        )
                        ctrl_data['rules'].append(rule_name)
                        if ctrl_data['severity'] is None \
                            or _sev_rank(rule_sev) > _sev_rank(ctrl_data['severity']):
                            ctrl_data['severity'] = rule_sev

        return result

    @staticmethod
    def _extend_with_control_coverages(
        collection: ShardsCollection,
        coverages: dict,
        full_coverage: dict,
        standard_control_to_rule_names: dict,
        location: str | None = None,
    ) -> None:
        all_successful_rules = []
        all_failed_rules = []
        for part in collection.iter_parts():
            if location is not None and part.location != location:
                continue
            if part.resources:
                all_failed_rules.append(part.policy)
            else:
                all_successful_rules.append(part.policy)

        for standard, data in coverages.items():
            new_controls = {}
            for control, coverage in data['controls'].items():
                control_data = deep_get(
                    standard_control_to_rule_names,
                    (
                        standard.name,
                        standard.version_str,
                        control
                    )
                ) or {}
                control_rules = control_data.get('rules', [])
                control_severity = control_data.get('severity', Severity.UNKNOWN)
                total_rules = full_coverage[standard][control]
                successful_rules = (
                    list(set(control_rules) & set(all_successful_rules))
                )
                failed_rules = (
                    list(set(control_rules) & set(all_failed_rules))
                )
                not_evaluated_rules = (
                    list(set(control_rules)
                         - set(all_successful_rules)
                         - set(all_failed_rules))
                )
                rules = {
                    'successful': successful_rules,
                    'failed': failed_rules,
                    'not_evaluated': not_evaluated_rules,
                    'total': total_rules,
                    'missing_from_meta': total_rules - len(control_rules)
                }
                new_controls[control] = {
                    'total': coverage,
                    'rules': rules,
                    'severity': control_severity
                }

            data['controls'] = new_controls
