from io import BytesIO
from types import SimpleNamespace
from typing import cast
from zipfile import ZipFile
from xml.etree import ElementTree

from openpyxl import load_workbook
from xlsxwriter import Workbook

from handlers.reports.compliance_handler import (
    ComplianceReportHandler,
    ComplianceReportXlsxWriter,
)
from helpers.constants import Cloud, Severity
from helpers.reports import Standard
from services.metadata import Metadata
from services.sharding import ShardPart, ShardsCollectionFactory


_DETAILED_REPORT = {
    'eu-west-1': {
        'CIS Kubernetes Benchmark v1.7.0': {
            'total': 0.5,
            'controls': {
                '1.1': {
                    'total': 1.0,
                    'severity': Severity.HIGH,
                    'rules': {
                        'successful': ['rule-z', 'rule-a'],
                        'failed': [],
                        'not_evaluated': [],
                        'total': 2,
                        'missing_from_meta': 0,
                    },
                },
                '1.2': {
                    'total': 0.0,
                    'severity': Severity.MEDIUM,
                    'rules': {
                        'successful': [],
                        'failed': ['rule-f'],
                        'not_evaluated': ['rule-ne'],
                        'total': 2,
                        'missing_from_meta': 0,
                    },
                },
                '1.3': {
                    'total': 0.0,
                    'severity': Severity.LOW,
                    'rules': {
                        'successful': [],
                        'failed': [],
                        'not_evaluated': ['rule-ne-only'],
                        'total': 1,
                        'missing_from_meta': 0,
                    },
                },
            },
        },
    },
}


def _render(coverages: dict, detailed: bool) -> bytes:
    buffer = BytesIO()
    with Workbook(buffer) as workbook:
        ComplianceReportXlsxWriter(coverages, detailed=detailed).write(
            wb=workbook,
            wsh=workbook.add_worksheet('Compliance'),
        )
    return buffer.getvalue()


def test_summary_coverages_are_written_as_numeric_percentages():
    workbook_bytes = _render(
        {'eu-west-1': {'CIS Kubernetes Benchmark v1.7.0': 0.75}},
        detailed=False,
    )

    worksheet = load_workbook(BytesIO(workbook_bytes)).active

    assert worksheet['A1'].value == 'Regions'
    assert worksheet['B1'].value == 'CIS Kubernetes Benchmark v1.7.0'
    assert worksheet['B2'].value == 0.75
    assert worksheet['B2'].number_format == '0.00%'


def test_detailed_writer_flattens_rows_and_uses_text_conditional_formats():
    workbook_bytes = _render(_DETAILED_REPORT, detailed=True)
    workbook = load_workbook(BytesIO(workbook_bytes))
    worksheet = workbook['Compliance']

    assert [cell.value for cell in worksheet[1]] == list(
        ComplianceReportXlsxWriter._DETAILED_HEADERS
    )
    assert worksheet.max_row == 4
    assert worksheet.auto_filter.ref == 'A1:L4'
    assert worksheet.freeze_panes == 'A2'
    assert [cell.value for cell in worksheet[2]] == [
        'eu-west-1',
        'CIS Kubernetes Benchmark v1.7.0',
        0.5,
        '1.1',
        'High',
        'Pass',
        1.0,
        'rule-a, rule-z',
        None,
        None,
        2,
        0,
    ]
    assert worksheet['F3'].value == 'Fail'
    assert worksheet['F4'].value == 'Not Evaluated'

    namespace = {
        'x': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    }
    with ZipFile(BytesIO(workbook_bytes)) as archive:
        root = ElementTree.fromstring(archive.read('xl/worksheets/sheet1.xml'))
    conditional_formats = root.findall('x:conditionalFormatting', namespace)
    assert {item.attrib['sqref'] for item in conditional_formats} == {
        'E2:E4',
        'F2:F4',
    }
    formulas = {
        formula.text
        for item in conditional_formats
        for formula in item.findall('.//x:formula', namespace)
    }
    assert '$E2="High"' in formulas
    assert '$F2="Not Evaluated"' in formulas


def test_detailed_writer_handles_empty_report():
    workbook_bytes = _render({}, detailed=True)
    worksheet = load_workbook(BytesIO(workbook_bytes))['Compliance']

    assert worksheet.max_row == 1
    assert worksheet.max_column == len(
        ComplianceReportXlsxWriter._DETAILED_HEADERS
    )
    assert worksheet.auto_filter.ref == 'A1:L1'


def test_detailed_rule_buckets_are_scoped_to_region():
    collection = ShardsCollectionFactory.from_cloud(Cloud.AWS)
    collection.put_parts(
        [
            ShardPart(policy='rule-a', location='eu-west-1', resources=[]),
            ShardPart(policy='rule-b', location='eu-west-1', resources=[{}]),
            ShardPart(
                policy='rule-a', location='eu-central-1', resources=[{}]
            ),
            ShardPart(policy='rule-b', location='eu-central-1', resources=[]),
        ]
    )
    standard = Standard('CIS Kubernetes Benchmark', 'v1.7.0')
    rule_mapping = {
        standard.name: {
            standard.version_str: {
                '1.1': {
                    'rules': ['rule-a', 'rule-b'],
                    'severity': Severity.HIGH,
                },
            },
        },
    }
    full_coverage = {standard: {'1.1': 2}}

    eu_west_coverage = {standard: {'controls': {'1.1': 0.5}}}
    ComplianceReportHandler._extend_with_control_coverages(
        collection=collection,
        coverages=eu_west_coverage,
        full_coverage=full_coverage,
        standard_control_to_rule_names=rule_mapping,
        location='eu-west-1',
    )
    assert eu_west_coverage[standard]['controls']['1.1']['rules'] == {
        'successful': ['rule-a'],
        'failed': ['rule-b'],
        'not_evaluated': [],
        'total': 2,
        'missing_from_meta': 0,
    }

    eu_central_coverage = {standard: {'controls': {'1.1': 0.5}}}
    ComplianceReportHandler._extend_with_control_coverages(
        collection=collection,
        coverages=eu_central_coverage,
        full_coverage=full_coverage,
        standard_control_to_rule_names=rule_mapping,
        location='eu-central-1',
    )
    assert eu_central_coverage[standard]['controls']['1.1']['rules'] == {
        'successful': ['rule-b'],
        'failed': ['rule-a'],
        'not_evaluated': [],
        'total': 2,
        'missing_from_meta': 0,
    }


def test_mapping_keeps_rules_and_selects_highest_cloud_severity():
    metadata = cast(
        Metadata,
        cast(
            object,
            SimpleNamespace(
                rules={
                    'rule-low': SimpleNamespace(
                        cloud=Cloud.AWS,
                        severity=Severity.LOW,
                        standard={
                            'CIS Kubernetes Benchmark': {
                                'v1.7.0': ['1.1'],
                            },
                        },
                    ),
                    'rule-high': SimpleNamespace(
                        cloud=Cloud.AWS,
                        severity=Severity.HIGH,
                        standard={
                            'CIS Kubernetes Benchmark': {
                                'v1.7.0': ['1.1'],
                            },
                        },
                    ),
                    'rule-other-cloud': SimpleNamespace(
                        cloud=Cloud.AZURE,
                        severity=Severity.HIGH,
                        standard={
                            'CIS Kubernetes Benchmark': {
                                'v1.7.0': ['1.1'],
                            },
                        },
                    ),
                }
            ),
        ),
    )

    result = ComplianceReportHandler._build_standard_control_to_rule_names(
        metadata=metadata,
        cloud=Cloud.AWS,
    )

    assert result == {
        'CIS Kubernetes Benchmark': {
            'v1.7.0': {
                '1.1': {
                    'rules': ['rule-low', 'rule-high'],
                    'severity': Severity.HIGH,
                },
            },
        },
    }
