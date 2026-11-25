"""
Renders K8s workloads rule-level tables to xlsx: an introduction sheet and
either one sheet per rule (detailed) or one Workloads sheet with all unique
workloads (summary). Each rule sheet has a short header block (title, rule,
severity). Tables have autofilter and frozen header row.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from helpers.constants import Severity
from services.k8s_rbac_xlsx import (
    EMPTY,
    MAX_CELL_LENGTH,
    _join,
    unique_sheet_name,
)
from services.k8s_workloads import WorkloadRow, WorkloadTable, build_summary


@dataclass(frozen=True)
class Column:
    header: str
    getter: Callable[[Any], str]
    width: int = 20


DETAILED_COLUMNS: tuple[Column, ...] = (
    Column('Pod name', lambda r: r.pod_name, 50),
    Column('Namespace', lambda r: r.namespace, 24),
    Column('Owner kind', lambda r: r.owner_kind, 16),
    Column('Owner name', lambda r: r.owner_name, 44),
    Column('Matched filters', lambda r: _join(r.matched_filters), 60),
)

SUMMARY_COLUMNS: tuple[Column, ...] = (
    Column('Owner name', lambda r: r.owner_name, 44),
    Column('Owner kind', lambda r: r.owner_kind, 16),
    Column('Namespace', lambda r: r.namespace, 24),
    Column('Violations', lambda r: _join(r.violations), 80),
)

_WORKSHEETS = {
    'detailed': (
        'Each subsequent worksheet contains findings of one pod rule and is '
        'named by the rule short ID.'
    ),
    'summary': (
        'The Workloads worksheet contains all unique workloads with '
        'violated pods.'
    ),
}
WORKLOADS_SHEET = 'Workloads'

_INTRO = {
    'detailed': (
        'Kubernetes workloads report: detailed',
        (
            'Lists every violated pod of each pod rule with its namespace, '
            'immediate owner and the rule filters that matched.'
        ),
    ),
    'summary': (
        'Kubernetes workloads report: summary',
        (
            'Lists unique workloads (immediate pod owners) with violated '
            'pods of all pod rules. Violations lists the titles of all '
            'pod rules that the workload violates.'
        ),
    ),
}


class WorkloadsXlsxWriter:
    def __init__(self, wb: Workbook, *, summary: bool = False):
        self._wb = wb
        self._summary = summary
        self._columns = SUMMARY_COLUMNS if summary else DETAILED_COLUMNS
        self._used: set[str] = set()
        self._bold = wb.add_format({'bold': True})
        self._title = wb.add_format({'bold': True, 'font_size': 13})
        self._intro_label = wb.add_format({'bold': True, 'valign': 'top'})
        self._intro_text = wb.add_format({'text_wrap': True, 'valign': 'top'})
        self._header = wb.add_format(
            {'bold': True, 'bg_color': '#d9e1f2', 'border': 1}
        )
        self._severity = {
            Severity.HIGH: wb.add_format({'bg_color': '#da9694'}),
            Severity.MEDIUM: wb.add_format({'bg_color': '#ffff00'}),
            Severity.LOW: wb.add_format({'bg_color': '#92d051'}),
        }
        self._gray = wb.add_format({'bg_color': '#bfbfbf'})

    def write(self, tables: list[WorkloadTable]) -> None:
        self.write_introduction()
        if self._summary:
            self.write_summary(build_summary(tables))
            return
        if not tables:
            self._write_empty()
            return
        for table in tables:
            self.write_table(table)

    def _write_empty(self) -> None:
        wsh = self._wb.add_worksheet(
            unique_sheet_name(WORKLOADS_SHEET, self._used)
        )
        wsh.write(0, 0, 'No pod rule findings', self._bold)

    def write_introduction(self) -> None:
        wsh = self._wb.add_worksheet(
            unique_sheet_name('Introduction', self._used)
        )
        wsh.set_column(0, 0, 24)
        wsh.set_column(1, 1, 100)
        kind = 'summary' if self._summary else 'detailed'
        title, purpose = _INTRO[kind]
        wsh.write(0, 0, title, self._title)
        sections = (
            ('Purpose', purpose),
            ('Worksheets', _WORKSHEETS[kind]),
            (
                'Owner',
                (
                    'Owner kind and name come from the pod immediate '
                    'controller (metadata.ownerReferences): a ReplicaSet '
                    'for Deployment pods and a Job for CronJob pods. A pod '
                    'without owner is shown as its own workload with the '
                    'Pod kind.'
                ),
            ),
        )
        for row, (label, text) in enumerate(sections, start=2):
            wsh.write(row, 0, label, self._intro_label)
            wsh.write(row, 1, text, self._intro_text)
            wsh.set_row(row, 54 if label == 'Owner' else 36)

    def write_table(self, table: WorkloadTable) -> Worksheet:
        wsh = self._wb.add_worksheet(
            unique_sheet_name(table.short_rule_id, self._used)
        )
        wsh.write(0, 0, 'Title', self._bold)
        wsh.write_string(0, 1, table.title[:MAX_CELL_LENGTH], self._title)
        wsh.write(1, 0, 'Rule ID', self._bold)
        wsh.write_string(1, 1, table.rule)
        wsh.write(2, 0, 'Severity', self._bold)
        wsh.write(
            2,
            1,
            table.severity.value,
            self._severity.get(table.severity, self._gray),
        )

        self._write_rows(wsh, 4, table.pods)
        return wsh

    def write_summary(self, rows: list[WorkloadRow]) -> Worksheet | None:
        if not rows:
            self._write_empty()
            return None
        wsh = self._wb.add_worksheet(
            unique_sheet_name(WORKLOADS_SHEET, self._used)
        )
        self._write_rows(wsh, 0, rows)
        return wsh

    def _write_rows(
        self, wsh: Worksheet, header_row: int, rows: list[Any]
    ) -> None:
        for col, column in enumerate(self._columns):
            wsh.write(header_row, col, column.header, self._header)
            wsh.set_column(col, col, column.width)

        row_idx = header_row
        for row_idx, row in enumerate(rows, start=header_row + 1):
            for col, column in enumerate(self._columns):
                value = column.getter(row) or EMPTY
                wsh.write_string(row_idx, col, value[:MAX_CELL_LENGTH])

        wsh.autofilter(header_row, 0, row_idx, len(self._columns) - 1)
        wsh.freeze_panes(header_row + 1, 1)
