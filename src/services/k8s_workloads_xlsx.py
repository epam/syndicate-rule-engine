"""
Renders K8s workloads rule-level tables to xlsx: an introduction sheet and
one sheet per rule. Each rule sheet has a short header block (title, rule,
severity) and a table with autofilter and frozen header row.
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
from services.k8s_workloads import PodRow, WorkloadRow, WorkloadTable


@dataclass(frozen=True)
class Column:
    header: str
    getter: Callable[[Any], str | int]
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
    Column('Pods', lambda r: r.pods, 10),
)

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
            'pods for each pod rule and the number of their violated pods.'
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
        self.write_introduction(tables)
        if not tables:
            wsh = self._wb.add_worksheet(
                unique_sheet_name('Workloads', self._used)
            )
            wsh.write(0, 0, 'No pod rule findings', self._bold)
            return
        for table in tables:
            self.write_table(table)

    def write_introduction(self, tables: list[WorkloadTable]) -> None:
        wsh = self._wb.add_worksheet(
            unique_sheet_name('Introduction', self._used)
        )
        wsh.set_column(0, 0, 24)
        wsh.set_column(1, 1, 100)
        title, purpose = _INTRO['summary' if self._summary else 'detailed']
        wsh.write(0, 0, title, self._title)
        sections = (
            ('Purpose', purpose),
            (
                'Worksheets',
                (
                    'Each subsequent worksheet contains findings of one pod '
                    'rule and is named by the rule short ID.'
                ),
            ),
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
        row = 2
        for row, (label, text) in enumerate(sections, start=2):
            wsh.write(row, 0, label, self._intro_label)
            wsh.write(row, 1, text, self._intro_text)
            wsh.set_row(row, 36)
        if not tables:
            return
        header_row = row + 2
        wsh.write(header_row, 0, 'Rule ID', self._header)
        wsh.write(header_row, 1, 'Title', self._header)
        for row, table in enumerate(tables, start=header_row + 1):
            wsh.write_string(row, 0, table.short_rule_id)
            wsh.write_string(row, 1, table.title[:MAX_CELL_LENGTH])

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

        header_row = 4
        for col, column in enumerate(self._columns):
            wsh.write(header_row, col, column.header, self._header)
            wsh.set_column(col, col, column.width)

        rows: list[PodRow] | list[WorkloadRow] = (
            table.workloads if self._summary else table.pods
        )
        row_idx = header_row
        for row_idx, row in enumerate(rows, start=header_row + 1):
            for col, column in enumerate(self._columns):
                value = column.getter(row)
                if isinstance(value, int):
                    wsh.write_number(row_idx, col, value)
                else:
                    wsh.write_string(
                        row_idx, col, (value or EMPTY)[:MAX_CELL_LENGTH]
                    )

        wsh.autofilter(header_row, 0, row_idx, len(self._columns) - 1)
        wsh.freeze_panes(header_row + 1, 1)
        return wsh
