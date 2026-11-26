"""
Renders K8s RBAC "WHO can do WHAT" tables to xlsx: one sheet per table.

Each sheet has a short header block (title, rules, severity), a header row
with autofilter and frozen panes, the main columns and a group of collapsed
detail columns that can be expanded with the "+" button above the sheet.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass

from xlsxwriter import Workbook
from xlsxwriter.worksheet import Worksheet

from helpers.constants import Severity
from services.k8s_rbac import RbacRow, RbacSummaryRow, RbacTable

EMPTY = '-'
SEPARATOR = ', '
MAX_CELL_LENGTH = 32767  # Excel limit of characters in one cell
_MAX_SHEET_NAME = 31
_INVALID_SHEET_CHARS = re.compile(r'[\[\]:*?/\\]')


def _join(values: tuple[str, ...] | list[str]) -> str:
    """
    Joins values into one cell. Values that do not fit into an Excel cell
    are replaced with a "… (+N more)" suffix
    """
    text = SEPARATOR.join(values)
    if len(text) <= MAX_CELL_LENGTH:
        return text
    reserve = len(f'{SEPARATOR}… (+{len(values)} more)')
    kept = size = 0
    for value in values:
        add = len(value) + (len(SEPARATOR) if kept else 0)
        if size + add + reserve > MAX_CELL_LENGTH:
            break
        size += add
        kept += 1
    more = f'… (+{len(values) - kept} more)'
    if not kept:
        return more
    return SEPARATOR.join(values[:kept]) + SEPARATOR + more


def _pods(row: RbacRow) -> str:
    return _join(row.workloads)


def _resource_names(row: RbacRow) -> str:
    # empty resourceNames means the rule is not restricted by names
    return _join(row.resource_names) if row.resource_names else '*'


def _subject_name(row: RbacRow) -> str:
    return row.subject_name if row.bound else '(not bound)'


@dataclass(frozen=True)
class RbacColumn:
    header: str
    getter: Callable[[RbacRow], str]
    width: int = 20
    detail: bool = False


COLUMNS: tuple[RbacColumn, ...] = (
    RbacColumn('Subject name', _subject_name, 30),
    RbacColumn('Subject kind', lambda r: r.subject_kind, 16),
    RbacColumn('Pods', _pods, 40),
    RbacColumn('Role name', lambda r: r.role_name, 36),
    RbacColumn('Role resource types', lambda r: _join(r.resource_types), 40),
    RbacColumn('Role resourceNames', _resource_names, 24),
    RbacColumn('Role verbs (permissions)', lambda r: _join(r.verbs), 36),
    RbacColumn('Effective namespace', lambda r: r.effective_namespace, 22),
    RbacColumn('Subject namespace', lambda r: r.subject_namespace, 20, True),
    RbacColumn('Role kind', lambda r: r.role_kind, 14, True),
    RbacColumn('Role namespace', lambda r: r.role_namespace, 20, True),
    RbacColumn('Binding kind', lambda r: r.binding_kind, 20, True),
    RbacColumn('Binding name', lambda r: r.binding_name, 36, True),
    RbacColumn('Binding namespace', lambda r: r.binding_namespace, 20, True),
    RbacColumn('API groups', lambda r: _join(r.api_groups), 24, True),
    RbacColumn('Aggregated via', lambda r: r.aggregated_via, 30, True),
    RbacColumn('Pod names', lambda r: _join(r.pods), 50, True),
)


def unique_sheet_name(name: str, used: set[str]) -> str:
    base = _INVALID_SHEET_CHARS.sub('_', name).strip("'") or 'Sheet'
    base = base[:_MAX_SHEET_NAME]
    candidate, i = base, 1
    while candidate.lower() in used:
        suffix = f' ({i})'
        candidate = base[: _MAX_SHEET_NAME - len(suffix)] + suffix
        i += 1
    used.add(candidate.lower())
    return candidate


class RbacXlsxWriter:
    def __init__(
        self, wb: Workbook, columns: tuple[RbacColumn, ...] = COLUMNS
    ):
        self._wb = wb
        self._columns = columns
        self._used: set[str] = set()
        self._bold = wb.add_format({'bold': True})
        self._title = wb.add_format({'bold': True, 'font_size': 13})
        self._intro_label = wb.add_format({'bold': True, 'valign': 'top'})
        self._intro_text = wb.add_format({'text_wrap': True, 'valign': 'top'})
        self._header = wb.add_format(
            {'bold': True, 'bg_color': '#d9e1f2', 'border': 1}
        )
        self._detail_header = wb.add_format(
            {'bold': True, 'bg_color': '#ededed', 'border': 1}
        )
        self._severity = {
            Severity.HIGH: wb.add_format({'bg_color': '#da9694'}),
            Severity.MEDIUM: wb.add_format({'bg_color': '#ffff00'}),
            Severity.LOW: wb.add_format({'bg_color': '#92d051'}),
        }
        self._gray = wb.add_format({'bg_color': '#bfbfbf'})

    def write_tables(self, tables: list[RbacTable]) -> None:
        self.write_introduction()
        if not tables:
            wsh = self._wb.add_worksheet(unique_sheet_name('RBAC', self._used))
            wsh.write(0, 0, 'No RBAC findings', self._bold)
            return
        for table in sorted(
            tables, key=lambda item: item.sheet_name.casefold()
        ):
            self.write_table(table)

    def write_introduction(self) -> None:
        wsh = self._wb.add_worksheet(
            unique_sheet_name('Introduction', self._used)
        )
        wsh.set_column(0, 0, 24)
        wsh.set_column(1, 1, 100)
        wsh.write(0, 0, 'Kubernetes RBAC report', self._title)
        sections = (
            (
                'Purpose',
                (
                    'Shows subjects and permissions associated with Roles '
                    'and ClusterRoles that triggered RBAC findings.'
                ),
            ),
            (
                'Worksheets',
                (
                    'Each subsequent worksheet contains findings for the '
                    'rule IDs shown in its title and rows for the affected '
                    'subjects and role permissions. Finding worksheets are '
                    'sorted by sheet name.'
                ),
            ),
            (
                'Effective namespace',
                (
                    'For bound findings, this is the binding scope: a '
                    'RoleBinding namespace or cluster-wide for a '
                    'ClusterRoleBinding. For unbound roles, it is the '
                    'Role namespace or cluster-wide for a ClusterRole.'
                ),
            ),
            (
                'Pods',
                (
                    'The visible Pods column lists workload/controller '
                    'names. Pod names are in the collapsed Pod names '
                    'detail column.'
                ),
            ),
            (
                'Not bound',
                (
                    '(not bound) means no matching binding was found in '
                    'the inventory snapshot for this scan.'
                ),
            ),
            (
                'Details',
                (
                    'Additional context columns are collapsed by default. '
                    'Use the + outline control above the column headers to '
                    'expand them.'
                ),
            ),
        )
        for row, (label, text) in enumerate(sections, start=2):
            wsh.write(row, 0, label, self._intro_label)
            wsh.write(row, 1, text, self._intro_text)
            wsh.set_row(row, 36)

    def write_table(self, table: RbacTable) -> Worksheet:
        wsh = self._wb.add_worksheet(
            unique_sheet_name(table.sheet_name, self._used)
        )
        wsh.write(0, 0, 'Title', self._bold)
        wsh.write(0, 1, table.title, self._title)
        wsh.write(1, 0, 'Rule ID', self._bold)
        wsh.write(1, 1, _join(table.rules))
        wsh.write(2, 0, 'Severity', self._bold)
        wsh.write(
            2,
            1,
            table.severity.value,
            self._severity.get(table.severity, self._gray),
        )

        header_row = 4
        for col, column in enumerate(self._columns):
            wsh.write(
                header_row,
                col,
                column.header,
                self._detail_header if column.detail else self._header,
            )
            options = {'level': 1, 'hidden': True} if column.detail else {}
            wsh.set_column(col, col, column.width, None, options)

        row_idx = header_row
        for row_idx, row in enumerate(table.rows, start=header_row + 1):
            for col, column in enumerate(self._columns):
                value = column.getter(row) or EMPTY
                wsh.write_string(row_idx, col, value[:MAX_CELL_LENGTH])

        last_col = len(self._columns) - 1
        wsh.autofilter(header_row, 0, row_idx, last_col)
        wsh.freeze_panes(header_row + 1, 1)
        return wsh

    def write_summary(self, rows: list[RbacSummaryRow]) -> Worksheet:
        wsh = self._wb.add_worksheet(
            unique_sheet_name('Single Pane of Glass', self._used)
        )
        wsh.write(0, 0, 'Kubernetes RBAC: Single Pane of Glass', self._title)
        wsh.write(
            1,
            0,
            (
                'Each subject is shown once with its highest detected '
                'access level.'
            ),
        )

        headers = ('Subject', 'Type', 'Access', 'Scope', 'Roles')
        widths = (34, 14, 14, 32, 48)
        header_row = 3
        for col, (header, width) in enumerate(zip(headers, widths)):
            wsh.write(header_row, col, header, self._header)
            wsh.set_column(col, col, width)

        for row_idx, row in enumerate(rows, start=header_row + 1):
            values = (
                row.subject_name,
                row.subject_kind,
                row.access,
                row.scope,
                _join(row.roles),
            )
            for col, value in enumerate(values):
                wsh.write_string(
                    row_idx, col, (value or EMPTY)[:MAX_CELL_LENGTH]
                )

        last_row = header_row + len(rows)
        wsh.autofilter(header_row, 0, last_row, len(headers) - 1)
        wsh.freeze_panes(header_row + 1, 1)
        return wsh
