"""
Standard Questionnaire Generator
======================================
Generates a single-sheet Excel file that maps every control of a chosen
security standard to its audit status, coverage and the rule(s) that
evaluate it.

Usage
-----
    python main.py \\
        --standard  "CIS Kubernetes Benchmark v1.7.0" \\
        --compliance job-compliance.json \\
        --schemas    mappings \\
        --output     cis_questionnaire.xlsx

Arguments
---------
  --standard   NAME  (required) Full standard name as it appears in the
                     compliance report, e.g. "CIS Kubernetes Benchmark v1.7.0"
                     or "CIS Controls v7".  The version suffix (v1.7.0 / v7)
                     is used to select the correct version block inside the
                     schema file.
  --compliance FILE  (required) Path to the SRE job compliance report JSON.
                     Contains per-region, per-control rule pass/fail lists.
  --schemas    DIR   (required) Root of the standards-schema repository
                     (the directory that contains the standards_schema/ sub-
                     folder, e.g. the cloned "mappings" repo).
  --output  /  -o    Output XLSX file (default: standard_questionnaire.xlsx).

Data sources
------------
  From compliance report (per-scan, changes with every run):
    • Coverage %   — the control ``total`` value supplied by the report
    • Status       — Pass / Fail / Not Evaluated
    • Rule IDs     — rules listed under each control
    • Severity     — the control ``severity`` value supplied by the report

  From standards_schema (static, same across all scans):
    • Section / sub-section / control hierarchy
    • Section and control titles

  Multi-region aggregation:
    • A rule is counted as "failed"  if it appears in *any* region's failed list.
    • A rule is counted as "successful" only when it never failed anywhere.
    • Remaining rules (only in not_evaluated lists) are Not Evaluated.

Status logic
------------
    Fail          = at least one rule is in ``failed``
    Pass          = every rule is in ``successful``
    Not Evaluated = no failure, but at least one rule is not evaluated
                    (or no rules are available)

The control coverage is not recalculated by this script.  It uses the
``total`` value already present in the detailed compliance report.
"""
from __future__ import annotations

import argparse
import json
import re
import runpy
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from openpyxl import Workbook
from openpyxl.cell import MergedCell
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.filters import AutoFilter
from openpyxl.worksheet.table import Table, TableColumn, TableStyleInfo
from openpyxl.worksheet.worksheet import Worksheet

# ── Colours ───────────────────────────────────────────────────────────────────

HEX_HEADER_BG    = '4472C4'
HEX_HEADER_FONT  = 'FFFFFF'
HEX_GREEN_PASTEL = 'C6EFCE'
HEX_GREEN_FONT   = '006100'
HEX_RED_PASTEL   = 'FFC7CE'
HEX_RED_FONT     = '9C0006'
HEX_GRAY         = 'BFBFBF'
HEX_GRAY_FONT    = '404040'
HEX_BORDER_BLUE  = '9DC3E6'

SEVERITY_HEADER_FILL_COLORS: dict[str, str] = {
    'High':   'FF3300',
    'Medium': 'FFCC00',
    'Low':    'FFFF99',
    'Info':   'A6C9EC',
}

STATUS_FILL_COLORS: dict[str, str] = {
    'Pass':          HEX_GREEN_PASTEL,
    'Fail':          HEX_RED_PASTEL,
    'Not Evaluated': HEX_GRAY,
}

STATUS_FONT_COLORS: dict[str, str] = {
    'Pass':          HEX_GREEN_FONT,
    'Fail':          HEX_RED_FONT,
    'Not Evaluated': HEX_GRAY_FONT,
}

_BORDER_SIDE = Side(style='thin', color=HEX_BORDER_BLUE)
_CELL_BORDER = Border(
    left=_BORDER_SIDE, right=_BORDER_SIDE,
    top=_BORDER_SIDE,  bottom=_BORDER_SIDE,
)
_WHITE_FILL = PatternFill(start_color='FFFFFF', end_color='FFFFFF', fill_type='solid')

_ALIGN_TOP_LEFT      = Alignment(horizontal='left',   vertical='top')
_ALIGN_TOP_CENTER    = Alignment(horizontal='center', vertical='top')
_ALIGN_TOP_LEFT_WRAP = Alignment(horizontal='left',   vertical='top', wrap_text=True)
_ALIGN_HEADER        = Alignment(horizontal='center', vertical='center', wrap_text=True)


# ── Data classes ──────────────────────────────────────────────────────────────

def _short_id(rule_id: str) -> str:
    """Return the last numeric group of a rule ID ('ecc-k8s-001-...' → '001')."""
    matches = re.findall(r'\d+', rule_id)
    return matches[-1] if matches else rule_id


@dataclass
class ControlRow:
    section_id:    str
    section_title: str
    # Intermediate sub-sections between the top section and the control.
    # Each entry is (id, title).
    sub_sections:  list[tuple[str, str]] = field(default_factory=list)
    control_id:    str = ''
    control_title: str = ''
    coverage:      float = 0.0
    severity:      str = ''
    # Rule ID lists populated from the compliance report.
    successful_rules:    list[str] = field(default_factory=list)
    failed_rules:        list[str] = field(default_factory=list)
    not_evaluated_rules: list[str] = field(default_factory=list)

    # ── Derived properties ────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if self.failed_rules:
            return 'Fail'
        if self.not_evaluated_rules or not self.successful_rules:
            return 'Not Evaluated'
        return 'Pass'

    @property
    def rule_short_ids(self) -> str:
        """Short numeric IDs of all rules for this control (all three lists)."""
        all_ids = (
            set(self.successful_rules)
            | set(self.failed_rules)
            | set(self.not_evaluated_rules)
        )
        return ', '.join(sorted(_short_id(r) for r in all_ids)) if all_ids else ''


# ── Schema loading ─────────────────────────────────────────────────────────────

def _parse_standard_name(name: str) -> tuple[str, str | None]:
    """
    Split a standard name into (base, version).

    Examples:
        'CIS Kubernetes Benchmark v1.7.0' → ('CIS Kubernetes Benchmark', 'v1.7.0')
        'CIS Controls v7'                 → ('CIS Controls', 'v7')
        'AWS Config'                      → ('AWS Config', None)
    """
    m = re.search(r'\s+(v[\d.]+)$', name, re.IGNORECASE)
    if m:
        return name[:m.start()].strip(), m.group(1)
    return name.strip(), None


def _find_schema_file(schemas_root: Path, base_name: str) -> Path | None:
    """
    Locate the Python schema file for *base_name* inside
    ``<schemas_root>/standards_schema/``.

    Tries an exact filename match first (spaces → underscores), then falls
    back to a case-insensitive glob.
    """
    schema_dir = schemas_root / 'standards_schema'
    if not schema_dir.is_dir():
        return None
    candidate = base_name.replace(' ', '_') + '.py'
    exact = schema_dir / candidate
    if exact.exists():
        return exact
    lower = candidate.lower()
    for p in schema_dir.glob('*.py'):
        if p.name.lower() == lower:
            return p
    return None


def _load_schema(schema_file: Path, version: str | None) -> tuple[dict, str]:
    """
    Execute the schema Python file and return (version_block, resolved_version).

    The file is expected to define a ``full`` (or ``tech``) variable that is a
    dict keyed by version string (e.g. ``"v1.7.0"``).  When *version* is None
    the first available version is used.
    """
    ns = runpy.run_path(str(schema_file))
    data: dict = ns.get('full') or ns.get('tech') or {}
    if not data:
        raise ValueError(
            f'Schema file {schema_file} defines neither "full" nor "tech".'
        )

    if version is None:
        version = next(iter(data))
    elif version not in data:
        for k in data:
            if k.lower() == version.lower():
                version = k
                break
        else:
            raise ValueError(
                f'Version "{version}" not found in {schema_file.name}. '
                f'Available: {list(data.keys())}'
            )
    return data[version], version


def _parse_schema_node(
    node: dict,
    ancestors: list[tuple[str, str]],
) -> Iterator[ControlRow]:
    """
    Recursively walk a schema section node and yield one ControlRow per control.

    *ancestors* is the stack of (id, title) pairs from the root section down to
    (but not including) the current node for sub-section recursion.
    """
    for ctrl_id, ctrl_data in (node.get('controls') or {}).items():
        section_id, section_title = ancestors[0]
        sub_sections = ancestors[1:]
        yield ControlRow(
            section_id=section_id,
            section_title=section_title,
            sub_sections=list(sub_sections),
            control_id=ctrl_id,
            control_title=(ctrl_data or {}).get('control_title', ''),
        )

    for sub_id, sub_node in (node.get('sections') or {}).items():
        sub_title = (sub_node or {}).get('section', '')
        yield from _parse_schema_node(sub_node, ancestors + [(sub_id, sub_title)])


def parse_schema(version_block: dict) -> list[ControlRow]:
    """Parse a version block and return a flat list of ControlRows."""
    rows: list[ControlRow] = []
    for sec_id, sec_node in (version_block.get('sections') or {}).items():
        sec_title = (sec_node or {}).get('section', '')
        rows.extend(_parse_schema_node(sec_node, [(sec_id, sec_title)]))
    return rows


# ── Compliance report parsing ─────────────────────────────────────────────────

def _match_standard_key(region_data: dict, standard_name: str) -> str | None:
    """Return the dict key that best matches *standard_name* (case-insensitive)."""
    lower = standard_name.lower()
    for key in region_data:
        if key.lower() == lower:
            return key
    return None


def parse_compliance(
    data: dict,
    standard_name: str,
) -> dict[str, dict[str, object]]:
    """
    Extract per-control rule lists for *standard_name* from a compliance JSON.

    The JSON is structured as::

        {
          "<region>": {
            "<Standard Name>": {
              "controls": {
                "<control_id>": {
                  "total": <float>,
                  "severity": "<severity>",
                  "rules": {
                    "successful":    [...],
                    "failed":        [...],
                    "not_evaluated": [...]
                  }
                }
              }
            }
          }
        }

    Multi-region aggregation:
      • *failed*        — union across ALL regions  (worst case wins)
      • *successful*    — union, minus anything in the failed set
      • *not_evaluated* — union, minus successful and failed

    The report's control ``total`` and ``severity`` values are preserved.  If
    the report contains more than one region, the first non-null coverage and
    severity values are retained; rule lists are still unioned across regions.

    Returns:
        ``{control_id: {"coverage": float, "severity": str, ...}}``
    """
    aggregated: dict[str, dict[str, Any]] = {}

    for region_data in data.values():
        std_key = _match_standard_key(region_data, standard_name)
        if std_key is None:
            continue
        for ctrl_id, ctrl_data in (region_data[std_key].get('controls') or {}).items():
            rules = (ctrl_data or {}).get('rules') or {}
            if not isinstance(rules, dict):
                rules = {}
            bucket = aggregated.setdefault(
                ctrl_id,
                {
                    'coverage': None,
                    'severity': None,
                    'successful': set(),
                    'failed': set(),
                    'not_evaluated': set(),
                },
            )
            coverage = (ctrl_data or {}).get('total')
            if bucket['coverage'] is None and coverage is not None:
                bucket['coverage'] = coverage
            severity = (ctrl_data or {}).get('severity')
            if bucket['severity'] is None and severity is not None:
                bucket['severity'] = str(severity)
            bucket['failed'].update(rules.get('failed') or [])
            bucket['successful'].update(rules.get('successful') or [])
            bucket['not_evaluated'].update(rules.get('not_evaluated') or [])

    result: dict[str, dict[str, Any]] = {}
    for ctrl_id, bucket in aggregated.items():
        failed        = bucket['failed']
        successful    = bucket['successful'] - failed
        not_evaluated = bucket['not_evaluated'] - failed - successful
        result[ctrl_id] = {
            'coverage':      bucket['coverage'] if bucket['coverage'] is not None else 0.0,
            'severity':      bucket['severity'] or '',
            'successful':    sorted(successful),
            'failed':        sorted(failed),
            'not_evaluated': sorted(not_evaluated),
        }
    return result


def apply_compliance_data(
    rows: list[ControlRow],
    compliance: dict[str, dict[str, Any]],
) -> None:
    """Populate rule lists on each ControlRow from the parsed compliance data."""
    for row in rows:
        if row.control_id in compliance:
            ctrl = compliance[row.control_id]
            coverage = ctrl.get('coverage', 0.0)
            try:
                row.coverage = float(coverage) if coverage is not None else 0.0
            except (TypeError, ValueError):
                row.coverage = 0.0
            row.severity = str(ctrl.get('severity') or '')
            row.successful_rules    = ctrl['successful']
            row.failed_rules        = ctrl['failed']
            row.not_evaluated_rules = ctrl['not_evaluated']


# ── Excel helpers ─────────────────────────────────────────────────────────────

def _fill(hex_color: str) -> PatternFill:
    return PatternFill(start_color=hex_color, end_color=hex_color, fill_type='solid')


def _font(hex_color: str, bold: bool = False) -> Font:
    return Font(color=hex_color, bold=bold)


def _add_dynamic_text_formatting(
    ws: Worksheet,
    column: int,
    last_data_row: int,
    fill_colors: dict[str, str],
    font_colors: dict[str, str] | None = None,
) -> None:
    """Apply conditional formatting that follows the cell's text value."""
    if last_data_row < 2:
        return

    column_letter = get_column_letter(column)
    cell_range = f'{column_letter}2:{column_letter}{last_data_row}'
    for text, fill_color in fill_colors.items():
        font = _font(font_colors[text]) if font_colors and text in font_colors else None
        ws.conditional_formatting.add(
            cell_range,
            FormulaRule(
                formula=[f'UPPER(${column_letter}2)="{text.upper()}"'],
                fill=_fill(fill_color),
                font=font,
            ),
        )


def auto_adjust_column_widths(
    ws: Worksheet,
    limit: int = 50,
    ignore_rows: set[int] | None = None,
) -> None:
    ignore_rows = ignore_rows or set()
    merged_top_left = {
        (min_row, min_col)
        for min_col, min_row, max_col, max_row in (
            rng.bounds for rng in ws.merged_cells.ranges
        )
        if max_col > min_col or max_row > min_row
    }
    for column in ws.columns:
        max_length    = 0
        column_letter = None
        for cell in column:
            if (cell.row, cell.column) in merged_top_left:
                continue
            if cell.row in ignore_rows:
                continue
            if isinstance(cell, MergedCell):
                continue
            if column_letter is None:
                column_letter = cell.column_letter
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except Exception:
                pass
        if column_letter is None:
            continue
        ws.column_dimensions[column_letter].width = max(10, min(max_length + 2, limit))


# ── Excel generation ──────────────────────────────────────────────────────────

def generate_excel(
    rows: list[ControlRow],
    standard_name: str,
    version: str,
    output_path: Path,
) -> None:
    """Write the questionnaire workbook to *output_path*."""

    max_sub_depth = max((len(r.sub_sections) for r in rows), default=0)

    if max_sub_depth == 0:
        sub_headers: list[str] = []
    elif max_sub_depth == 1:
        sub_headers = ['Sub-section', 'Sub-title']
    else:
        sub_headers = []
        for level in range(1, max_sub_depth + 1):
            sub_headers.append(f'Sub-section (l{level + 1})')
            sub_headers.append(f'Sub-title (l{level + 1})')

    headers: list[str] = (
        ['Section', 'Title']
        + sub_headers
        + ['Control', 'Severity', 'Status', 'Control Title', 'Coverage', 'Rule IDs']
    )

    n_pre          = 2 + len(sub_headers)
    COL_SECTION    = 1
    COL_TITLE      = 2
    COL_CONTROL    = n_pre + 1
    COL_SEVERITY   = n_pre + 2
    COL_STATUS     = n_pre + 3
    COL_CTRL_TITLE = n_pre + 4
    COL_COVERAGE   = n_pre + 5
    COL_RULES      = n_pre + 6
    total_cols     = len(headers)

    wb = Workbook()
    ws = wb.active
    ws.title = 'Standard Questionnaire'

    # ── Header row ────────────────────────────────────────────────────────────
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill      = _fill(HEX_HEADER_BG)
        cell.font      = _font(HEX_HEADER_FONT, bold=True)
        cell.alignment = _ALIGN_HEADER
    ws.row_dimensions[1].height = 30

    # ── Data rows ─────────────────────────────────────────────────────────────
    for data_idx, row in enumerate(rows, start=2):

        ws.cell(row=data_idx, column=COL_SECTION, value=row.section_id).alignment = _ALIGN_TOP_LEFT
        ws.cell(row=data_idx, column=COL_TITLE,   value=row.section_title).alignment = _ALIGN_TOP_LEFT

        for level in range(max_sub_depth):
            col_id    = 3 + level * 2
            col_title = 4 + level * 2
            if level < len(row.sub_sections):
                sid, stitle = row.sub_sections[level]
                ws.cell(row=data_idx, column=col_id,    value=sid   ).alignment = _ALIGN_TOP_LEFT
                ws.cell(row=data_idx, column=col_title, value=stitle).alignment = _ALIGN_TOP_LEFT
            else:
                ws.cell(row=data_idx, column=col_id   ).alignment = _ALIGN_TOP_LEFT
                ws.cell(row=data_idx, column=col_title).alignment = _ALIGN_TOP_LEFT

        ws.cell(row=data_idx, column=COL_CONTROL, value=row.control_id).alignment = _ALIGN_TOP_LEFT

        # Severity — supplied by the detailed compliance report.
        ws.cell(row=data_idx, column=COL_SEVERITY, value=row.severity or None).alignment = _ALIGN_TOP_LEFT

        # Status
        status = row.status
        c = ws.cell(row=data_idx, column=COL_STATUS, value=status)
        c.alignment = _ALIGN_TOP_CENTER

        # Control Title
        ws.cell(row=data_idx, column=COL_CTRL_TITLE, value=row.control_title).alignment = _ALIGN_TOP_LEFT_WRAP

        # Coverage — stored as 0.0–1.0 float with percentage format
        c = ws.cell(row=data_idx, column=COL_COVERAGE, value=row.coverage)
        c.number_format = '0%'
        c.alignment     = _ALIGN_TOP_LEFT

        # Rule IDs
        c = ws.cell(row=data_idx, column=COL_RULES, value=row.rule_short_ids or None)
        c.alignment = _ALIGN_TOP_LEFT

    last_data_row = len(rows) + 1

    # ── White fill + light-blue borders + alignment ───────────────────────────
    for r in range(1, last_data_row + 1):
        for col in range(1, total_cols + 1):
            cell = ws.cell(row=r, column=col)
            if isinstance(cell, MergedCell):
                continue
            cell.border = _CELL_BORDER
            if r > 1:
                if col == COL_STATUS:
                    cell.alignment = _ALIGN_TOP_CENTER
                elif col == COL_CTRL_TITLE:
                    cell.alignment = _ALIGN_TOP_LEFT_WRAP
                else:
                    cell.alignment = _ALIGN_TOP_LEFT
                if cell.fill is None or cell.fill.fill_type in (None, 'none'):
                    cell.fill = _WHITE_FILL

    # ── Excel Table ───────────────────────────────────────────────────────────
    table_end_ref = f'{get_column_letter(total_cols)}{last_data_row}'
    table = Table(displayName='StandardQuestionnaire', ref=f'A1:{table_end_ref}')
    table.autoFilter = AutoFilter(ref=f'A1:{table_end_ref}')
    for i, header in enumerate(headers, start=1):
        table.tableColumns.append(TableColumn(id=i, name=header))
    table.tableStyleInfo = TableStyleInfo(
        name='TableStyleMedium9',
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=False,
        showColumnStripes=False,
    )
    ws.add_table(table)

    # ── Dynamic text-based formatting ────────────────────────────────────────
    # Conditional formatting evaluates the current cell text in Excel, so
    # changing Severity or Status manually also changes its color.
    _add_dynamic_text_formatting(
        ws,
        COL_SEVERITY,
        last_data_row,
        SEVERITY_HEADER_FILL_COLORS,
    )
    _add_dynamic_text_formatting(
        ws,
        COL_STATUS,
        last_data_row,
        STATUS_FILL_COLORS,
        STATUS_FONT_COLORS,
    )

    # ── Summary rows (outside the table) ─────────────────────────────────────
    row_applicable = last_data_row + 2
    row_full       = last_data_row + 3

    def _write_summary(row_num: int, label: str, formula: str) -> None:
        lc = ws.cell(row=row_num, column=COL_SECTION, value=label)
        lc.font      = Font(bold=True)
        lc.alignment = _ALIGN_TOP_LEFT
        vc = ws.cell(row=row_num, column=COL_COVERAGE, value=formula)
        vc.font          = Font(bold=True)
        vc.alignment     = _ALIGN_TOP_LEFT
        vc.number_format = '0%'

    _write_summary(
        row_applicable,
        'SRE Applicable Coverage',
        ('=IFERROR(AVERAGEIF(StandardQuestionnaire[Status],"<>Not Evaluated",'
         'StandardQuestionnaire[Coverage]),0)'),
    )
    _write_summary(
        row_full,
        'Total Coverage',
        '=IFERROR(AVERAGE(StandardQuestionnaire[Coverage]),0)',
    )

    auto_adjust_column_widths(ws, limit=50, ignore_rows={row_applicable, row_full})
    ws.freeze_panes = 'A2'

    # Ask Excel to recalculate the structured-reference formulas on open/save.
    wb.calculation.calcMode = 'auto'
    wb.calculation.fullCalcOnLoad = True
    wb.calculation.forceFullCalc = True
    wb.calculation.calcOnSave = True

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)

    evaluated = sum(1 for r in rows if r.status != 'Not Evaluated')
    passed    = sum(1 for r in rows if r.status == 'Pass')
    applicable_coverages = [
        r.coverage for r in rows if r.status != 'Not Evaluated'
    ]
    sre_applicable_coverage = (
        sum(applicable_coverages) / len(applicable_coverages)
        if applicable_coverages else 0.0
    )
    total_coverage = (
        sum(r.coverage for r in rows) / len(rows)
        if rows else 0.0
    )
    print(
        f'✓ Saved: {output_path}  ({len(rows)} controls)\n'
        f'  Pass          : {passed}\n'
        f'  Fail          : {evaluated - passed}\n'
        f'  Not Evaluated : {len(rows) - evaluated}\n'
        f'  SRE Applicable Coverage : {sre_applicable_coverage:.0%}\n'
        f'  Total Coverage          : {total_coverage:.0%}'
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Generate a standard questionnaire Excel file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--standard',
        required=True,
        metavar='NAME',
        help=(
            'Full standard name as it appears in the compliance report, '
            'e.g. "CIS Kubernetes Benchmark v1.7.0" or "AWS Config". '
            'The version suffix selects the correct version block in the schema file.'
        ),
    )
    parser.add_argument(
        '--compliance',
        type=Path,
        required=True,
        metavar='FILE',
        help='Path to the SRE job compliance report JSON.',
    )
    parser.add_argument(
        '--schemas',
        type=Path,
        required=True,
        metavar='DIR',
        help=(
            'Root of the standards-schema repository '
            '(the directory that contains the standards_schema/ sub-folder).'
        ),
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('standard_questionnaire.xlsx'),
        metavar='XLSX',
        help='Output Excel file path (default: standard_questionnaire.xlsx).',
    )
    args = parser.parse_args(argv)

    if not args.compliance.exists():
        print(f'Error: compliance file not found: {args.compliance}', file=sys.stderr)
        return 1
    if not args.schemas.is_dir():
        print(f'Error: schemas directory not found: {args.schemas}', file=sys.stderr)
        return 1

    base_name, version = _parse_standard_name(args.standard)
    schema_file = _find_schema_file(args.schemas, base_name)
    if schema_file is None:
        schema_dir = args.schemas / 'standards_schema'
        available  = sorted(p.stem for p in schema_dir.glob('*.py') if p.stem != '__init__')
        print(
            f'Error: no schema file found for "{base_name}" in {schema_dir}.\n'
            f'Available schemas: {", ".join(available)}',
            file=sys.stderr,
        )
        return 1

    try:
        version_block, resolved_version = _load_schema(schema_file, version)
    except ValueError as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1

    print(f'Schema   : {schema_file.name}  (version {resolved_version})')

    rows = parse_schema(version_block)
    print(f'Controls : {len(rows)} controls loaded from schema')

    data = json.loads(args.compliance.read_text(encoding='utf-8'))
    compliance = parse_compliance(data, args.standard)
    if not compliance:
        print(
            f'Warning: standard "{args.standard}" was not found in the compliance '
            f'report.  All controls will be marked "Not Evaluated".',
            file=sys.stderr,
        )
    else:
        print(f'Compliance: {len(compliance)} controls found in report')

    apply_compliance_data(rows, compliance)

    generate_excel(rows, base_name, resolved_version, args.output)
    return 0


if __name__ == '__main__':
    sys.exit(main())
