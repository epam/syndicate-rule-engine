# Standard Questionnaire Generator

`generate.py` generates an Excel questionnaire by combining:

- the **control hierarchy and titles** from the standards-schema repository; and
- the **coverage, severity, rule IDs, and rule results** from an SRE detailed compliance report.

The generated workbook starts with an explanatory **Introduction** sheet, followed by a **Compliance**
sheet containing one row for every control defined by the selected schema and a `StandardQuestionnaire`
Excel table. A formula-driven **Summary** dashboard contains the source statistics table and pie and
stacked-bar charts.

## Requirements

- Python 3.14+ when running with the repository environment (`pyproject.toml` specifies `>=3.14,<4`).
  The script itself only relies on language features available from Python 3.10.
- `openpyxl>=3,<4`.

`openpyxl` is already declared as a dependency of the repository. Run the script from an activated
project environment, or use `uv run` from the repository root.

## Standard mapping layout

The `--schemas` argument must point to the root directory that contains `standards_schema`:

```text
mappings/
├── README.md
└── standards_schema/
    ├── CIS_Kubernetes_Benchmark.py
    ├── CIS_Controls.py
    ├── NIST.py
    └── ...
```

The schema filename is resolved from the standard name. For example:

| `--standard` value                | Schema file                   | Version block |
|-----------------------------------|-------------------------------|---------------|
| `CIS Kubernetes Benchmark v1.7.0` | `CIS_Kubernetes_Benchmark.py` | `v1.7.0`      |
| `CIS Controls v7`                 | `CIS_Controls.py`             | `v7`          |
| `AWS Config`                      | `AWS_Config.py`               | `null`        |

The standard name is matched case-insensitively in the compliance report and the version suffix is
matched case-insensitively in the schema file. When the standard name carries no version suffix, the
first version block defined in the schema file is used.

Each schema file must define a `full` (or `tech`) dictionary keyed by version string. Schema files are
Python modules and are **executed** when loaded, so `--schemas` must point at a trusted repository.

## Command-line usage

Linux / macOS:

```bash
cd scripts/standard_questionnaire
python ./generate.py \
  --standard "CIS Kubernetes Benchmark v1.7.0" \
  --compliance ./job-compliance.json \
  --schemas ./mappings \
  --output ./output/standard_questionnaire.xlsx
```

Windows PowerShell:

```powershell
cd scripts\standard_questionnaire
python .\generate.py `
  --standard "CIS Kubernetes Benchmark v1.7.0" `
  --compliance .\job-compliance.json `
  --schemas .\mappings `
  --output .\output\standard_questionnaire.xlsx
```

### Arguments

| Argument              | Required | Description                                                                                    |
|-----------------------|---------:|------------------------------------------------------------------------------------------------|
| `--standard NAME`     |      Yes | Full standard name and version, as represented in the compliance report and schema repository. |
| `--compliance FILE`   |      Yes | Path to a detailed SRE compliance report JSON file.                                            |
| `--schemas DIR`       |      Yes | Root of the schema repository; it must contain `standards_schema/`.                            |
| `--output XLSX`, `-o` |       No | Output workbook path. Defaults to `standard_questionnaire.xlsx` in the current directory.      |

Relative paths are resolved against the current directory, not against the directory containing
`generate.py`. Missing parent directories of `--output` are created automatically.

## Compliance report requirements

The report must be the **detailed** JSON report. It must contain per-region standard data, per-control
coverage and severity, and rule result lists. The expected shape is:

```json
{
  "eu-west-1": {
    "CIS Kubernetes Benchmark v1.7.0": {
      "total": 0.72,
      "controls": {
        "1.2.1": {
          "total": 0.75,
          "severity": "High",
          "rules": {
            "successful": [
              "ecc-k8s-001-example"
            ],
            "failed": [],
            "not_evaluated": []
          }
        }
      }
    }
  }
}
```

For the current SRE compliance endpoint, request a detailed JSON report using the next command:

```bash
sre report compliance jobs --job_id <job_id> --detailed --href
```
or tenant-specific:

```bash
sre report compliance accumulated --tenant_name <tenant_name> --detailed --href
```

When the command returns a one-time download URL, save the downloaded JSON content as a file and pass
that file to `--compliance`. The downloaded JSON should contain the region keys at its root, as shown
above.

A non-detailed report containing only standard-level floating-point coverages is insufficient because
it does not contain the control rule lists and severity values needed by the questionnaire.

### Meaning of the report fields

- `controls.<control_id>.total`: control coverage supplied by SRE. The script writes this value to
  Excel and does not recalculate control coverage.
- `controls.<control_id>.severity`: severity supplied by SRE.
- `rules.successful`: rules that completed successfully without a violation.
- `rules.failed`: rules for which at least one resource violated the rule.
- `rules.not_evaluated`: rules that were not evaluated for the scan.

## Multi-region behavior

The report may contain more than one region. The script combines rule lists for matching controls as
follows:

1. Rule lists are unioned across regions.
2. A rule found in `failed` in any region is considered failed.
3. A successful rule is removed if it also appears in the failed set.
4. A not-evaluated rule is removed if it appears in either the failed or successful set.
5. The first non-null control coverage (`total`) and severity encountered across regions are retained.
   Rule results still use the union of all regions.

If coverage or severity differs between regions, the report should be reviewed because the
questionnaire has one row per control and cannot display a separate value for every region.

## Status rules

The status shown in Excel is derived from the rule lists, not from the coverage number:

| Condition                                                                              | Status          |
|----------------------------------------------------------------------------------------|-----------------|
| At least one rule is in `failed`                                                       | `Fail`          |
| No failed rules, at least one rule is in `not_evaluated`, or no successful rules exist | `Not Evaluated` |
| Successful rules exist and there are no failed or not-evaluated rules                  | `Pass`          |

Therefore, one successful rule plus another not-evaluated rule produces `Not Evaluated`, not `Pass`.
Controls that are absent from the compliance report are also reported as `Not Evaluated` with `0%`
coverage.

## Generated workbook

The workbook contains three visible sheets and one hidden helper sheet.

### 1. `Introduction` sheet

The first and active sheet explains the workbook structure, the selected standard and version, the
meaning of report fields, status and coverage calculations, and multi-region aggregation behavior.

### 2. `Compliance` sheet

The second sheet is always named `Compliance` and contains the control hierarchy and compliance values
for the selected standard and version.

Columns:

- section ID and title;
- sub-section IDs and titles, with as many hierarchy levels as the selected schema requires;
- control ID;
- severity;
- status;
- control title;
- report-provided control coverage;
- rule IDs associated with the control (short numeric part, e.g. `001` from `ecc-aws-001-<name>`).

Behavior:

- The data is an Excel Table named `StandardQuestionnaire`.
- Filter dropdowns are enabled on every table header.
- The header row is frozen and column widths are auto-fitted (10–50 characters).
- Coverage cells are numeric values formatted as whole percentages (`0%`).
- Severity colors are conditional-formatting rules based on the current cell text:
  - `High` → `FF3300`
  - `Medium` → `FFCC00`
  - `Low` → `FFFF99`
  - `Info` → `A6C9EC`
- Status colors are also conditional and follow the current status text.
- The two summary rows are outside the table:
  - **SRE Applicable Coverage**: average of coverage values whose status is not `Not Evaluated`
    (`AVERAGEIF`);
  - **Total Coverage**: average of all control coverage values (`AVERAGE`), with not-evaluated controls
    contributing their report value (normally `0%`).

### 3. `Summary` sheet

- A source statistics table near the top (`Fail` / `Pass` / `Not Applicable` / `Total`) broken down by
  `High`, `Medium`, and `Low` severity.
- **Total Coverage** pie chart: Fail / Pass / Not Applicable control counts.
- **Applicable Coverage** bar-of-pie chart: passing controls versus failing controls split by severity.
- **Severity vs Status** and **Status vs Severity** horizontal stacked-bar charts below the coverage
  charts.

### 4. `_ChartData` (hidden)

Source cells for the bar-of-pie chart on the `Summary` sheet. It contains formulas only and can be
ignored; do not delete it while the `Summary` charts are in use.

### Reactivity

Every formula-driven value on the `Summary` sheet is a structured-reference formula
(`COUNTIF`/`COUNTIFS`/`ROWS` over `StandardQuestionnaire[...]`), and the workbook is configured for
full recalculation on load and on save. Editing a control's `Status`, `Severity`, or `Coverage` in the
`Compliance` sheet therefore updates the Summary statistics table, both coverage charts, both horizontal
bar charts, and the coverage totals as soon as Excel recalculates.

Only `High`, `Medium`, and `Low` appear in the dashboard table and charts. `Info` severity is
color-coded in the `Compliance` sheet but is not charted, so severity breakdown rows may not add up
to the control totals if the report uses `Info`.

## Terminal output

On success the script prints the resolved schema file and version, the number of controls loaded from
the schema, the number of controls found in the report, and the aggregate result, for example:

```text
Schema     : CIS_Controls.py  (version v7)
Controls   : 171 controls loaded from schema
Compliance : 46 controls found in report
✓ Saved: standard_questionnaire.xlsx  (171 controls)
  Pass          : 8
  Fail          : 19
  Not Evaluated : 144
  SRE Applicable Coverage : 34%
  Total Coverage          : 7%
```

The printed percentages are calculated from the report-provided control values and match the two
summary rows written to the `Compliance` sheet.
