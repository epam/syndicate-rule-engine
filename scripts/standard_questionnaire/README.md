# Standard Questionnaire Generator

`generate.py` generates a single-sheet Excel questionnaire by combining:

- the **control hierarchy and titles** from the standards-schema repository; and
- the **coverage, severity, rule IDs, and rule results** from an SRE detailed compliance report.

The generated workbook contains one row for every control defined by the selected schema.

## Requirements

- Python 3.14+ when running with the repository environment (`pyproject.toml` specifies `>=3.14,<4`).
- `openpyxl>=3,<4`.

`openpyxl` is already declared as a dependency of the repository. Run the script from an activated project environment, or use `uv run` from the repository root.

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

The standard name is matched case-insensitively in the compliance report and the version suffix is matched case-insensitively in the schema file.

## Command-line usage

```bash
cd scripts\standard_questionnaire && \
python .\generate.py && \
  --standard "CIS Kubernetes Benchmark v1.7.0" && \
  --compliance .\job-compliance.json && \
  --schemas .\mappings && \
  --output .\output\standard_questionnaire.xlsx
```

### Arguments

| Argument              | Required | Description                                                                                    |
|-----------------------|---------:|------------------------------------------------------------------------------------------------|
| `--standard NAME`     |      Yes | Full standard name and version, as represented in the compliance report and schema repository. |
| `--compliance FILE`   |      Yes | Path to a detailed SRE compliance report JSON file.                                            |
| `--schemas DIR`       |      Yes | Root of the schema repository; it must contain `standards_schema/`.                            |
| `--output XLSX`, `-o` |       No | Output workbook path. Defaults to `standard_questionnaire.xlsx` in the current directory.      |

Relative paths are resolved against the current directory, not against the directory containing `generate.py`.

## Compliance report requirements

The report must be the **detailed** JSON report. It must contain per-region standard data, per-control coverage and severity, and rule result lists. The expected shape is:

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

When the command returns a one-time download URL, save the downloaded JSON content as a file and pass that file to `--compliance`. The downloaded JSON should contain the region keys at its root, as shown above.

A non-detailed report containing only standard-level floating-point coverages is insufficient because it does not contain the control rule lists and severity values needed by the questionnaire.

### Meaning of the report fields

- `controls.<control_id>.total`: control coverage supplied by SRE. The script writes this value to Excel and does not recalculate control coverage.
- `controls.<control_id>.severity`: severity supplied by SRE.
- `rules.successful`: rules that completed successfully without a violation.
- `rules.failed`: rules for which at least one resource violated the rule.
- `rules.not_evaluated`: rules that were not evaluated for the scan.

## Multi-region behavior

The report may contain more than one region. The script combines rule lists for matching controls as follows:

1. Rule lists are unioned across regions.
2. A rule found in `failed` in any region is considered failed.
3. A successful rule is removed if it also appears in the failed set.
4. A not-evaluated rule is removed if it appears in either the failed or successful set.
5. The first non-null control coverage (`total`) and severity encountered across regions are retained. Rule results still use the union of all regions.

If coverage or severity differs between regions, the report should be reviewed because the questionnaire has one row per control and cannot display a separate value for every region.

## Status rules

The status shown in Excel is derived from the rule lists, not from the coverage number:

| Condition                                                                              | Status          |
|----------------------------------------------------------------------------------------|-----------------|
| At least one rule is in `failed`                                                       | `Fail`          |
| No failed rules, at least one rule is in `not_evaluated`, or no successful rules exist | `Not Evaluated` |
| Successful rules exist and there are no failed or not-evaluated rules                  | `Pass`          |

Therefore, one successful rule plus another not-evaluated rule produces `Not Evaluated`, not `Pass`.

## Generated workbook

The workbook contains a sheet named **Standard Questionnaire** with:

- section ID and title;
- sub-section IDs and titles, with as many hierarchy levels as the selected schema requires;
- control ID;
- severity;
- status;
- control title;
- report-provided control coverage;
- rule IDs associated with the control.

Additional workbook behavior:

- The data is an Excel Table named `StandardQuestionnaire`.
- Filter dropdowns are enabled on every table header.
- The header row is frozen.
- Coverage cells are numeric values formatted as whole percentages (`0%`).
- Severity colors are conditional-formatting rules based on the current cell text:
  - `High` → `FF3300`
  - `Medium` → `FFCC00`
  - `Low` → `FFFF99`
  - `Info` → `A6C9EC`
- Status colors are also conditional and follow the current status text.
- The two summary rows are outside the table:
  - **SRE Applicable Coverage**: average of coverage values whose status is not `Not Evaluated`;
  - **Total Coverage**: average of all control coverage values, with not-evaluated controls contributing their report value (normally `0%`).
- Summary values use structured Excel formulas and the workbook is configured for automatic recalculation. Editing a control's coverage in Excel updates the summaries when Excel recalculates the workbook.

The terminal output also displays the current aggregate SRE Applicable and Total coverage percentages calculated from the report-provided control values.
