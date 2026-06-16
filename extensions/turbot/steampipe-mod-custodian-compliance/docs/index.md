# Custodian Compliance (SRE) — Powerpipe Mod

Powerpipe mod for Kubernetes compliance dashboards based on SRE Custodian scan results.

## Purpose

The mod transforms data from the `syndicate-rule-engine` Steampipe plugin tables into **controls** and **benchmarks** compatible with the Turbot/Steampipe compliance mod approach (CIS, NSA/CISA).

Rule source: YAML metadata from `ecc-kubernetes-metadata` (on-prem K8s rules `ecc-k8s-*`).

## Benchmarks

| Benchmark | Mod path | Description |
|-----------|----------|-------------|
| All Controls | `custodian_compliance.benchmark.all_controls` | All ~82 controls grouped by `service` (Pod, API Server, …) |
| CIS v1.7.0 | `custodian_compliance.benchmark.cis_v170` | ~69 controls mapped to CIS Kubernetes Benchmark v1.7.0 |
| CIS v1.20 | `custodian_compliance.benchmark.cis_v120` | ~21 controls from CIS GKE Benchmark v1.2.0 (Kubernetes v1.20) |
| NSA/CISA | `custodian_compliance.benchmark.nsa_cisa_v1` | Controls grouped by `service_section` (API Server, Kubelet, Pod, …) |

Run:

```bash
powerpipe benchmark run custodian_compliance.benchmark.cis_v170 --var-file powerpipe.ppvars
```

HTML report:

```bash
powerpipe benchmark run custodian_compliance.benchmark.all_controls --var-file powerpipe.ppvars --export benchmark.html
```

## Variables

```hcl
variable "job_id" {
  # UUID of a K8s scan job (SUCCEEDED)
}

variable "customer_id" {
  # customer scope for system users
}
```

See `powerpipe.ppvars.example`.

## How a control works

Each control is a SQL query against `sre_finding` and `sre_rule_result` with a fixed `rule_name` / `policy`.

Three possible row statuses (Powerpipe aggregate):

- **alarm** — violation found
- **ok** — compliant (logic depends on cluster vs workload)
- **error** — rule did not execute (`error_type` in rules report)

Detailed logic — [root README](../../README.md#compliance-control-logic).

## Generation

Files in `generated/` are produced by `scripts/generate_mapping.py`:

```bash
python3 scripts/generate_mapping.py
```

After adding new `ecc-k8s-*` rules to metadata, regenerate before running benchmarks.

## Control tags

Base tags (`variables.pp` → `local.common_tags`):

```hcl
category = "Compliance"
plugin   = "syndicate-rule-engine"
service  = "Kubernetes"
type     = "Control"
```

For CIS v1.7.0, `cis_item_id` and `cis_version = "v1.7.0"` are added.

## Dependencies

- Steampipe plugin `syndicate-rule-engine` with a configured connection
- A completed K8s platform scan job in SRE

Full setup guide: [../../README.md](../../README.md).
