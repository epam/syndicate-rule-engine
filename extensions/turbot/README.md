# Turbot / Steampipe integration with Syndicate Rule Engine

PoC integration of SRE Custodian with the [Turbot Pipes](https://turbot.com/pipes) / [Powerpipe](https://powerpipe.io) ecosystem for Kubernetes compliance reporting.

K8s cluster scan data from the SRE API is exposed as Steampipe tables; the Powerpipe mod turns them into CIS/NSA-CISA benchmarks and dashboards.

## Architecture

```
┌─────────────────────┐     REST API      ┌──────────────────────────┐
│  Syndicate Rule     │ ◄──────────────── │ steampipe-plugin-        │
│  Engine (SRE)       │   /signin, /jobs, │ syndicate-rule-engine    │
│  K8s scan job       │   /reports/...    │ (Go plugin)              │
└─────────────────────┘                   └────────────┬─────────────┘
                                                         │ SQL tables
                                                         ▼
                                              ┌──────────────────────────┐
                                              │ Powerpipe / Steampipe    │
                                              │ steampipe-mod-custodian- │
                                              │ compliance               │
                                              └────────────┬─────────────┘
                                                         │ benchmarks
                                                         ▼
                                              CIS v1.7.0, CIS v1.20,
                                              NSA/CISA, All Controls
```

### Components

| Path | Purpose |
|------|---------|
| `steampipe-plugin-syndicate-rule-engine/` | Go plugin: SRE API connection, 7 SQL tables |
| `steampipe-mod-custodian-compliance/` | Powerpipe mod: ~82 controls, 4 benchmarks, generated from `ecc-kubernetes-metadata` |

The mod references metadata from the `metadata/ecc-kubernetes-metadata` repository.

## Prerequisites

- [Steampipe](https://steampipe.io/downloads) or [Powerpipe](https://powerpipe.io/downloads) (Powerpipe recommended for benchmarks)
- Go 1.23+
- Python 3 + PyYAML
- Access to the SRE API (DEV/STAGE/PROD) with user credentials
- A **completed K8s scan job** in `SUCCEEDED` status linked to a platform (K8s)

## Quick start

### 1. Build and install the plugin

```bash
cd extensions/turbot/steampipe-plugin-syndicate-rule-engine
go build -o ~/.steampipe/plugins/hub.steampipe.io/plugins/epam/syndicate-rule-engine/latest/steampipe-plugin-syndicate-rule-engine.plugin .
```

Alternative for local development:

```bash
mkdir -p ~/.steampipe/plugins/local/syndicate-rule-engine
go build -o ~/.steampipe/plugins/local/syndicate-rule-engine/steampipe-plugin-syndicate-rule-engine.plugin .
```

### 2. Connection configuration

File `~/.steampipe/config/syndicate_rule_engine.spc` (connection name is arbitrary):

```hcl
connection "syndicate_rule_engine" {
  plugin = "syndicate-rule-engine"   # or "local" for a local build

  api_url    = "https://sre-dev.example.com/api/v1"
  username   = "your-user"
  password   = "your-password"

  # Required for system users (see below)
  customer_id = "customer"
}
```

Restart the service after changing config:

```bash
steampipe service restart
# or for Powerpipe:
powerpipe service restart
```

### 3. Verify tables

```bash
steampipe query "select id, status, platform_id, submitted_at from sre_job where status = 'SUCCEEDED' limit 5"
```

### 4. Run a compliance benchmark

```bash
cd extensions/turbot/steampipe-mod-custodian-compliance

# Copy and edit variables
cp powerpipe.ppvars.example powerpipe.ppvars
# Set the UUID of a successful K8s job

powerpipe benchmark run custodian_compliance.benchmark.cis_v170 --var-file powerpipe.ppvars
```

Available benchmarks:

- `custodian_compliance.benchmark.all_controls` — all SRE K8s controls grouped by service
- `custodian_compliance.benchmark.cis_v170` — CIS Kubernetes Benchmark v1.7.0 (~69 controls)
- `custodian_compliance.benchmark.cis_v120` — CIS Kubernetes v1.20 / GKE v1.2.0 (~21 controls)
- `custodian_compliance.benchmark.nsa_cisa_v1` — NSA/CISA Hardening Guidance grouped by `service_section`

## Mod variables (`powerpipe.ppvars`)

| Variable | Description |
|----------|-------------|
| `job_id` | UUID of a scan job with `SUCCEEDED` status (required) |
| `customer_id` | Customer scope for system users; leave empty for tenant users |

Example:

```hcl
job_id      = "25245391-5832-4218-b133-4fe60f68aa55"
customer_id = "customer"
```

`customer_id` in the mod mirrors the connection-level `customer_id` for control SQL queries (`${var.job_id}`). Tenant users typically leave it empty.

## Steampipe plugin: tables

### Catalog (scope selection for dashboards)

| Table | Description | Key qual filters |
|-------|-------------|------------------|
| `sre_customer` | Customer list (system users) | — |
| `sre_tenant` | Tenants | `customer_id` (optional) |
| `sre_platform` | K8s platforms | `customer_id`, `tenant_name` (optional) |
| `sre_job` | Scan jobs | `customer_id`, `tenant_name`, `platform_id` (optional); `SUCCEEDED` only |

### Reports (job-scoped)

All report tables require the `job_id` qual. `customer_id` is an optional qual or comes from the connection.

| Table | Description |
|-------|-------------|
| `sre_finding` | Violations: rule → resource (flat list) |
| `sre_rule_result` | Per-rule execution stats: scanned/failed, errors |
| `sre_resource_result` | Per-resource result + `violated_rules` JSON |

Example:

```sql
select rule_name, count(*) as violations
from sre_finding
where job_id = '25245391-5832-4218-b133-4fe60f68aa55'
group by rule_name
order by violations desc;
```

## Compliance control logic

Each control is generated from `ecc-k8s-*_metadata.yml` and uses SQL with three branches (`UNION ALL`):

| Powerpipe status | Condition |
|------------------|-----------|
| `alarm` | A row exists in `sre_finding` for `rule_name` = policy |
| `ok` | No finding and no execution error in `sre_rule_result` |
| `error` | `error_type` is set in `sre_rule_result` for the policy |

### Cluster-level vs workload-level

The type is determined by the `service_section` field in metadata:

**Cluster-level** (`API Server`, `etcd`, `Controller Manager`, `Scheduler`, `Kubelet`):

- One `ok` row for the whole cluster when there are no findings
- `alarm` — one row per violating resource (usually a single cluster resource)

**Workload-level** (Pod, Deployment, Namespace, …):

- `ok` rows are generated via `generate_series`: count = `sum(scanned_resources) - sum(failed_resources)` from `sre_rule_result`
- This approximates “how many resources passed” for dashboard scoring

> **Note:** if `scanned_resources` / `failed_resources` are missing from the API, a workload control may show 0 `ok` rows even without findings. Check `sre_rule_result` for problematic rules.

### Rule name mapping

- **Policy name** in SRE = metadata filename without `_metadata.yml` (e.g. `ecc-k8s-001-apiserver_anonymous_auth_argument_is_set_to_false`)
- **Control ID** in Powerpipe = `safe_name(policy)` — hyphens → underscores, lowercase

Names in control SQL and in the findings API **must match exactly**.

## SRE API: nuances and limitations

### Authentication

- `POST /signin` with `username` / `password` → `access_token`, `refresh_token`
- Token sent in the `Authorization` header (no `Bearer` prefix)
- On `401` — automatic refresh or re-signin
- Client is cached at the Steampipe connection level

### `customer_id`

The SRE API uses the `customer_name` field as the customer identifier for tenants/jobs. The plugin column is named `customer_id`, but the value is the **customer name** from the API.

For **system users**, `customer_id` is required in:

- connection config
- query quals (if not set in the connection)
- `powerpipe.ppvars` for the mod

Without it, the API returns an error or empty data.

### Platform jobs (K8s)

When a job has `platform_id` (typical K8s scan):

- Findings are built from the **resource report** (`/reports/resources/jobs/{id}`), not the separate findings endpoint
- `violated_rules` from each resource are aggregated into a rule → resources map
- Empty region → `global`

For non-platform jobs, `/reports/findings/jobs/{id}` is used.

### `job_types` in report URLs

The plugin adds query `job_types` = the job’s actual type plus `standard`, `scheduled`, `reactive` — for compatibility with different SRE job types.

### Caching

- HTTP API responses are cached in the client memory (per path + customer_id)
- Job metadata is cached separately
- Repeated queries in the same Steampipe session do not hit the API again

After a new scan with the same job_id (rare) or for fresh data — run `steampipe service restart`.

## Regenerating controls

After changes in `ecc-kubernetes-metadata`:

```bash
cd extensions/turbot/steampipe-mod-custodian-compliance

# Default — metadata/on-prem in ecc-kubernetes-metadata
python3 scripts/generate_mapping.py

# Custom path:
METADATA_DIR=/path/to/metadata/on-prem python3 scripts/generate_mapping.py
```

Generates in `generated/`:

- `controls.pp` — controls + queries
- `benchmark_*.pp` — benchmark hierarchy
- `mapping.json` — intermediate JSON for mapping audit

**Do not edit `generated/` manually** — changes will be overwritten.

### What the generator reads from metadata

| Metadata field | Usage |
|----------------|-------|
| `standard.CIS Kubernetes Benchmark` | Mapping to CIS v1.7.0 (format `v1.7.0 (1.2.1, ...)`) |
| `standard.CIS GKE Benchmark` | Mapping to CIS v1.20 |
| `service_section` | Cluster vs workload SQL; NSA/CISA grouping |
| `service` | Grouping in `all_controls` |
| `article` (first line) | Control `description` |

CIS tags (`cis_item_id`, `cis_version`) are added only for rules with CIS v1.7.0.

## Common scenarios

### Find the latest successful K8s job

```sql
select id, tenant_name, platform_id, submitted_at, rulesets
from sre_job
where customer_id = 'customer'
  and platform_id is not null
  and platform_id != ''
order by submitted_at desc
limit 1;
```

### Diagnose a rule with error status

```sql
select policy, error_type, reason, scanned_resources, failed_resources
from sre_rule_result
where job_id = '<job-uuid>'
  and nullif(error_type, '') is not null;
```

### Compare findings vs rule stats

```sql
select
  f.rule_name,
  count(*) as finding_rows,
  r.scanned_resources,
  r.failed_resources
from sre_finding f
left join sre_rule_result r
  on r.job_id = f.job_id and r.policy = f.rule_name
where f.job_id = '<job-uuid>'
group by f.rule_name, r.scanned_resources, r.failed_resources;
```

## Troubleshooting

| Symptom | Possible cause | Action |
|---------|----------------|--------|
| `required qual "job_id" is missing` | Report table query without `job_id` | Add `where job_id = '...'` |
| `SRE signin failed` | Wrong URL / credentials | Check `api_url`, username/password |
| `401` after a long session | Expired token | Restart service; plugin refreshes automatically |
| Empty findings despite violations in UI | Wrong `customer_id` | Check scope for system user |
| Control always `error` | Rule did not run on scan | See `sre_rule_result.error_type` |
| Control `alarm` but UI shows passed | Different job_id or stale cache | Check `job_id`, restart service |
| Empty CIS v1.20 benchmark | Rule has no GKE mapping | Expected — only rules with `CIS GKE Benchmark v1.2.0` |
| `Metadata directory not found` on generate | Missing ecc-kubernetes-metadata | Clone the repo or set `METADATA_DIR` |

## PoC status / known limitations

- Integration is developed as a **PoC for Kubernetes reporting** (see scope 5.20.0)
- Plugin is not published to Steampipe Hub — local build only
- `mod.pp` references `docs/index.md` — mod details in [steampipe-mod-custodian-compliance/docs/index.md](./steampipe-mod-custodian-compliance/docs/index.md)
- Plugin implementation details — [steampipe-plugin-syndicate-rule-engine/README.md](./steampipe-plugin-syndicate-rule-engine/README.md)
- Dashboard UI (Turbot Pipes) — separate step after validating benchmarks locally
- AWS / other platforms — out of scope for this mod (K8s on-prem metadata only)
- `powerpipe.ppvars` in the repo may contain real job_id values — do not commit production credentials

## Directory structure

```
extensions/turbot/
├── README.md                          ← this document
├── steampipe-plugin-syndicate-rule-engine/
│   ├── main.go
│   ├── connection/config.go           ← HCL config + client cache
│   └── sreplugin/
│       ├── plugin.go                  ← table registration
│       ├── sre/                       ← HTTP client, auth, API paths
│       └── tables/
│           ├── catalog/               ← customer, tenant, platform, job
│           ├── reports/               ← finding, rule_result, resource_result
│           └── common/                ← quals, flatten, streaming
└── steampipe-mod-custodian-compliance/
    ├── mod.pp
    ├── variables.pp
    ├── scripts/generate_mapping.py
    ├── generated/                     ← auto-generated PP files
    └── docs/index.md
```

## Related repositories

- `syndicate-rule-engine` — SRE API and scan engine
- `metadata/ecc-kubernetes-metadata` — rule source and CIS mapping
- [Steampipe plugin SDK](https://github.com/turbot/steampipe-plugin-sdk)
- [Powerpipe mod development](https://powerpipe.io/docs/build)
