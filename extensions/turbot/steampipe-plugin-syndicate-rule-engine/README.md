# steampipe-plugin-syndicate-rule-engine

Steampipe plugin for reading Syndicate Rule Engine (SRE) data via the REST API.

## Tables

### `sre_customer`

Customer list. Useful for system users when building dashboards with scope selection.

### `sre_tenant`

| Column | API source |
|--------|------------|
| `name` | tenant name |
| `customer_id` | `customer_name` (API has no separate customer_id) |
| `customer_name` | `customer_name` |
| `account_id` | `account_id` |
| `is_active` | `is_active` |

Qual: `customer_id` (optional).

### `sre_platform`

K8s platforms from `/platforms/k8s`.

Qual: `customer_id`, `tenant_name` (optional).

### `sre_job`

Scan jobs from `/jobs`. The plugin filters to `status = 'SUCCEEDED'` only. Additional `platform_id` filtering is done in the plugin (post-filter).

Qual: `customer_id`, `tenant_name`, `platform_id` (optional).

### `sre_finding`

Flat list of violations. One row = one rule violation on a resource.

**Required qual:** `job_id`  
**Optional qual:** `customer_id`

Columns: `rule_name`, `description`, `severity`, `region`, `resource_id`, `resource_name`, `namespace`, `kind`.

For platform jobs, findings are assembled from the resource report (see `sre/reports.go` → `findingsFromResourceItems`).

### `sre_rule_result`

Per-rule execution summary from `/reports/rules/jobs/{id}`.

Columns: `policy`, `region`, `succeeded`, `scanned_resources`, `failed_resources`, `error_type`, `reason`.

Used by the mod for `error` status and `ok` row counts on workload controls.

### `sre_resource_result`

Per-resource results with violated rules (JSON).

Columns: `platform_id`, `resource_type`, `region`, `resource_id`, `resource_name`, `namespace`, `violated_rules`, `violation_count`, `status` (`ok` / `alarm`).

## Connection config

```hcl
connection "syndicate_rule_engine" {
  plugin = "syndicate-rule-engine"

  api_url     = "https://host/api/v1"   # optional if a default exists
  username    = "..."
  password    = "..."
  customer_id = "..."                   # optional, for system users
}
```

| Field | Description |
|-------|-------------|
| `api_url` | SRE API base URL (no trailing slash) |
| `username` / `password` | Credentials for `/signin` |
| `customer_id` | Default customer scope (`customer_id` query param in API) |

## API endpoints (internal implementation)

| Operation | Method | Path |
|-----------|--------|------|
| Sign in | POST | `/signin` |
| Refresh | POST | `/refresh` |
| Customers | GET | `/customers` |
| Tenants | GET | `/tenants?customer_id=` |
| K8s platforms | GET | `/platforms/k8s?tenant_name=` |
| Jobs | GET | `/jobs?tenant_name=&status=` |
| Job detail | GET | `/jobs/{id}?customer_id=` |
| Findings report | GET | `/reports/findings/jobs/{id}?customer_id=&job_types=` |
| Rules report | GET | `/reports/rules/jobs/{id}?...` |
| Resources report | GET | `/reports/resources/jobs/{id}?...` |

Authorization: `access_token` value sent directly in the `Authorization` header.

## Build

```bash
go build -o steampipe-plugin-syndicate-rule-engine.plugin .
```

Go module: `github.com/epam/steampipe-plugin-syndicate-rule-engine`  
SDK: `github.com/turbot/steampipe-plugin-sdk/v5`

## Development

Package layout:

- `connection/` — HCL schema, singleton SRE client per connection
- `sreplugin/sre/` — HTTP transport, auth, caching, API decoding
- `sreplugin/tables/catalog/` — list tables without job scope
- `sreplugin/tables/reports/` — job-scoped report tables
- `sreplugin/tables/common/` — shared quals, row mapping, streaming

`decodeList` / `decodeMapContent` in `envelope.go` support multiple SRE API JSON response formats (`items` top-level, `data.items`, `data.content`).

Full integration documentation: [../README.md](../README.md).
