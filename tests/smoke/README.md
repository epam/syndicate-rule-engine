# Syndicate Rule Engine smoke tests

These smoke tests run against a configured Syndicate Rule Engine (SRE)
environment and validate  its general health through the `sre` CLI.

**Note:** smoke tests execute commands via the `sre` CLI entry point (configurable
through `SMOKE_SRE_CLI_ENTRYPOINT`, default `sre`). Install the CLI in the execution
environment before running the suite.

## Layout

```
tests/smoke/
  main.py                 # unified entry point
  requirements.txt         # dependencies for smoke tests
  core/
    commons.py            # Case / Step / Condition framework
    settings.py           # pydantic-settings configuration
    cli.py                  # CLI command helper
  cases/
    main_flow.py            # describe actions and optional job scans
    rules_management.py     # rules & rulesets lifecycle
```

The test case style (`Case`, `Step`, `expectations` dict with JSON paths and
`Condition` objects) is intentionally preserved.

## Configuration

All settings are loaded through `pydantic-settings` with the `SMOKE_SRE_` prefix.

| Variable | Description | Default |
|----------|-------------|---------|
| `SMOKE_SRE_CLI_ENTRYPOINT` | CLI binary name | `sre` |
| `SMOKE_SRE_USERNAME` | SRE username | — |
| `SMOKE_SRE_PASSWORD` | SRE password | — |
| `SMOKE_SRE_CUSTOMER` | Customer name | — |
| `SMOKE_SRE_API_LINK` | SRE API link | `http://0.0.0.0:8000/caas` |
| `SMOKE_SRE_SYSTEM_CUSTOMER` | Licensed rulesets system customer | `CUSTODIAN_SYSTEM` |
| `SMOKE_SRE_TEST_DELAY` | Delay in seconds after each step | `0` |
| `SMOKE_SRE_{AWS\|AZURE\|GCP}_RULE_SOURCE_*` | Rule source settings for rules management | — |

Rule source variables per cloud:

- `SMOKE_SRE_{CLOUD}_RULE_SOURCE_PID` (required to enable cloud in rules management)
- `SMOKE_SRE_{CLOUD}_RULE_SOURCE_SECRET`
- `SMOKE_SRE_{CLOUD}_RULE_SOURCE_REF` (default `main`)
- `SMOKE_SRE_{CLOUD}_RULE_SOURCE_URL` (default `https://api.github.com`)
- `SMOKE_SRE_{CLOUD}_RULE_SOURCE_PREFIX` (default `policies/`)

## Running

Smoke modules import the package as `smoke.*`. Add `tests/` to `PYTHONPATH`
(do not add a `tests/__init__.py` — that would break the rest of the test suite).

Install required dependencies:

```bash
pip install -r tests/smoke/requirements.txt
```

## Main flow

Checks authentication, describe actions for core entities, and optionally
submits scan jobs for specified tenants.

```bash
export SMOKE_SRE_USERNAME=admin
export SMOKE_SRE_PASSWORD=secret
export SMOKE_SRE_API_LINK=http://127.0.0.1:8000/caas

PYTHONPATH=tests uv run python -m smoke.main main_flow \
  --tenants TEST_TENANT:eu-west-1,eu-west-2 TEST_TENANT2
```

This runs describe checks for all listed tenants and submits jobs: one for
`TEST_TENANT` in `eu-west-1` and `eu-west-2`, and one for `TEST_TENANT2` in
all configured regions.

## Rules management flow

Checks rules and rulesets lifecycle. Requires at least one configured rule
source (`SMOKE_SRE_*_RULE_SOURCE_PID`).

```bash
export SMOKE_SRE_USERNAME=test
export SMOKE_SRE_PASSWORD=password
export SMOKE_SRE_CUSTOMER=TEST
export SMOKE_SRE_API_LINK=http://127.0.0.1:8000/caas

export SMOKE_SRE_AWS_RULE_SOURCE_PID=epam/ecc-aws-rulepack
export SMOKE_SRE_AWS_RULE_SOURCE_REF=main
export SMOKE_SRE_AWS_RULE_SOURCE_PREFIX=policies/

PYTHONPATH=tests uv run python -m smoke.main rules_management
```

## Linting

From the repository root:

```bash
uv sync --group test --group lint
uv run ruff format tests/smoke
uv run ruff check tests/smoke
uv run mypy tests/smoke
```

## Reports

Each run writes a markdown report in the current directory
(`smoke-report-YYYY-MM-DD.md` by default). Use `--filename` to override the
output path.
