# Syndicate Rule Engine CLI

`sre` is a command line client for the Syndicate Rule Engine REST API. It allows to manage customers, tenants, rulesets,
licenses and integrations, to run scans and to retrieve reports.

## Install

Python `>=3.14` is required. From the root of the repository:

    pip install cli/

(or `make install-cli` to install it in editable mode). The `sre` command becomes available.

## Configure and log in

    sre configure --api_link <Syndicate Rule Engine host>

    sre login --username <username> --password <password>

The password is requested interactively if `--password` is omitted. Other basic commands:

- `sre whoami` - shows the current user
- `sre show_config` - shows the CLI configuration
- `sre health_check` - checks the availability of Syndicate Rule Engine components
- `sre cleanup --confirm` - removes all the configuration data stored by the tool

## Command groups

| Group          | Purpose                                                                          |
|----------------|----------------------------------------------------------------------------------|
| `customer`     | Customer entities                                                                |
| `tenant`       | Tenant entities                                                                  |
| `role`         | Roles                                                                            |
| `policy`       | Policies                                                                         |
| `users`        | API users (system customer only)                                                 |
| `job`          | Scans (`job scan aws\|azure\|gcp\|k8s`), job status, scheduled and event-driven jobs |
| `report`       | Reports                                                                          |
| `resource`     | Resources                                                                        |
| `rule`         | Rules                                                                            |
| `ruleset`      | Rulesets                                                                         |
| `rulesource`   | Rule sources                                                                     |
| `license`      | Licenses                                                                         |
| `setting`      | Settings                                                                         |
| `metadata`     | Locally stored metadata                                                          |
| `metrics`      | Scan and tenant metrics                                                          |
| `platform`     | Kubernetes platforms                                                             |
| `integrations` | Integrations                                                                     |
| `service`      | Services                                                                         |

Run `sre --help` or `sre <group> --help` to see the commands and options available in your version.

Use `sre report questionnaire standards` to list the questionnaire standards
and versions available to the configured customer, grouped by cloud or
platform domain. If a requested standard is not compatible with a job or
tenant, the report error points to this command. Use `--domain` to filter to
one or more domains:

    sre report questionnaire standards --domain AWS
    sre report questionnaire standards --domain AWS --domain KUBERNETES

## Commands reference

The full description of all the commands, their parameters and examples is in
the [SRE commands reference guide](../docs/sre_commands_reference_guide.pdf).
