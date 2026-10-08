### Syndicate Rule Engine

Syndicate Rule Engine is a solution that allows checking and assessing virtual infrastructures in AWS, Azure, GCP clouds and Kubernetes clusters against different types of standards, requirements and rulesets.
By default, the solution covers hundreds of security, compliance, utilization and cost-effectiveness rules, which cover world-known standards like GDPR, PCI DSS, CIS Benchmark, and a bunch of others.

### Notice

All the technical details described below are actual for the particular version, or a range of versions of the software.

### Actual for versions: 5.22.0

## Deployment options

| Option | Purpose | Documentation |
|--------|---------|---------------|
| **AWS AMI** (Minikube on EC2) | Main deployment flow | [deployment/aws-ami](deployment/aws-ami/docs/main.md) |
| **Kubernetes Helm** | Custom deployments | [deployment/helm](deployment/helm/README.md) |
| **Docker Compose** | Evaluation / development purposes | [deployment/compose](deployment/compose/README.md) |

See also [QUICKSTART](QUICKSTART.md), [architecture diagrams](docs/architecture.md)
and [data flow diagrams](docs/data_flows.md).

## Components

| Component | Description |
|-----------|-------------|
| `rule-engine` (API) | Gunicorn-based REST API (`src/main.py`) that handles all API resources: jobs, scheduled jobs, events, accounts/tenants, rulesets, rule sources, reports, integrations, etc. |
| `celeryworker` | Celery worker that executes scan jobs and background tasks |
| `celerybeat` | Celery beat that triggers periodic tasks: `assemble-events`, `clear-events`, `sync-license`, `collect-metrics`, `make-findings-snapshots`, `scan-resources`, `process-interval-reports`, `process-periodic-rules`, etc. Schedules are configured with `SRE_CELERY_*_SCHEDULE` environment variables |
| `event-sources-consumer` | Pulls events from the configured event sources (AWS SQS, Kubernetes watch) and ingests them for event-driven scans |
| MongoDB, MinIO, Vault, Valkey | Database, object storage, secrets storage and cache/broker |
| `sre` CLI | Command line client for the API ([cli](cli/README.md)) |

## Rules format
Each rule file in the repository must be in the following format:

```yaml
policies:
  - name: name
    description: description
    metadata:
      version: version
      cloud: AWS/GCP/Azure
      source: source
      article: article
      remediation: remediation
      service_section: service_section
      standard:
        standard_name_1:
          - point 1
          - point 2
        standard_name_2:
          - point 1
          - point 2
          - point 3
    some more: content
          - and: more
```

All fields are required.

## Tests
To run tests use the command below:
```bash
pytest tests/
```


## Event-Driven scans
If there is no need to scan the entire cloud account, but only certain resources and only after their changes
(for example, an EC2 instance was created, the content of an S3 bucket was updated, a Kubernetes pod was deleted, etc.),
then the solution is event-driven scans. Event-driven scans use rulesets that have the `event_driven` field set to `true`
and require the Event-Driven feature to be enabled in the license.

### Flow
1. **Ingestion.** Events reach SRE in one of two ways:
   * **Push** - a client sends events to the `POST /event` endpoint in the format
     `{"version": "1.0.0", "vendor": "...", "events": [...]}`. Supported vendors: `AWS`, `MAESTRO`,
     `SRE_K8S_AGENT`, `SRE_K8S_WATCHER`. The response (`202 Accepted`) contains the number of
     `received`, `saved` and `rejected` events.
   * **Pull** - the `event-sources-consumer` service reads events from the registered event sources:
     an **AWS SQS** queue (the queue is accessed using the `role_arn` of the source, if specified) or a
     **Kubernetes** cluster (a watch on cluster events). Event sources are managed via the
     `/integrations/event-sources` API resource or the `sre integrations event sources add|describe|update|delete` commands.
2. **Storage.** Normalized events are saved to the `SREEvents` collection.
3. **Assembling.** The periodic task `assemble-events` (every 5 minutes by default,
   `SRE_CELERY_ASSEMBLE_EVENTS_SCHEDULE`) groups the accumulated events by tenant/platform and region,
   maps them to rules and submits event-driven jobs that scan only the affected resources.
4. **Cleaning.** The periodic task `clear-events` (daily at 00:00 UTC by default,
   `SRE_CELERY_CLEAR_EVENTS_SCHEDULE`) removes events that have already been assembled.

Refer to [Syndicate Rule Engine User Guide](docs/sre_user_guide.md) for the supported event formats and to the
[Event Driven Scan DFD](docs/assets/dfd_event_driven_scan.png) for the data flow.
