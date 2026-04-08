# Event-Driven Supported Events

Short overview of the Event-Driven flow in SRE.

## 1) Incoming API

Events are received via `POST /event`.

Minimal request wrapper:

```json
{
  "version": "1.0.0",
  "vendor": "AWS | MAESTRO | SRE_K8S_AGENT | SRE_K8S_WATCHER",
  "events": [ ... ]
}
```

## 2) Supported event formats

### AWS (`vendor = "AWS"`)

EventBridge/CloudTrail-style event is expected. Fields used for processing:

- `detail-type` must be `AWS API Call via CloudTrail`, otherwise the event is rejected
- `detail.userIdentity.accountId`
- `detail.awsRegion`
- `detail.eventSource`
- `detail.eventName`

Example:

```json
{
  "version": "1.0.0",
  "vendor": "AWS",
  "events": [
    {
      "detail": {
        "userIdentity": { "accountId": "123456789012" },
        "awsRegion": "eu-central-1",
        "eventSource": "ec2.amazonaws.com",
        "eventName": "RunInstances"
      }
    }
  ]
}
```

### MAESTRO (`vendor = "MAESTRO"`)

Unified Maestro events are expected. Fields used for processing:

- `tenantName`
- `regionName`
- `eventMetadata.eventSource`
- `eventMetadata.eventName`
- `eventMetadata.cloud`

Example:

```json
{
  "version": "1.0.0",
  "vendor": "MAESTRO",
  "events": [
    {
      "tenantName": "GOOGLE_ACCOUNT",
      "regionName": "global",
      "eventMetadata": {
        "cloud": "GOOGLE",
        "eventSource": "container.googleapis.com",
        "eventName": "google.container.v1.ClusterManager.CreateNodePool"
      }
    }
  ]
}
```
### SRE K8S Agent (`vendor = "SRE_K8S_AGENT"`)

SRE K8S Agent events are expected. Fields used for processing:

- `platformId`
- `type`
- `reason`

Example:

```json
{
  "version": "1.0.0",
  "vendor": "SRE_K8S_AGENT",
  "events": [
    {
      "type": "Pod",
      "reason": "PodDeleted",
      "platformId": "5d18b222-9aa5-454f-8736-587cc39de3f8"
    }
  ]
}
```

### SRE K8S Watcher (`vendor = "SRE_K8S_WATCHER"`)

Events produced by the built-in K8s watch connector (pull model). The
`event_sources_consumer` opens a persistent `GET /api/v1/events?watch=true`
connection to each registered K8s cluster and translates the raw watch stream
into the same shape as `SRE_K8S_AGENT` events.

Fields used for processing:

- `platformId` — resolved from the event source configuration
- `type` — `involvedObject.kind` from the K8s watch event
- `reason` — `reason` from the K8s watch event

Example:

```json
{
  "version": "1.0.0",
  "vendor": "SRE_K8S_WATCHER",
  "events": [
    {
      "type": "Pod",
      "reason": "Scheduled",
      "platformId": "5d18b222-9aa5-454f-8736-587cc39de3f8"
    }
  ]
}
```
