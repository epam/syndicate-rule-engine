# Event-Driven Supported Events

Short overview of the Event-Driven flow in SRE.

## 1) Incoming API

Events are received via `POST /event`.

Minimal request wrapper:

```json
{
  "version": "1.0.0",
  "vendor": "AWS | MAESTRO",
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
