## Lambda `custodian-job-updater`

This lambda is designed to update Jobs state in `CaaSJobs` DynamoDB table.
Triggered by CloudWatch Rule `custodian-job-state-update`.

### Required configuration:

#### Should have next permission actions:
- Allow: ssm:DeleteParameter
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: dynamodb:*
- Allow: s3:*


#### Trigger event

[Event defined by AWS Batch](https://docs.aws.amazon.com/batch/latest/userguide/batch_cwe_events.html)
