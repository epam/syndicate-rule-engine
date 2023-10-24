## custodian-report-api-handler

This lambda generates statistics reports based on a Batch jobs result.

### Lambda Configuration

#### Should have next permission actions:
- Allow: batch:DescribeJobs
- Allow: kms:Decrypt
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: logs:GetLogEvents
- Allow: dynamodb:*
- Allow: dynamodb:GetItem
- Allow: dynamodb:BatchGetItem
- Allow: s3:*
- Allow: s3:Get*
- Allow: s3:List*

#### Settings:

- `SECURITY_STANDARDS_COVERAGE` - with coverage percentage of security standards
  divided by cloud:

```json
{
  "name": "SECURITY_STANDARDS_COVERAGE",
  "value": {
    "AWS": {
      "CIS Controls v7": 0.928,
      "HIPAA": 0.5285,
      "PCI DSS v3.2.1 May 2018": 0.2899
    },
    "AZURE": {
      "GDPR 27 April 2016": 0.28,
      "HIPAA": 0.3933,
      "HITRUST": 0.3
    }
  }
}
```

#### Env Variables:

- `caas_rulesets_bucket` - name of s3 bucket with compiled ruleset files.
- `reports_bucket_name` - name of s3 bucket with job execution reports (
  Generated reports will be saved there as well).
- `stats_s3_bucket_name` - name of s3 bucket with job execution statistics.

#### Execution:

- Compliance report:
  Report that describes the compliance of account(s) resources to security
  standard; Event:
  ```json5
  {
    "action": "compliance_report",
    "start": "2021-09-20T00:00:00.000000", // ISO 8601, Include jobs FROM
    "end": "2021-09-23T00:00:00.000000", // ISO 8601, Include jobs TO
    "cloud": "AWS" // "AWS" / "GCP" / "AZURE"
  }
  ```

- Rule report:
  Report that describes total execution statistics by rule; Event:
  ```json5
  {
    "action": "rule_report",
    "start": "2021-09-20T00:00:00.000000", // ISO 8601, Include jobs FROM
    "end": "2021-09-23T00:00:00.000000", // ISO 8601, Include jobs TO
    "cloud": "AWS" // "AWS" / "GCP" / "AZURE"
  }
  ```
