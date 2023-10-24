## custodian-rule-meta-updater

This lambda is designed to pull the latest data from rules GIT repository and
store the data in `CaaSRules` DynamoDB table. Repository path and SSM parameter
for credentials are stored in `CaaSCustomers` DynamoDB table. The Rule model:

* `customer (str)`
* `id (str)`. Format: `name_version`
* `name (str)`
* `description (str)`
* `cloud (str)`. Possible values: `AWS/GCP/Azure`
* `version (str)`
* `creator (str)`
* `updated_date (str)`
* `source (str)`
* `commit_hash (str)`

The rule file in the repository must be the following format:

```yaml
policies:
  - name: name
    description: description
    metadata:
      version: version
      cloud: AWS/GCP/Azure
      source: SOURCE
    some more: content
                 - and: more
    article: ARTICLE
    service_section: SECTION
    impact: IMPACT
    severity: SEVERITY
    min_core_version: upstream_version.custom_version
    standard:
      STANDARD1:
        - point 1
        - point 2
      STANDARD2:
        - point 1
      remediation: REMEDIATION
```

### External Resources Usage

This lambda uses the following resource:

#### DynamoDB

* `CaaSRules` - the table used to store data about rules;
* `CaaSCustomers`, `CaaSTenants`, - tables with customer, tenant
  configurations where the information about repositories are stored

### Lambda Configuration

#### Should have next permission actions:
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: dynamodb:*
- Allow: s3:*

#### Trigger event

Cloudwatch rule `caas-rule-meta-updater-event-trigger`. The cron is set to run
the lambda daily.