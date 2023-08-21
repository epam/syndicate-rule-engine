## custodian-configuration-backupper

Back up current service configuration, to git and S3 in folders to be able to 
restore current configuration using custodian-configuration-updater 
lambda. Will be back upped next tables:

1. Backup repo:
    - Customers
    - Tenants
    - Accounts
    - Settings
    - Policies
    - Rules meta
    - Ruleset files

2. Backup s3 bucket:
    - encrypted credentials

### Lambda Configuration

#### Should have next permission actions:
- Allow: batch:SubmitJob
- Allow: kms:Encrypt
- Allow: ssm:DescribeParameters
- Allow: ssm:GetParameters
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: dynamodb:GetItem
- Allow: dynamodb:*
- Allow: s3:Get*
- Allow: s3:List*
- Allow: s3:*


#### Settings:

- `BACKUP_REPO_ACCESS_INFO` - setting with git access info for configuration
  repository. Format:

```json5
{
  "name": "BACKUP_REPO_ACCESS_INFO",
  "value": {
    "git_access_secret": "caas-configuration-repo-credentials",
    // SSM param name
    "git_access_type": "TOKEN",
    "git_project_id": 104097,
    "git_ref": "change-folders-structure",
    // name of branch/commit to pull conf
    "git_url": "https://git.epam.com/bohdan_onsha/caas-configuration"
  }
}
```

#### Env Variables:

- `caas_ssm_backup_bucket` - caas-ssm-backup - name of s3 bucket to store
  encrypted secrets.
- `caas_ssm_backup_kms_key_id` - KMS key id to encrypt secrets data.

#### Trigger event

Cloudwatch rule `caas-configuration-backupper-event-trigger`. The cron is set to
run the lambda daily.