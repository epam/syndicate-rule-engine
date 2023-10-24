## custodian-configuration-updater

Synchronize configurations according to state stored in git repo Push
configuration data to:

1. Dynamodb:
    - Customers
    - Tenants
    - Accounts
    - Rules
    - Policies
    - Roles
    - Settings

2. S3:
    - rulesets

3. Secrets:
    - SSM (Parameter Store)

### Lambda Configuration

#### Should have next permission actions:

- Allow: batch:SubmitJob
- Allow: kms:Decrypt
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

- `caas_rulesets_bucket` - name of s3 bucket to store ruleset files. DEV
  bucket: `caas-rulesets-dev2`

- `caas_ssm_backup_bucket` - caas-ssm-backup - name of s3 bucket to store
  encrypted secrets.

- `caas_ssm_backup_kms_key_id` - KMS key id to encrypt secrets data.
