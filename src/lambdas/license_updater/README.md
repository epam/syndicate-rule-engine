## Lambda `license-updater`

This lambda is designed to sync with SRE Service License Manager and update licenses on SRE Service side.
It will update items in `SRELicenses` DynamoDB table and ruleset files in s3 bucket.

### Required configuration:

#### Should have next permission actions:
- Allow: dynamodb:GetItem
- Allow: dynamodb:PutItem
- Allow: s3:Get*
- Allow: s3:List*
- Allow: s3:PutObject

#### DynamoDB
* SRESettings
* Customers

#### Settings:
- `ACCESS_DATA_LM` - host, port and version of License Manager with which to sync:

```json
{
 "name": "ACCESS_DATA_LM",
 "value": {
  "host": "https://{HOST}.execute-api.{REGION}.amazonaws.com/sre",
  "port": "443",
  "version": "1"
 }
}
```

#### Env Variables:
- `sre_rulesets_bucket` - name of s3 bucket were to store compiled ruleset files.

#### Trigger event
Cloudwatch rule `sre-license-update-trigger`. The cron is set to run the lambda every three hour.

