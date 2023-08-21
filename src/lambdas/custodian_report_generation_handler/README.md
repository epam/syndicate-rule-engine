## Lambda `custodian-report-generation-handle`

This lambda generates reports based on a metrics data.

### Required configuration:

#### Should have next permission actions:
- Allow: dynamodb:GetItem
- Allow: dynamodb:BatchGetItem
- Allow: dynamodb:Scan
- Allow: dynamodb:Query
- Allow: dynamodb:ConditionCheckItem
- Allow: s3:Get*
- Allow: s3:List*
- Allow: ssm:GetParameter
- Allow: logs:GetLogEvents
- Allow: cognito-idp:AdminInitiateAuth
- Allow: cognito-idp:SignUp
- Allow: cognito-idp:ListUserPools
- Allow: cognito-idp:ListUsers
- Allow: cognito-idp:ListUserPoolClients
- Allow: cognito-idp:AdminRespondToAuthChallenge

#### DynamoDB
* CaaSSettings
* Customers
* Tenants
* CaaSTenantMetrics
* CaaSCustomerMetrics
* Applications

#### S3
* `caas-metrics-{$suffix}` -  metrics bucket.

#### Settings

- `REPORT_DATE_MARKER` - dates of last metrics collection for each report level:

```json
{
 "name": "REPORT_DATE_MARKER",
 "value": {
  "c-level": {
   "current": "2023-03-12",
   "previous": null
  },
  "department": {
   "current": "2023-03-12",
   "previous": null
  },
  "operational": {
   "current": "2023-03-12",
   "previous": "2023-03-06"
  },
  "project": {
   "current": "2023-03-12",
   "previous": "2023-03-06"
  }
 }
}
```

#### Env Variables
- `metrics_bucket_name` - name of s3 bucket where to store collected metrics;
- `caas_user_pool_name` - Cognito user pool name for custodian users;
- `MODULAR_AWS_REGION` - AWS region name where MODULAR has been deployed. Can be empty if it is in the same region with Custodian Service ;
- `modular_assume_role_arn` - role ARN to connect to MODULAR components if they were deployed in another AWS account;


### API endpoints

1. /reports/operational GET

   Request body:

    ```json
    {
       "tenant_name": "TENANT_NAME"
    }
    ```
   
   Required RBAC permissions:
   - `report:operational`

2. /reports/project GET

   Request body:

    ```json
    {
       "tenant_display_name": "TENANT_DISPLAY_NAME"
    }
    ```
   
   Required RBAC permissions:
   - `report:project`

3. /reports/department GET

   Request body:

    ```json
    {
       "customer": "CUSTOMER_NAME"
    }
    ```
   
   Required RBAC permissions:
   - `report:department`

4. /reports/clevel GET

   Request body:

    ```json
    {
       "customer": "CUSTOMER_NAME"
    }
    ```
   
   Required RBAC permissions:
   - `report:clevel`
