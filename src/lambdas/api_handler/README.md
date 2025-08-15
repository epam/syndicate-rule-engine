### custodian-api-handler

This lambda is designed as a handler for all API resources:

* `/job POST` - initiates the custodian scan for the requested account;
* `/job GET` - returns job details for the requested query with the paths to
  result reports (if any);
* `/job DELETE` - terminates the custodian scan;
* `/report GET` - returns scan details with the result reports contents.
  With `?detailed=true`
  returns scan details with the detailed result reports contents;
* `/signin POST` - returns access and refresh tokens for specific user. This
  user must be in Cognito user pool
  (first go through the signup resource);
* `/signup POST` - resource for registering a new Custodian user. Saves the user
  in Cognito user pool.

Possible parameters for GET requests:

* `account (str)` - requested account name;
* `job_id (str)` - requested AWS Batch job id;
* `date (int)` - timestamp of the day(any from 0:00 to 23:59) job must be
  submitted at;
* `nearest_to (int)` - request the nearest succeeded job details/report to the
  given timestamp;
* `latest (bool)` - request the latest succeeded job details/report.

Note that either `job_id` or `account` must be provided for GET request.

### External Resources Usage

This lambda uses the following resources:

#### DynamoDB

* CaaSCustomers - the table used by service to get and store data about
  activated Customers;
* CaaSRules - the table used by service to get and store data about rules which
  are used for scans;
* CaaSTenants - the table used by service to get and store data about activated
  Tenants;
* CaaSRoles - the table used by service to get and store data about roles;
* CaaSPolicies - the table used by service to get and store data about policies;
* CaaSJobs - the table used by service to store data about scans;
* CaaSSettings - the table used by service to get and store settings data;
* CaaSUsers - the table used by service to get and store user data: user_id and
  tenant;
* CaaSRulesets - the table used by service to get and store data about custom and licensed rulesets.

### Lambda Configuration

#### Should have next permission actions:

- Allow: batch:SubmitJob
- Allow: batch:TerminateJob
- Allow: ssm:PutParameter
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: cognito-idp:AdminInitiateAuth
- Allow: cognito-idp:SignUp
- Allow: cognito-idp:AdminCreateUser
- Allow: cognito-idp:AdminSetUserPassword
- Allow: cognito-idp:ListUserPools
- Allow: cognito-idp:AdminRespondToAuthChallenge
- Allow: cognito-idp:ListUserPoolClients
- Allow: cognito-idp:ListUsers
- Allow: sts:AssumeRole
- Allow: s3:Get*
- Allow: s3:List*

#### Environment variables

* `batch_job_def_name`: name of the AWS Batch Job Definition to submit a job of;
* `batch_job_log_level`: level of logging in Batch (use default - `DEBUG`);
* `batch_job_queue_name`: name of the AWS Batch Job Queue to submit a job to;
* `reports_bucket_name`: name of the S3 Bucket where the result reports should
  be uploaded;
* `caas_user_pool_name`: name of the Cognito User Pool to resolve users from;
* `stats_s3_bucket_name`: name of the bucket to store scan statistics;
* `job_lifetime_min`: lifetime of the job in minutes. After this time, the job
  will be forcibly stopped;
* `feature_skip_cloud_identifier_validation`: choose this option if you 
  want to skip validation of credentials and cloud identifier;

#### Trigger event

There are 7 events lambda accepts as the lambda is a handler for 7 API handler.

1. /job POST

   Request body for AWS account:

    ```json
    {
        "account": "ACCOUNT_NAME",
        "credentials": {
            "AWS_ACCESS_KEY_ID": "KEY",
            "AWS_SECRET_ACCESS_KEY": "SECRET_KEY",
            "AWS_DEFAULT_REGION": "REGION"
        },
        "target_ruleset": [
            "RULESET_1",
            "RULESET_2",
            "RULESET_3"
        ],
        "target_regions": [
            "REGION_1",
            "REGION_2"
        ]
    }
    ```
   Request body for GCP account:
    ```json
    {
        "account": "ACCOUNT_NAME",
        "credentials": {
            "type": "TYPE",
            "project_id": "PROJECT_ID",
            "private_key_id": "PRIVATE_KEY_ID",
            "private_key": "PRIVATE_KEY",
            "client_email": "CLIENT_EMAIL",
            "client_id": "CLIENT_ID",
            "auth_uri": "AUTH_URI",
            "token_uri": "TOKEN_URI",
            "auth_provider_x509_cert_url": "CERT_URL",
            "client_x509_cert_url": "CERT_URL"
        },
        "target_ruleset": [
            "RULESET_1",
            "RULESET_2",
            "RULESET_3"
        ],
        "target_regions": [
            "REGION_1",
            "REGION_2"
        ]
    }
    ```
   OR
    ```json
    {
        "account": "ACCOUNT_NAME",
        "credentials": {
            "access_token": "ACCESS_TOKEN",
            "refresh_token": "REFRESH_TOKEN",
            "client_id": "CLIENT_ID",
            "client_secret": "CLIENT_SECRET"
        },
        "target_ruleset": [
            "RULESET_1",
            "RULESET_2",
            "RULESET_3"
        ],
        "target_regions": [
            "REGION_1",
            "REGION_2"
        ]
    }
    ```

   Request body for AZURE account:
    ```json
    {
        "account": "ACCOUNT_NAME",
        "credentials": {
            "AZURE_TENANT_ID": "TENANT_ID",
            "AZURE_SUBSCRIPTION_ID": "SUBSCRIPTION_ID",
            "AZURE_CLIENT_ID": "CLIENT_ID",
            "AZURE_CLIENT_SECRET": "SECRET"
        },
        "target_ruleset": [
            "RULESET_1",
            "RULESET_2",
            "RULESET_3",
            "RULESET_n"
        ],
        "target_regions": [
            "REGION_1",
            "REGION_2",
            "REGION_n"
        ]
    }
    ```
    * account: [Required] Name (`CaasAccounts.display_name`) of the account to
      initiate the scan for;
    * credentials: [Required] Account credentials;
    * target_ruleset: [Optional] List of rulesets that will be used for the
      scan;
    * target_regions: [Optional] List of activated regions where to scan
      resources.

   Response body:
    ```json
      {
        "job_id": "JOB_ID",
        "job_owner": "USERNAME",
        "account": "ACCOUNT_DISPLAY_NAME",
        "submitted_at": "SUBMITTED_AT_TIMESTAMP"
      }
    ```
2. /report GET

   Request params:

    ```json
    {
        "job_id": "JOB_ID",
        "account": "ACCOUNT_NAME",
        "detailed": "true"
    }
    ```
    * job_id: [Optional] Id (`CaasJobs.job_id`) of the job which report will be
      displayed. Either job_id or account required;
    * account: [Optional] Name (`CaasAccounts.display_name`) of the account
      whose reports will be displayed. Either job_id or account required;
    * detailed: [Optional] Use this boolean parameter to return scan details
      with the detailed result reports contents.

   Response body format:

    ```json5
    {
        "items": [
            {
                "job_id": "JOB_ID",
                "account_display_name": "ACCOUNT_NAME",
                "total_checks_performed": 6,
                "successful_checks": 3,
                "failed_checks": 3,
                "total_resources_violated_rules": 5
            },
            ...
        ]
    }
    ```
3. /job GET

   Request params:
    ```json
    {
        "job_id": "JOB_ID"
    }
    ```
    * job_id: [Required] Job id (`CaasJobs.job_id`) which details will be
      displayed;

   _Without query params, it will return the full list of jobs within the
   customer._

   Response body format:
    ```json
    {
        "items": [
            {
                "created_at": "CREATED_AT_TIMESTAMP",
                "job_owner": "USER_ID",
                "scan_regions": [
                    "us-west1"
                ],
                "status": "FAILED",
                "scan_rulesets": [
                    "ALL"
                ],
                "customer_display_name": "CUSTOMER_NAME",
                "job_id": "JOB_ID",
                "submitted_at": "SUBMITTED_AT_TIMESTAMP",
                "account_display_name": "ACCOUNT_NAME"
            },
            {
                "created_at": "CREATED_AT_TIMESTAMP",
                "job_owner": "USER_ID",
                "status": "SUCCEEDED",
                "customer_display_name": "CUSTOMER_NAME",
                "job_id": "JOB_ID",
                "submitted_at": "SUBMITTED_AT_TIMESTAMP",
                "account_display_name": "ACCOUNT_NAME",
                "scan_regions": [
                    "eu-central-1"
                ],
                "stopped_at": "STOPPED_AT_TIMESTAMP",
                "scan_rulesets": [
                    "ALL"
                ],
                "started_at": "STARTED_AT_TIMESTAMP"
            }
        ]
    }
    ```
4. /job DELETE

   Request params:
    ```json
    {
        "job_id": "JOB_ID"
    }
    ```
    * job_id: [Required] Id (`CaasJobs.job_id`) of the job to terminate.

   Response body format:
    ```json lines
    {
        "The job with id JOB_ID will be terminated"
    }
    ```
5. /signup POST

   Request params:
    ```json
    {
        "username": "USERNAME",
        "password": "PASSWORD",
        "customer": "CUSTOMER_NAME",
        "role": "ROLE_NAME"
    }
    ```

    * username: [Required] Username of new user. Must be unique;
    * password: [Required] Password associated with the user. Must contain upper
      and lower case characters, special symbols and numbers;
    * customer: [Required] Customer name that the user belongs to;
    * role: [Required] Role name that defines permissions for users.

      Response body format:
      ```json
      {
      "The user USERNAME was created"
      }
      ```
6. /signin POST

   Request params:
    ```json
    {
        "username": "USERNAME",
        "password": "PASSWORD"
    }
    ```
    * username: [Required] Name of Custodian user;
    * password: [Required] User's password.

   Response body format:
    ```json
    {
        "id_token": "ID_TOKEN",
        "refresh_token": "REFRESH_TOKEN"
    }
    ```
7. /event POST

   Request params:
    ```json
    {
        "event_body": {...}
    }
    ```
    * event_body: [Required] Content that comes from a client lambda request.
    The "event_body" parameter can be in two formats:
        * eventbridge rule format (the `account` and `source` fields are required)

        ```json
        "event_body": {
              "source": "aws.ec2",
              "account": "111122223333",
              ...
        }
        ```
        * cloudtrail format (the `accountId` and `eventSource` fields are required)

        ```json
        "event_body": {
            "Records": [
                {
                    "userIdentity": {
                        "accountId": "111122223333",
                        ...
                    },
                    "eventSource": "ssm.amazonaws.com",
                    ...
                    },
                    {...},
                    ...
                    {...}
                }
            ]
        }
        ```

   Response body format:
    ```json
      {
        "job_id": "JOB_ID",
        "job_owner": "USERNAME",
        "account": "ACCOUNT_DISPLAY_NAME",
        "submitted_at": "SUBMITTED_AT_TIMESTAMP"
      }
    ```
---