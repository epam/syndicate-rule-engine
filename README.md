### Syndicate Rule Engine

Syndicate Rule Engine is a solution that allows checking and assessing virtual infrastructures in AWS, Azure, GCP infrastructures against different types of standards, requirements and rulesets.
By default, the solution covers hundreds of security, compliance, utilization and cost effectiveness rules, which cover world-known standards like GDPR, PCI DSS, CIS Benchmark, and a bunch of others.

### Notice

All the technical details described below are actual for the particular version,
or a range of versions of the software.

### Actual for versions: 3.0.0

## Lambdas description

### Lambda `custodian-api-handler`

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
  in Cognito user pool;
* `/scheduled-job GET|POST|PATCH|DELETE` - resource for retrieving/registering/updating/deregistering a scheduled job
  which will be executed according to the given cron;
* `/event POST` - resource for starting job in event-driven;

Possible parameters for GET requests:

* `account (str)` - requested account display name;
* `job_id (str)` - requested AWS Batch job id;
* `latest (bool)` - request the latest succeeded job details/report.

Note that either `job_id` or `account` must be provided for GET request.

Refer to [custodian-api-handler](src/lambdas/custodian_api_handler/README.md)
for more details.

---

### Lambda `custodian-job-updater`

This lambda is designed to update Jobs state in `CaaSJobs` DynamoDB table.
Triggered by CloudWatch Rule `custodian-job-state-update`.

Refer to [custodian-job-updater](src/lambdas/custodian_job_updater/README.md)
for more details.

---

### Lambda `custodian-rule-meta-updater`

This lambda is designed to pull the latest data from rules GIT repository and
store the data in `CaaSRules` DynamoDB table. The Rule model:

* `id (str)`. Format: `name_version`
* `name (str)`
* `description (str)`
* `cloud (str)`. Possible values: `AWS/GCP/AZURE`
* `version (str)`
* `creator (str)`
* `updated_date (str)`
* `source (str)`

Refer
to [custodian-rule-meta-updater](src/lambdas/custodian_rule_meta_updater/README.md)
for more details.

---

### Lambda `custodian-configuration-backupper`

Back up current service configuration, push configuration data to:

1. Backup repo:
    - Accounts
    - Settings
    - Rules meta
    - Ruleset files

2. Backup s3 bucket:
    - encrypted credentials

Refer
to [custodian-configuration-backupper](src/lambdas/custodian_configuration_backupper/README.md)
for more details.

---

### Lambda `custodian-configuration-updater`

Synchronize configurations according to state stored in git repo Push
configuration data to:

1. Dynamodb:
    - Accounts
    - Rules
    - Settings

2. S3:
    - rulesets

3. Secrets:
    - SSM (Parameter Store)

Refer
to [custodian-configuration-updater](src/lambdas/custodian_configuration_updater/README.md)
for more details.

---

### Lambda `custodian-ruleset-compiler`

This lambda is designed to assemble all the rules required for the specific scan
and put resulted `.yml` files into s3 bucket.

While working, lambda will set/change Customer/Tenant/Account rulesets
configuration `s3_path` and `status` attributes.

Refer
to [custodian-ruleset-compiler](src/lambdas/custodian_ruleset_compiler/README.md)
for more details.

---

### Lambda `custodian-report-generator`

This lambda generates statistics reports based on a Batch jobs result.

Refer
to [custodian-report-generator](src/lambdas/custodian_report_generator/README.md)
for more details.

---

### Lambda `custodian-configuration-api-handler`

This lambda is designed to handle the API for Accounts,
Rulesets, Rule Sources and Account Regions configurations

Refer
to [custodian-configuration-api-handler](src/lambdas/custodian_configuration_api_handler/README.md)
for more details.

## Statistics collecting
Rules statistics are stored in the S3 bucket.

#### Env Variables:
- `STATS_S3_BUCKET_NAME` - name of s3 bucket to store statistic files. DEV
  bucket: `caas-statistics-dev2`.

#### Statistics format:
```text
[
  {
    "id": "rule_id",
    "region": "region",
    "started_at": "ISO_time",
    "finished_at": "ISO_time",
    "status": "SUCCEEDED|SKIPPED|FAILED",
    "resourced_scanned": resources_amount,
    "elapsed_time": "time_in_seconds",
    "failed_resources": [
      {
        resource1_description
      },
      {
        resource2_description
      },
      ...
      {
        resourceN_description
      }
    ]
    "account_display_name": "account_name",
    "tenant_display_name": "tenant_name",
    "customer_display_name": "customer_name"
  }
]
```

## Rules format
Each rule file in the repository must be in the following format:

```yaml
policies:
  - name: name
    description: description
    metadata:
      version: version
      cloud: AWS/GCP/Azure
      source: source
      article: article
      remediation: remediation
      service_section: service_section
      standard:
        standard_name_1:
          - point 1
          - point 2
        standard_name_2:
          - point 1
          - point 2
          - point 3
    some more: content
          - and: more
```

All fields are required.

## Tests
To run tests use the command below:
```bash
python -m unittest discover -s tests -v
```
The unittests are in the folder custodian-as-a-service/tests. If you want to 
see the coverage install `coverage` library:
```bash
pip install coverage
```
Execute the following command to run tests with coverage:
```bash
coverage run -m unittest discover -s tests -v
```
Execute to generate the HTML-report:
```bash
coverage html --omit "tests*"
```
The generated files are in the `htmlcov` directory that appeared in your working dir. Use the `index.html` from within.


## Event-Driven scans
If there is no need to scan the entire cloud account, but only certain resources and only after their changes
(for example, an ec2 instance was created, the content of an s3 bucket was updated, etc.), then the solution is
event-driven scans.


Using `/account/credentials-manager` endpoint or `c7n account credentials-manager add` command add credentials
configuration: cloud name, cloud identifier, trusted role ARN using which service can get temporary credits from
specified account for event-driven scan.
Temporary credentials are stored in the `CaaSCredentials` table along with their expiration. If expiration time is
less than 15 minutes, the new credentials will be obtained from the assumed role, otherwise the existing credentials
will be used.

The trigger for executing event-driven scans is a request from the client lambda received at the /event endpoint.
The entire flow of deploying resources to a client account is described in the [documentation](src/cloudformation/README.md).

Event-driven scans use rulesets that have the `event_driven` field set to `true`.
