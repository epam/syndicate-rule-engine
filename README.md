# Syndicate Rule Engine


## Quickstart with docker compose

Use this quickstart only for fast introduction or development. Default credentials are used as they are hardcoded in 
`compose.yaml`


```bash
git clone https://github.com/epam/ecc.git && cd ecc
```

```bash
docker compose --file deployment/compose/compose.yaml --profile rule-engine up -d
```

```bash
pip install ./cli
```

```bash
sre configure --api_link http://127.0.0.1:8000/caas
```

```bash
sre signup --username admin --password Password123= --customer_name "DEMO" --customer_display_name "Demo Customer" --customer_admin admin@gmail.com 
```

```bash
sre login --username admin --password Password123=
```


### Syndicate Rule Engine

Syndicate Rule Engine is a solution that allows checking and assessing virtual infrastructures in AWS, Azure, GCP infrastructures against different types of standards, requirements and rulesets.
By default, the solution covers hundreds of security, compliance, utilization and cost effectiveness rules, which cover world-known standards like GDPR, PCI DSS, CIS Benchmark, and a bunch of others.

### Notice

All the technical details described below are actual for the particular version,
or a range of versions of the software.

### Actual for versions: 5.0.0

## Lambdas description

### Lambda `custodian-api-handler`

This lambda is designed as a handler for all API resources:

* `/jobs POST` - initiates the custodian scan for the requested account;
* `/jobs GET` - returns job details for the requested query with the paths to
  result reports (if any);
* `/jobs DELETE` - terminates the custodian scan;
* `/signin POST` - returns access and refresh tokens for specific user. This
  user must be in Cognito user pool
  (first go through the signup resource);
* `/signup POST` - resource for registering a new Custodian user. Saves the user
  in Cognito user pool;
* `/scheduled-job GET|POST|PATCH|DELETE` - resource for retrieving/registering/updating/deregistering a scheduled job
  which will be executed according to the given cron;
* `/event POST` - resource for starting job in event-driven;

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
pytest tests/
```


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
