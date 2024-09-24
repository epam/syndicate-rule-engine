
# Quickstart

When AMI instance is running you can log in using ssh and immediately use Rule Engine. 
```bash
ssh -i "private-key.pem" admin@domain.compute.amazonaws.com
syndicate version
```

`syndicate` is the main CLI entrypoint that you should use to interact with Rule Engine API and Modular Service API. 
Rule Engine API allows to execute scans and receive reports. Modular Service - is an admin API. It allows to configure 
such organization entities as Customers and Tenants. Use commands `syndicate re` and `syndicate admin` accordingly.

Both Rule Engine API and Modular Service API has authentication mechanism and credentials to access them. Those were set for you 
during setup and their refresh tokens are updated automatically when session ends. `syndicate` tool also has its 
authentication mechanism, and it may require to log in once in a while. So, if any `syndicate ...` command tells that session has ended, use this command:
```bash
syndicate login
```
**Note:** credentials are situated here `~/.modular_cli/`.

From the beginning, only entity that represents the AWS account where instance is running - is activated. Such entities are called `Tenants`. You can describe them using this command:
```bash
syndicate re tenant describe
```
**Note:** this one by default has `TENANT_1` name that must be used to reference this entity.

When the instance was starting it made a request to our License Manager and received a license and rule-sets. You can 
describe the license using this command:
```bash
syndicate re license describe
```

Command to describe rulesets:
```bash
syndicate re ruleset describe
```

If the instance has Instance Role with access to this AWS Account you can execute scans immediately without further configuration.
Use this command:

```bash
syndicate re job submit --tenant_name TENANT_1 --region eu-west-1  # or the region you want
```
Rule Engine will use rulesets that are available by license and credentials from instance profile.

To see job's status use:

```bash
syndicate re job describe --limit 1
```

When the status is `SUCCEEDED` you can request some reports:

```bash
syndicate re report digests jobs --job_id <job_id> --json
```

```bash
syndicate re report resource latest --tenant_name TENANT_1 --json > data.json
```

See the full documentation for further details.


# 1. General Overview

## 1.1. General overview & value brought by the service

## 1.2. Use cases

## 1.3. Delivery Options

### 1.3.1. AMI based

### 1.3.2. AWS Serverless based

## 1.4. Distribution points

### 1.4.1. EPAM Solutions Hub

### 1.4.2. AWS Marketplace

## 1.5. Licensing

### 1.5.1. What is License

License - is a logical entity that issues rule-sets for scanning. Each customer can have a license assigned to it and therefore a 
unique list of allowed rulesets. Licenses are issued by Rule Engine License Manager

### 1.5.2. How to get License

#### 1.5.2.1. AMI based

Licenses are issued automatically for each launched AMI-based instance. What license is issued depends on Marketplace `product code` 
or AWS `Account Id` where the instance is launched. Specific unique licenses can be issued as private offering.

#### 1.5.2.2. AWS Serverless based

License can be created by Rule Engine License Manager team and issued to a customer. After that either customer by himself or 
the support team must activate this license for Custodian installation they use

### 1.5.3. How to prolong existing License

Contact the support team

### 1.5.4. How to update existing License

Contact the support team

### 1.5.5. How to cancel existing License

Contact the support team

# 2. Installation & Configuration Guide

## 2.1. AMI

AWS AMI image must be shared privately with your AWS Account or as a part of Marketplace product. After that you must 
launch an instance from that AMI to start using the product.

### 2.1.1. Receive & install AMI - EPAM SH & AWS Marketplace

todo: tell here how to start instance?

### 2.1.2. Configure AMI

When AMI is launched and its instance `Status check` is green you can log in to the instance via SSH and use Rule Engine immediately 
from `syndicate` entrypoint. There are two groups of commands: `syndicate re ...` and `syndicate admin ...`. 
The former is used to access scanning and reporting API (`re` - rule engine) and the latter is used to manage logical entities 
that represent accounts and organizations (`admin` - entities administrator)

#### 2.1.2.1. Activate Tenants

`Tenant` is a main entity that Rule Engine manages and requires. One tenant represents one AWS Account or AZURE subscription or
GOOGLE project that is to be scanned. If you want to scan something you must create a tenant that represents it.

When an instance is launched from AMI one tenant is created automatically. Its default name is `TENANT_1` and it 
represents the AWS account where the AMI was launched. This tenant has one active region (the one where instance is launched).
In case instance profile allowed READ access to the AWS account that tenant can be immediately used.

##### 2.1.2.1.1. Activating Tenants linked to cloud accounts

If you want to scan an account other from the one where the instance is launched you must do three configuration steps:
- create a tenant entity that represents the account you want to scan:

    ```bash
    syndicate admin tenant create --name "MY_OTHER_ACCOUNT" --display_name "Dev account" --cloud AWS --account_id 111111111111 --primary_contacts admin@example.com --secondary_contacts admin@example.com --tenant_manager_contacts admin@exampl.com --default_owner admin@example.com
    ```
  
- activate necessary regions for the tenant:

    ```bash
    syndicate admin tenant regions activate --tenant_name MY_OTHER_ACCOUNT --region_name eu-west-1
    syndicate admin tenant regions activate --tenant_name MY_OTHER_ACCOUNT --region_name eu-central-1
    ```

- configure AWS access keys or access role for that specific tenant:

    Access keys can be provided individually for each scan which is definitely the case, but it's somewhat inconvenient:

    ```bash
    syndicate re job submit_aws --tenant_name TENANT_1 --access_key $AWS_ACCESS_KEY_ID --secret_key $AWS_SECRET_ACCESS_KEY --session_token $AWS_SESSION_TOKEN --region eu-west-1
    ```

    You can configure an AWS role or, for instance, AZURE Certificate to be used for scanning multiple times. 
    To do this you must create a so-called `Application` entity and then bound it to some tenants. Let's take a look at
    AWS Role example. Create an application:

    ```bash
    syndicate admin application create_aws_role --role_name rule-engine-scanner --account_id 111111111111 --description "Generic role for AWS tenants"
    ```
  
    Link the application to `ALL` the tenants

    ```bash
    syndicate re tenant credentials link --application_id <application id received from command above> --all_tenants
    ```

    Now, if you submit a scan for any tenant within the customer Rule engine will try to use that `rule-engine-scanner` AWS Role.
    Account ID is generic so, `111111111111` is just the default value. Tenant's account id will be used dynamically, 
    i.e. `arn:aws:iam::123123123123:role/rule-engine-scanner` for tenant with account id `123123123123` and so on. The 
    save way one Application with AZURE Certificate can be linked to multiple AZURE tenants (remember, one tenant is one subscription)


#### 2.1.2.2. Create API Users

When the AMI is launched admin users are created for Rule Engine and for Modular Service. They are configured by default. 
Their passwords are placed to `/usr/local/sre/secrets/rule-engine-pass` and `/usr/local/sre/secrets/modular-service-pass` correspondingly (all the sensitive 
information is listed in `Attachment 1`). Username `customer_admin` is default for both and can be configured via User 
data script before instance startup.

Your admin Rule engine user has rights to manage other users inside your customer:
```bash
syndicate re users describe
```

Each user has an assigned role. Each role can have multiple policies attached. Each policy can allow or deny specific 
actions over API, so you can flexibly configure access to the system

##### 2.1.2.2.1. Users registration

Let's create a new user, but first we should create a separate policy and role for him. Create a policy:

```bash
syndicate re policy add --name run_scan_for_all_tenants --permission "job:post_for_tenant" --effect allow --description "Allows to submit jobs for all tenants"
```
**Note:** list of all permission can be found in `Attachment 2`

Create a role:

```bash
syndicate re role add --name run_scans_role --policies run_scan_for_all_tenants --description "Allows only to submit jobs"
```

Create a user:

```bash
syndicate re users create --username job_submitter --password $SECRET_PASSWORD --role_name run_scans_role
```

Now you can log in as the newly created user or give its credentials to someone else:

```bash
syndicate re login --username job_submitter --password $SECRET_PASSWORD
syndicate re job describe  # 403
```

##### 2.1.2.2.2. Policies & Roles assign

Policies can be added and removed from roles:

```bash
syndicate re role update --name run_scans_role --attach_policy admin_policy --detach_policy run_scan_for_all_tenants
```

todo: what exactly should be here?


#### 2.1.2.3. Activate License

Each AMI-based instance will have a single license which can give multiple rule-sets. You can add more licenses to the
installation if you have license keys. Those can be issued by the Rule Engine License Manager team. Let's assume you have one
and want to add it to the installation. First, just add a license:
```bash
syndicate re license add --tenant_license_key $TENANT_LICENSE_KEY --description "my newly provided license"
```

After that you should activate the license for tenants. You must do that because you can have overlap (two different licenses 
can issue different rulesets for the same tenant):

```bash
syndicate re license activate --license_key <License key from previous command> --tenant_name TENANT_1 --tenant_name ANOTHER_ACCOUNT
```
**Note:** `License Key from previous command` is not the same as `$TENANT_LICENSE_KEY`


### 2.1.3. Initialize AMI for another linux user

Ami has a script called `sre-init` that allows to init Rule engine for a new linux user. By default, only the first non-root 
linux user has sre installed. If you want to initialize rule engine for other linux execute the command:

```bash
sre-init --user "username" --public-ssh-key "ssh-..." --re-username job_submitter --re-password $SECRET_PASSWORD
```
A user with name `username` will be created if it does not exist yet. If `--public-ssh-key` is specified it will be added to
`~/.ssh/authorized_keys` of that user. You can also provide `--re-username` & `--re-password` of a Rule Engine user 
created before to configure set these credentials.

# 3. Scanning Guide

Scans can be executed individually for each tenant. By default, all the active regions inside the tenant will be scanned 
using all the rules from the default license. You can specify exact regions and rule-sets you want to use during the scan 
when you submit it

## 3.1. Requesting full scan

To request a scan for `TENANT_1` tenant for all regions and all rules you the following command:

```bash
syndicate re job submit --tenant_name TENANT_1 --region eu-west-1
```

To see the status of the submitted job use the describe command:

```bash
syndicate re job describe --job_id <job id from submit command>
```


## 3.2. Requesting scan with exact rules

You can use rule-sets names to scan only those in case the license provides multiple rulesets for one cloud:

```bash
syndicate re ruleset describe
```

And submit the scan:

```bash
syndicate re job submit --tenant_name TENANT_1 --ruleset FULL_AWS --region eu-west-1
```

Also, you can restrict the scope to specific rule names. List of all rules and their descriptions can be found in 
`Attachment 3`

```bash
syndicate re job submit --tenant_name TENANT_1 --ruleset FULL_AWS --rules_to_scan ecc-aws-001... --rules_to_scann ecc-aws-002...
```

## 3.3. Disabling rules for tenant

You can exclude some rules for specific tenant or for the whole customer if you know that you won't need those although 
the ruleset has them.

Exclude for tenant:

```bash
syndicate re tenant set_excluded_rules --tenant_name TENANT_1 --rules ecc-aws-001 --rules ecc-aws-002
```

Exclude for customer:

```bash
syndicate re customer set_excluded_rules --rules ecc-aws-003
```

# 4. Receiving Reports

There are two types of reports. `Built-in` reports are those that you can receive using CLI immediately after scans. Another 
type of reports is Analytical high-level reports that can generated by Rule Engine Saas installation. Those required 
you to obfuscate your private data and then give it to the Rule Engine Team. We will generate reports and then you will 
de-obfuscate the data using your local dictionary of obfuscated values

## 4.1. Built-in Reports

Generally all `built-in` reports can be divided into two parts: job-scope reports and tenant-scope reports. As the name 
implies job-scope reports will show information about a concrete job independently of others. Tenant-scope reports will 
contain data accumulated from multiple jobs within a tenant. All the reports CLI commands can be accessed from 
`syndicate re report` entrypoint.

To get the latest resources state for a tenant you can generate resources report: 
```bash
syndicate re report resource latest --tenant_name TENANT_1 --json > data.json
```

If you want to get the save report in more human-readable format use `--format xlsx` and download the file from received url:

```bash
syndicate re report resource latest --tenant_name TENANT_1 --format xlsx --json
```

If you want the same report but filtered based on some attributes use CLI params:

```bash
syndicate re report resource latest --tenant_name TENANT_1 --format xlsx --region eu-central-1 --name my-lambda --json
```

If you want to look at `Access Denied` errors during the scan you can generate errors report:

```bash
syndicate re report errors jobs --job_id d9db86d6-a8fb-4383-8204-14961a90b8d4 --error_type ACCESS
```

## 4.2. Defect Dojo

When the AMI-based instance is running you can access Defect Dojo Ui on `8080` port of instance public ipv4. Admin 
password is inside `/usr/local/sre/secrets/defect-dojo-pass` file. Admin username is `admin`. Rule Engine is configured to push results of 
each job automatically, so you should see active findings after at least one job was successfully finished.


## 4.3. Analytical Reports

As it was mentioned Analytical Reports can be generated by SAAS installation of Rule Engine, so you must obfuscate your 
data and give it to us.

### 4.3.1. Data Obfuscation & De-obfuscation

Data obfuscation is process of concealing sensitive content inside that data. Rule engine considers every field sensitive 
and hides all of them. You can request almost any `built-in` report specifying `--obfuscated` flag. Rule Engine team 
needs just raw report to be able to generate Analytics reports. To receive obfuscated raw report use the command:

```bash
syndicate re report raw latest --tenant_name TENANT_1 --obfuscated --json
```
This command will give two urls. `url` is the obfuscated report itself. It can be given to our team. `dictionary_url` is
the obfuscation dictionary of values. Keep it to yourself.

When you receive the generated Analytics report it most likely would have `.eml` format. You can de-obfuscate it using 
`sre-obfuscator` cli tool. Just specify path to the obfuscated report (or directory with multiple obfuscated reports) and 
path to the dictionary:

```bash
sre-obfuscator deobfuscate --dump-directory obfuscated.eml --to result --dictionary obfuscation_dictionary.json
```
Values will be replaced back to origin ones.

### 4.3.2. Reports Types

There are four types of Analytics reports: Operational report, Project report, Department report and Clevel report.

#### 4.3.2.1 Operational report

This reports contains tenant-level aggregated data. It can be generated to display:
- general overview
- violated resources
- compliance and coverages percents
- used rules and statistics
- attack vectors

#### 4.3.2.2 Project report

This report contains project-level aggregated data. A project is cluster of tenants (possible of different clouds) that 
are used by one project. It can be generated to display:
- general overview
- violated resources
- compliance and coverages percents
- used rules and statistics
- attack vectors

#### 4.3.2.3 Department report

This report contains data within a customer accumulated by all its tenants and ranked by some attributes. Such variations 
exist:
- top resources by clouds
- top tenants violated resources
- top tenants compliance
- top compliance by cloud
- top tenants attacks vectors
- top attacks by clouds


#### 4.3.2.4 Clevel report

This report contains high level overview of all the accumulated data within a customer. This report is not technical and 
is mainly to show the current security situation


# 5. Cutting costs for AMI-based products.

todo: what exactly should be here? - disabling Defect Dojo and using lower instance type?

# Attachments:

1. All secrets inside AMI
All the secrets that are generated during installation belong to the linux user with id `1000`. They are in inside 
`/usr/local/sre/secrets/`. There are these files:
- `defect-dojo-pass`: Defect Dojo admin password
- `modular-service-pass`: Modular service admin user password
- `rule-engine-pass`: Rule Engine admin user password
- `rule-engine.env`: Rule Engine environment variables generated before starting the server. It contains credentials to microservices (mongo, minio and vault) and system password for Rule Engine and Modular Service
- `defect-dojo.env`: Defect Dojo environment variables generated before starting the server
- `lm-link`: Syndicate License Manager API link
- `lm-response`: Syndicate License Manager API response. It contains tenant license key and private keys to sign requests

2. All permissions:

|                            Endpoint                           |               Permission               |                                   Description                                  |
|---------------------------------------------------------------|----------------------------------------|--------------------------------------------------------------------------------|
|                          POST /signup                         |                    -                   |  Registers a new API user, creates a new customer and admin role for that user |
|                          POST /signin                         |                    -                   |               Allows log in and receive access and refresh tokens              |
|                         POST /refresh                         |                    -                   |                       Allows to refresh the access token                       |
|                          GET /health                          |                    -                   |                      Performs all available health checks                      |
|                        GET /health/{id}                       |                    -                   |                   Performs a specific health check by its id                   |
|             GET /batch-results/{batch_results_id}             |            batch_results:get           |                 Allows to get a specific event-driven job by id                |
|                       GET /batch-results                      |           batch_results:query          |                        Allows to query event driven jobs                       |
|                 PUT /credentials/{id}/binding                 |            credentials:bind            |         Allows to link tenants to a specific credentials configuration         |
|                        GET /credentials                       |          credentials:describe          |           Allows to get credentials configurations within a customer           |
|                     GET /credentials/{id}                     |          credentials:describe          |                 Allows to get a credentials configuration by id                |
|                 GET /credentials/{id}/binding                 |         credentials:get_binding        |  Allows to show tenants that are linked to specific credentials configuration  |
|                DELETE /credentials/{id}/binding               |           credentials:unbind           |     Allows to unlink a specific credentials configuration from all tenants     |
|                         GET /customers                        |            customer:describe           |                          Allows to describe customers                          |
|                 GET /customers/excluded-rules                 |       customer:get_excluded_rules      |                     Allows to get customer`s excluded rules                    |
|                 PUT /customers/excluded-rules                 |       customer:set_excluded_rules      |                      Allows to exclude rules for customer                      |
|         PUT /integrations/defect-dojo/{id}/activation         |        dojo_integration:activate       |             Allows to activate Defect Dojo integration for tenants             |
|         GET /integrations/defect-dojo/{id}/activation         |        dojo_integration:activate       |         Allows to get tenants Defect Dojo integration is activated for         |
|                 POST /integrations/defect-dojo                |         dojo_integration:create        |                   Allows to register Defect Dojo integration                   |
|             DELETE /integrations/defect-dojo/{id}             |         dojo_integration:delete        |                 Allows to delete Defect Dojo integration by id                 |
|        DELETE /integrations/defect-dojo/{id}/activation       |   dojo_integration:delete_activation   |                  Allows to deactivate Defect Dojo integration                  |
|                 GET /integrations/defect-dojo                 |        dojo_integration:describe       |               Allows to list registered Defect Dojo integrations               |
|               GET /integrations/defect-dojo/{id}              |        dojo_integration:describe       |                Allows to describe Defect Dojo integration by id                |
|                          POST /event                          |               event:post               |                          Receives event-driven events                          |
|                         POST /policies                        |            iam:create_policy           |                            Allows to create a policy                           |
|                          POST /roles                          |             iam:create_role            |                             Allows to create a role                            |
|                         GET /policies                         |           iam:describe_policy          |                          Allows to list rbac policies                          |
|                      GET /policies/{name}                     |           iam:describe_policy          |                         Allows to get a policy by name                         |
|                           GET /roles                          |            iam:describe_role           |                            Allows to list rbac roles                           |
|                       GET /roles/{name}                       |            iam:describe_role           |                          Allows to get a role by name                          |
|                    DELETE /policies/{name}                    |            iam:remove_policy           |                        Allows to delete a policy by name                       |
|                      DELETE /roles/{name}                     |             iam:remove_role            |                         Allows to delete a role by name                        |
|                     PATCH /policies/{name}                    |            iam:update_policy           |                         Allows to update a policy name                         |
|                      PATCH /roles/{name}                      |             iam:update_role            |                         Allows to update a role by name                        |
|                       GET /jobs/{job_id}                      |                 job:get                |                       Allows to get a specific job by id                       |
|                         POST /jobs/k8s                        |        job:post_for_k8s_platform       |                Allows to submit a licensed job for a K8S cluster               |
|                           POST /jobs                          |           job:post_for_tenant          |                   Allows to submit a licensed job for a cloud                  |
|                      POST /jobs/standard                      |      job:post_for_tenant_standard      |  Allows to submit a standard not licensed job. Ruleset must be present locally |
|                           GET /jobs                           |                job:query               |                              Allows to query jobs                              |
|                     DELETE /jobs/{job_id}                     |              job:terminate             |                    Allows to terminate a job that is running                   |
|             PUT /licenses/{license_key}/activation            |            license:activate            |             Allows to activate a specific license for some tenants             |
|                         POST /licenses                        |           license:add_license          |              Allows to add a license from LM by tenant license key             |
|           DELETE /licenses/{license_key}/activation           |        license:delete_activation       |                     Allows to deactivate a specific license                    |
|                 DELETE /licenses/{license_key}                |         license:delete_license         |                       Allows to delete a specific license                      |
|                  GET /licenses/{license_key}                  |               license:get              |              Allows to describe a specific license by license key              |
|             GET /licenses/{license_key}/activation            |         license:get_activation         |                Allows to list tenants a license is activated for               |
|                         GET /licenses                         |              license:query             |                      Allows to list locally added licenses                     |
|               POST /licenses/{license_key}/sync               |              license:sync              |                         Allows to trigger license sync                         |
|            PATCH /licenses/{license_key}/activation           |        license:update_activation       |              Allows to update tenants the license is activated for             |
|                    POST /rule-meta/mappings                   |          meta:update_mappings          |              Allows to submit a job to update rules meta mappings              |
|                      POST /rule-meta/meta                     |            meta:update_meta            |              Allows to submit a job to update rules meta mappings              |
|                   POST /rule-meta/standards                   |          meta:update_standards         |                 Allows to submit a job to update standards meta                |
|                      POST /platforms/k8s                      |           platform:create_k8s          |                         Allows to register K8S platform                        |
|              DELETE /platforms/k8s/{platform_id}              |           platform:delete_k8s          |                       Allows to deregister a K8S platform                      |
|                GET /platforms/k8s/{platform_id}               |            platform:get_k8s            |                         Allows to register K8S platform                        |
|                       GET /platforms/k8s                      |            platform:query_k8           |                    Allows to query registered K8S platforms                    |
|                    POST /customers/rabbitmq                   |             rabbitmq:create            |             Allows to create a RabbitMQ configuration for customer             |
|                   DELETE /customers/rabbitmq                  |             rabbitmq:delete            |                    Allows to remove a RabbitMQ configuration                   |
|                    GET /customers/rabbitmq                    |            rabbitmq:describe           |                    Allows to describe RabbitMQ configuration                   |
|               GET /reports/details/jobs/{job_id}              |           report:get_details           |                    Allows to get a detailed report by job id                   |
|        GET /reports/details/tenants/{tenant_name}/jobs        |           report:get_details           |          Allows to get multiple detailed reports by tenant latest jobs         |
|                    GET /reports/diagnostic                    |          report:get_diagnostic         |                         Allows to get diagnostic report                        |
|               GET /reports/digests/jobs/{job_id}              |            report:get_digest           |                     Allows to get a digest report by job id                    |
|        GET /reports/digests/tenants/{tenant_name}/jobs        |            report:get_digest           |           Allows to get multiple digest reports by tenant latest jobs          |
|              GET /reports/findings/jobs/{job_id}              |           report:get_findings          |                        Allows to get findings by job id                        |
|        GET /reports/findings/tenants/{tenant_name}/jobs       |           report:get_findings          |                Allows to get findings by latest jobs of a tenant               |
|             GET /reports/compliance/jobs/{job_id}             |        report:get_job_compliance       |                    Allows to get compliance report by a job                    |
|               GET /reports/errors/jobs/{job_id}               |          report:get_job_errors         |                   Allows to get errors occurred during a job                   |
|              GET /reports/resources/jobs/{job_id}             |        report:get_job_resources        |                  Allows to get latest resources report by job                  |
|       GET /reports/resources/tenants/{tenant_name}/jobs       |     report:get_job_resources_batch     |           Allows to get latest resources report by latest tenant jobs          |
|                GET /reports/rules/jobs/{job_id}               |          report:get_job_rules          |           Allows to get information about rules executed during a job          |
|GET /reports/resources/platforms/k8s/{platform_id}/state/latest|report:get_k8s_platform_latest_resources|              Allows to get latest resources report by K8S platform             |
|                      GET /reports/status                      |            report:get_status           |                     Allows to get a status of report by id                     |
|         GET /reports/compliance/tenants/{tenant_name}         |      report:get_tenant_compliance      |                   Allows to get a compliance report by tenant                  |
|      GET /reports/raw/tenants/{tenant_name}/state/latest      |   report:get_tenant_latest_raw_report  |                   Allows to request raw report data by tenant                  |
|   GET /reports/resources/tenants/{tenant_name}/state/latest   |   report:get_tenant_latest_resources   |                 Allows to get latest resources report by tenant                |
|            GET /reports/rules/tenants/{tenant_name}           |         report:get_tenant_rules        |             Allows to get average rules data by latest tenant jobs             |
|                      POST /reports/clevel                     |           report:post_clevel           |                         Allows to request clevel report                        |
|                    POST /reports/department                   |         report:post_department         |                       Allows to request department report                      |
|                   POST /reports/operational                   |         report:post_operational        |                      Allows to request operational report                      |
|                     POST /reports/project                     |           report:post_project          |                        Allows to request project report                        |
|                POST /reports/push/dojo/{job_id}               |       report:push_report_to_dojo       |                  Allows to push a specific job to Defect Dojo                  |
|                    POST /reports/push/dojo                    |        report:push_to_dojo_batch       |                   Allows to push multiple jobs to Defect Dojo                  |
|                         DELETE /rules                         |               rule:delete              |                      Allows to delete local rules content                      |
|                           GET /rules                          |              rule:describe             |                   Allows to describe locally available rules                   |
|                       POST /rule-sources                      |           rule_source:create           |                       Allows to add a rule-source locally                      |
|                      DELETE /rule-sources                     |           rule_source:delete           |                      Allows to delete a local rule-source                      |
|                       GET /rule-sources                       |          rule_source:describe          |                  Allows to list all locally added rule sources                 |
|                      PATCH /rule-sources                      |           rule_source:update           |                      Allows to update a local rule-source                      |
|                         POST /rulesets                        |             ruleset:create             |                Allows to create a local ruleset from local rules               |
|                  POST /rulesets/event-driven                  |       ruleset:create_event_driven      |                Allows to create a ruleset for event-driven scans               |
|                        DELETE /rulesets                       |             ruleset:delete             |                        Allows to delete a local ruleset                        |
|                 DELETE /rulesets/event-driven                 |       ruleset:delete_event_driven      |                Allows to delete a ruleset for event-driven scans               |
|                         GET /rulesets                         |            ruleset:describe            |                       Allows to query available rulesets                       |
|                   GET /rulesets/event-driven                  |      ruleset:describe_event_driven     |                 Allows to list rulesets for event-driven scans                 |
|                     GET /rulesets/content                     |           ruleset:get_content          |                       Allows to retrieve ruleset content                       |
|                        PATCH /rulesets                        |             ruleset:update             |                        Allows to update a local ruleset                        |
|                  DELETE /scheduled-job/{name}                 |        scheduled-job:deregister        |                      Allows to deregister a scheduled job                      |
|                   GET /scheduled-job/{name}                   |            scheduled-job:get           |              Allows to get a registered scheduled job by its name              |
|                       GET /scheduled-job                      |           scheduled-job:query          |                    Allows to query registered scheduled jobs                   |
|                      POST /scheduled-job                      |         scheduled-job:register         |                       Allows to register a scheduled job                       |
|                  PATCH /scheduled-job/{name}                  |          scheduled-job:update          |               Allows to update a registered scheduled job by name              |
|                   PUT /integrations/temp/sre                  |         self_integration:create        |Allows to create an application with type CUSTODIAN for integration with Maestro|
|                 DELETE /integrations/temp/sre                 |         self_integration:delete        |                  Allows to delete an integration with Maestro                  |
|                   GET /integrations/temp/sre                  |        self_integration:describe       |                     Allows to get integration with Maestro                     |
|                  PATCH /integrations/temp/sre                 |         self_integration:update        |     Allows to change tenants that are active for integrations with Maestro     |
|                  POST /settings/send_reports                  |      settings:change_send_reports      |             Allows to enable or disable high-level reports sending             |
|             POST /settings/license-manager/client             |        settings:create_lm_client       |                      Allows to add license manager client                      |
|             POST /settings/license-manager/config             |        settings:create_lm_config       |                   Allows to set license manager configuration                  |
|                      POST /settings/mail                      |          settings:create_mail          |                        Allows to set mail configuration                        |
|            DELETE /settings/license-manager/client            |        settings:delete_lm_client       |                     Allows to delete license manager client                    |
|            DELETE /settings/license-manager/config            |        settings:delete_lm_config       |                 Allows to delete license manager configuration                 |
|                     DELETE /settings/mail                     |          settings:delete_mail          |                       Allows to delete mail configuration                      |
|              GET /settings/license-manager/client             |       settings:describe_lm_client      |                    Allows to describe license manager client                   |
|              GET /settings/license-manager/config             |       settings:describe_lm_config      |                   Allows to get license manager configuration                  |
|                       GET /settings/mail                      |         settings:describe_mail         |                      Allows to describe mail configuration                     |
|                      GET /metrics/status                      |          system:metrics_status         |                   Allows to get latest metrics update status                   |
|                    POST /rules/update-meta                    |           system:update_meta           |           Allows to submit a job that will pull latest rules content           |
|                      POST /metrics/update                     |          system:update_metrics         |                 Allows to submit a job that will update metrics                |
|                   GET /tenants/{tenant_name}                  |               tenant:get               |                         Allows to get a tenant by name                         |
|           GET /tenants/{tenant_name}/active-licenses          |       tenant:get_active_licenses       |         Allows to get licenses that are activated for a specific tenant        |
|           GET /tenants/{tenant_name}/excluded-rules           |        tenant:get_excluded_rules       |                Allows to get rules that are excluded for tenant                |
|                          GET /tenants                         |              tenant:query              |                             Allows to query tenants                            |
|           PUT /tenants/{tenant_name}/excluded-rules           |        tenant:set_excluded_rules       |                       Allows to exclude rules for tenant                       |
|                          POST /users                          |              users:create              |                         Allows to create a new API user                        |
|                    DELETE /users/{username}                   |              users:delete              |                        Allows to delete a specific user                        |
|                     GET /users/{username}                     |             users:describe             |                        Allows to get an API user by name                       |
|                           GET /users                          |             users:describe             |                            Allows to list API users                            |
|                       GET /users/whoami                       |            users:get_caller            |                  Allows to describe the user making this call                  |
|                   POST /users/reset-password                  |          users:reset_password          |                          Allows to change you password                         |
|                    PATCH /users/{username}                    |              users:update              |                        Allows to update a specific user                        |


3. All rules and descriptions
