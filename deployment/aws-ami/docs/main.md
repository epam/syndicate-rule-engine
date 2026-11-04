
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
syndicate re job scan aws --tenant_name TENANT_1 --region eu-west-1  # or the region you want
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

## 1.1. Licensing

### 1.1.1. What is License

License - is a logical entity that issues rule-sets for scanning. Each customer can have a license assigned to it and therefore a 
unique list of allowed rulesets. Licenses are issued by Rule Engine License Manager

### 1.1.2. How to get License

#### 1.1.2.1. AMI based

Licenses are issued automatically for each launched AMI-based instance. What license is issued depends on Marketplace `product code` 
or AWS `Account Id` where the instance is launched. Specific unique licenses can be issued as private offering.

### 1.1.3. How to prolong existing License

Contact the support team

### 1.1.4. How to update existing License

Contact the support team

### 1.1.5. How to cancel existing License

Contact the support team

# 2. Installation & Configuration Guide

## 2.1. AMI

AWS AMI image must be shared privately with your AWS Account or as a part of Marketplace product. After that you must 
launch an instance from that AMI to start using the product.

### 2.1.1. Receive & install AMI - EPAM SH & AWS Marketplace

Follow the [provisioning guide](./provisioning-via-AWS-CloudFormation.md) to start the instance and the [access guide](./access.md) to connect to it.

### 2.1.2. Configure AMI

When AMI is launched and its instance `Status check` is green you can log in to the instance via SSH and use Rule Engine immediately 
from `syndicate` entrypoint. There are two groups of commands: `syndicate re ...` and `syndicate admin ...`. 
The former is used to access scanning and reporting API (`re` - rule engine) and the latter is used to manage logical entities 
that represent accounts and organizations (`admin` - entities administrator)

#### 2.1.2.1. Activate Tenants

`Tenant` is a main entity that Rule Engine manages and requires. One tenant represents one AWS Account or AZURE subscription or
GOOGLE project that is to be scanned. If you want to scan something you must create a tenant that represents it.

When an instance is launched from AMI one tenant is created automatically. It represents the AWS account where the AMI 
was launched. Its name is taken from the `Tenant Name` CloudFormation parameter (`TENANT_NAME` env); if it is empty, 
the AWS Account Alias is used, and the default is `TENANT_1`. By default, all AWS regions are activated for this tenant. 
You can restrict them using the `Tenant regions to activate` CloudFormation parameter (`TENANT_AWS_REGIONS` env).
In case instance profile allowed READ access to the AWS account that tenant can be immediately used.

##### 2.1.2.1.1. Activating Tenants linked to cloud accounts

If you want to scan an account other from the one where the instance is launched you must do three configuration steps:
- create a tenant entity that represents the account you want to scan:

    ```bash
    syndicate admin tenant create --name "MY_OTHER_ACCOUNT" --display_name "Dev account" --cloud AWS --account_id 111111111111 --primary_contacts admin@example.com --secondary_contacts admin@example.com --tenant_manager_contacts admin@example.com --default_owner admin@example.com
    ```
  
- activate necessary regions for the tenant:

    ```bash
    syndicate admin tenant regions activate --tenant_name MY_OTHER_ACCOUNT --region_name eu-west-1
    syndicate admin tenant regions activate --tenant_name MY_OTHER_ACCOUNT --region_name eu-central-1
    ```

- configure AWS access keys or access role for that specific tenant:

    Access keys can be provided individually for each scan which is definitely the case, but it's somewhat inconvenient:

    ```bash
    syndicate re job scan aws --tenant_name MY_OTHER_ACCOUNT --access_key_id $AWS_ACCESS_KEY_ID --secret_access_key $AWS_SECRET_ACCESS_KEY --session_token $AWS_SESSION_TOKEN --region eu-west-1
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
    same way one Application with AZURE Certificate can be linked to multiple AZURE tenants (remember, one tenant is one subscription)


#### 2.1.2.2. Create API Users

When the AMI is launched admin users are created for Rule Engine and for Modular Service. They are configured by default. 
Their passwords are stored in Kubernetes secrets and can be read using `sre-init secrets rule-engine-admin-password` and 
`sre-init secrets modular-service-admin-password` correspondingly (all the sensitive information is listed in `Attachment 1`). 
Username `admin` is default for both and can be configured via the `Syndicate Rule Engine admin username` and 
`Syndicate Modular Service admin username` CloudFormation parameters (or `RULE_ENGINE_USERNAME` and 
`MODULAR_SERVICE_USERNAME` envs in the user data script) before instance startup.

Your admin Rule engine user has rights to manage other users inside your customer:
```bash
syndicate re users describe
```

Each user has an assigned role. Each role can have multiple policies attached. Each policy can allow or deny specific 
actions over API, so you can flexibly configure access to the system

##### 2.1.2.2.1. Users registration

Let's create a new user, but first we should create a separate policy and role for him. Create a policy:

```bash
syndicate re policy add --name run_scan_for_all_tenants --permission "job:post_for_tenant" --effect allow --description "Allows to submit jobs for all tenants" --tenant '*'
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


#### 2.1.2.3. Activate License

Each AMI-based instance will have a single license which can give multiple rule-sets. You can add more licenses to the
installation if you have license keys. Those can be issued by the Rule Engine License Manager team. Let's assume you have one
and want to add it to the installation. First, just add a license:
```bash
syndicate re license add --tenant_license_key $TENANT_LICENSE_KEY
```

After that you should activate the license for tenants. You must do that because you can have overlap (two different licenses 
can issue different rulesets for the same tenant):

```bash
syndicate re license activate --license_key <License key from previous command> --tenant_name TENANT_1 --tenant_name ANOTHER_ACCOUNT
```
**Note:** `License Key from previous command` is not the same as `$TENANT_LICENSE_KEY`


### 2.1.3. Initialize AMI for another linux user

The AMI has a script called `sre-init` that allows to configure `syndicate` CLI access for an additional linux user. 
By default, only the first non-root linux user has `syndicate` installed. Use the `init` subcommand to add another one:

```bash
sre-init init --user "username" --public-ssh-key "ssh-ed25519 AAAA..."
```
**Note:** must be run directly by a user with `sudo` rights (normally the first, already initialized user) — do **not**
prefix the whole command with `sudo` yourself; `sre-init` escalates internally (via `sudo useradd`/`sudo su -`) only
for the specific steps that need it. Running the whole command as `sudo`/`root` breaks `kubectl`/`minikube` access,
since their config only exists under the first user's home directory. You do **not** need to create the linux user
beforehand either — `sre-init init --user` creates it automatically (via `useradd`) if it does not exist yet.

What this command does for `username`:
- creates the linux user if it does not already exist;
- if `--public-ssh-key` is given, appends it to `~/.ssh/authorized_keys` so the user can log in over SSH;
- installs `modular-cli` and `sre-obfuscator` CLIs for that user (via `pipx`);
- creates a corresponding `modular-api` user (if one does not exist yet) and logs `syndicate` in automatically.

Required parameters:
- either `--system` (first-time, system-wide init; only possible for the very first user) or `--user <username>` must
  be specified — exactly one of these flows must be chosen.

Optional parameters:
- `--public-ssh-key "<key>"` — public SSH key to add to the target user's `authorized_keys`.
- `--re-username` & `--re-password` — must be given **together**. That Rule Engine user must **already exist** —
  `sre-init` only logs the linux user into it (`syndicate re login`), it does not create it. Create the user
  beforehand (see [2.1.2.2.1](#21221-users-registration)).
- `--admin-username` & `--admin-password` — must be given **together**. That Modular Service user must likewise
  **already exist** — `sre-init` only logs in (`syndicate admin login`), it does not create it. Create it beforehand,
  e.g.:
  ```bash
  syndicate admin policy add --name admin_policy --permissions_admin --customer_id "$CUSTOMER_NAME"
  syndicate admin role add --name admin_role --policies admin_policy --customer_id "$CUSTOMER_NAME"
  syndicate admin users create --username user1 --password "$SECRET_PASSWORD" --role_name admin_role --customer_id "$CUSTOMER_NAME"
  ```

Example: onboarding a User1 user with an SSH key and an existing Rule Engine user:

```bash
sre-init init --user user1 \
  --public-ssh-key "ssh-ed25519 AAAA... user1@example" \
  --re-username user1 --re-password "$SECRET_PASSWORD"
```

> **Note:** `modular-cli`/`sre-obfuscator` are installed per linux user via `pipx`. Every `sre-init update` also
> auto-detects other onboarded users (those with either CLI installed via `pipx`) and refreshes their CLIs to the
> new release; a refresh failure for one user only prints a warning and does not fail the whole update. Pass
> `--no-secondary-user-clis` to `sre-init update` to skip this step. You can also refresh CLIs independently at any
> time with `sre-init update cli` (see `sre-init update cli --help`).

# 3. Scanning Guide

Scans can be executed individually for each tenant. By default, all the active regions inside the tenant will be scanned 
using all the rules from the default license. You can specify exact regions and rule-sets you want to use during the scan 
when you submit it

## 3.1. Requesting full scan

To request a scan for `TENANT_1` tenant for all regions and all rules use the following command:

```bash
syndicate re job scan aws --tenant_name TENANT_1 --region eu-west-1
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
syndicate re job scan aws --tenant_name TENANT_1 --ruleset FULL_AWS --region eu-west-1
```

Also, you can restrict the scope to specific rule names. List of all rules and their descriptions can be received using 
`syndicate re rule describe`

```bash
syndicate re job scan aws --tenant_name TENANT_1 --ruleset FULL_AWS --rules_to_scan ecc-aws-001... --rules_to_scan ecc-aws-002...
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

When the AMI-based instance is running you can access Defect Dojo Ui on `http://<instance public ipv4>` (port `80`). Admin 
password can be read using `sre-init secrets dojo-system-password`. Admin username is `admin`. Rule Engine is configured to push results of 
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
`sreobf` cli tool. Just specify path to the obfuscated report (or directory with multiple obfuscated reports) and 
path to the dictionary:

```bash
sreobf deobfuscate --dump-directory obfuscated.eml --to result --dictionary obfuscation_dictionary.json
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


# Attachments:

1. All secrets inside AMI

All the secrets that are generated during installation are stored in Kubernetes secrets (`minio-secret`, `mongo-secret`,
`vault-secret`, `valkey-secret`, `rule-engine-secret`, `modular-api-secret`, `modular-service-secret`,
`defectdojo-secret` and `lm-data`). The passwords of the users can be read using `sre-init secrets <name>`, where
`<name>` is one of `rule-engine-admin-password`, `rule-engine-system-password`, `modular-service-admin-password`,
`modular-service-system-password`, `modular-api-system-password` and `dojo-system-password`.
See [Annex 1](../../../docs/sre_user_guide.md#annex-1-secrets-inside-ami) of the user guide for details.

2. All permissions

The up-to-date list of permissions and the endpoints they protect is in
[Annex 2](../../../docs/sre_user_guide.md#annex-2-permissions) of the user guide.
