## Install

    pip install cli/

## Configure user credentials

    sre configure --api_link <Custodian_Service_API_link>

    sre login --username <username> --password <password>

## Commands

### Commands

[`login`](#login)      Authenticates user to work with Custodian Service.

[`configure`](#configure)  Configures sre tool to work with Custodian Service.

[`cleanup`](#cleanup)  Removes all the configuration data related to the tool.

[`health`](#health)  Checks Custodian Service components availability (for onprem mode).

### Command groups

[`account`](#account) - Manages Account Entity

[`customer`](#customer)   Manages Customer Entity

[`job`](#job)       Manages Custodian Service jobs

[`lm`](#lm)      Manages License Entity

[`policy`](#policy)      Manages Policy Entity

[`report`](#report)      Manages Custodian Service reports

[`role`](#role)        Manages Role Entity

[`rule`](#rule)        Manages Rule Entity

[`rulesource`](#rulesource)      Manages Rule Source entity

[`ruleset`](#ruleset)      Manages Ruleset Entity

[`setting`](#setting)      Manages Custodian Service Settings

[`siem`](#siem)      Manages SIEM configuration

[`tenant`](#tenant)      Manages Tenant Entity

[`user`](#user)      Manages User Entity

## Commands

### login

**Usage:**

    sre login --username USERNAME --password PASSWORD

_Authenticates user to work with Custodian as a Service. Pay attention that,
the password can be entered in an interactive mode_

`-u,--username` `TEXT` Custodian Service user username. [Required]

`-p,--password` `TEXT` Custodian Service user password. [Required]

### configure

**Usage:**

    sre configure --api_link <Custodian_Service_API_link>

_Configures sre tool to work with Custodian as a Service._

`-api,--api_link` `TEXT` Link to the Custodian as a Service host. [Required]

### cleanup

**Usage:**

    sre cleanup

_Removes all the configuration data related to the tool._

## Command groups

----------------------------------------

### `account`

**Usage:** `sre account  COMMAND [ARGS]...`

_Manages Account Entity_

### Commands

[`activate`](#account-activate) Creates Custodian Service Account entity.

[`deactivate`](#account-deactivate) Deletes the account entity by the provided
account name.

[`describe`](#account-describe) Describes Custodian Service account entities.

[`update`](#account-update) Updates Custodian Service Account entity.

### Command groups

[`region`](#account-region) Manages Account Region entity

[`findings`](#account-findings) Manages Account Findings state

----------------------------------------

### `customer`

**Usage:** `sre customer  COMMAND [ARGS]...`

_Manages Customer Entity_

### Commands

[`activate`](#customer-activate) Creates Custodian Service Customer entity.

[`deactivate`](#customer-deactivate) Deletes the Customer entity by the
provided name.

[`describe`](#customer-describe) Describes Custodian Service customer entities.

[`update`](#customer-update) Updates Custodian Service Customer entity.

----------------------------------------

### `job`

**Usage:** `sre job  COMMAND [ARGS]...`

_Manages Custodian Service jobs_

### Commands

[`describe`](#job-describe) Describes Custodian Service Scans

[`terminate`](#job-terminate) Terminates Custodian Service Scan

### Command groups

[`submit`](#job-submit) Manages Job submit action

----------------------------------------

### `policy`

**Usage:** `sre policy  COMMAND [ARGS]...`

_Manages Policy Entity_

[`add`](#policy-add) Creates a Custodian Service policy for the specified customer.

[`clean-cache`](#policy-clean-cache) Cleans cached policy from lambda.

[`delete`](#policy-delete) Deletes customers policy.

[`describe`](#policy-describe) Describes a Custodian Service policies for the given
customer.

[`update`](#policy-update) Updates list of permissions attached to the policy.


----------------------------------------

### `report`

**Usage:** `sre report  COMMAND [ARGS]...`

_Manages Custodian Service reports_

[`compliance`](#report-compliance) Generates compliance report

[`error`](#report-error) Describes error report

[`job`](#report-job) Describes job report

[`rule`](#report-rule) Describes rule report

--------------------------------------------------

### `role`

**Usage:** `sre role  COMMAND [ARGS]...`

_Manages Role Entity_

[`add`](#role-add) Creates the Role entity with the given name from Customer...

[`clean-cache`](#role-clean-cache) Cleans cached role from lambda.

[`delete`](#role-delete) Deletes customers role.

[`describe`](#role-describe) Describes a Custodian Service roles for the given customer.

[`update`](#role-update) Updates role configuration.

----------------------------------------

### `rule`

**Usage:** `sre rule  COMMAND [ARGS]...`

_Manages Rule Entity_

[`describe-rules`](#rule-describe) Describes Custodian Service rules for the given customer.

[`update-rules`](#rule-update-rules) Updates Customer's rules meta

[`delete-rules`](#rule-delete) Deletes Custodian Service rules for the given customer

----------------------------------------

### `tenant`

**Usage:** `sre tenant  COMMAND [ARGS]...`

_Manages Tenant Entity_

### Commands

[`activate`](#tenant-activate) Creates Custodian Service Tenant entity.

[`deactivate`](#tenant-deactivate) Deletes the Tenant entity by the provided
name.

[`describe`](#tenant-describe) Describes Custodian Service tenant entities.

[`update`](#tenant-update) Updates Custodian Service Tenant entity.

---------------------------------------

### `siem`

### Commands

[`delete`](#siem-delete) Deletes SIEM configuration

[`describe`](#siem-describe) Describes a SIEM manager configuration for the given customer

### Command groups

[`add`](#siem-add) Manages SIEM configuration create action

[`update`](#siem-update) Manages SIEM configuration update action

----------------------------------------

### `setting`

### Command groups

[`lm`](#setting-lm) Manages License Manager Setting(s)

[`mail`](#setting-mail) Manages Custodian Service Mail configuration

-----------------------------------------

### `user`

**Usage:** `sre user [OPTIONS] COMMAND [ARGS]...`

_Manages Rule User Entity_

### Command groups

[`tenants`](#user-tenants) Manages custom attribute that is responsible for tenants

----------------------------------------

### account-activate

**Usage:** `sre account activate`

_Creates Custodian Service Account entity._

`-n,--name` `TEXT` [Required]

`-t,--tenant` `TEXT` Name of related tenant [Required]

`-c,--cloud` `TEXT` Cloud name. Possible options: AWS, AZURE, GCP [Required]

`-cust,--customer` `TEXT` Name of related customer [Optional]

`-cid,--cloud_identifier` `TEXT` Cloud account identifier ('account id' for
AWS, 'subscription id' for AZURE, 'project id' for GCP) [Optional]

`-inh,--inherit` `BOOLEAN` Inherit configuration from
tenant/customer [Optional]

`-S,--send_scan_result` `FLAG` Flag for sending a file with information about found vulnerabilities to customer
contacts after scanning this account. Default value: False [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account activate -n ACCOUNT_NAME  -t TENANT_NAME  -cust CUSTOMER_NAME -c AWS --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "ACCOUNT_NAME",
      "tenant_display_name": "TENANT_NAME",
      "customer_display_name": "CUSTOMER_NAME",
      "cloud": "aws",
      "inherit": true,
      "activation_date": "2021-10-13T21:03:47.535730"
    }
  ]
}
```

----------------------------------------

### account-deactivate

**Usage:** `sre account deactivate`

_Deletes the account entity by the provided account name._

`-n,--name` `TEXT` Account name to delete [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account deactivate -n ACCOUNT_NAME --json

**Response**

```json
    {
  "message": "account with name 'ACCOUNT_NAME' has been deleted"
}
```

    
----------------------------------------

### account-describe

**Usage:** `sre account describe`

_Describes Custodian Service account entities._

`-n,--name` `TEXT` Account name to describe [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account describe --json

**Response**

```json lines
    {
  "items": [
    {
      "display_name": ACCOUNT_NAME,
      "tenant_display_name": TENANT_NAME,
      "customer_display_name": CUSTOMER_NAME,
      "cloud": <String>,
      "inherit": <boolean>,
      "activation_date": <String>,
      "regions": [
        <String>,
        <String>,
        <String>
      ],
      "cloud_identifier": <String>
    },
    ...,
    {
      ...
    },
    ...
  ]
}
```

----------------------------------------

### account-update

**Usage:** `sre account update`

_Updates Custodian Service Account entity._

`-n,--name` `TEXT` [Required]

`-t,--tenant` `TEXT` Name of related tenant [Optional]

`-cust,--customer` `TEXT` Name of related customer [Optional]

`-inh,--inherit` `BOOLEAN` Inherit configuration from
tenant/customer [Optional]

`-c,--cloud` `TEXT` Cloud name. Possible options: AWS, AZURE, GCP [Optional]

`-cid,--cloud_identifier` `TEXT` Cloud account identifier ('account id' for
AWS, 'subscription id' for AZURE, 'project id' for GCP) [Optional]

`-S,--send_scan_result` `FLAG` Flag for sending a file with information about found vulnerabilities to customer
contacts after scanning this account. Default value: False [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre account update -n ACCOUNT_NAME  -t TENANT_NAME  -cust CUSTOMER_NAME -inh true -cid 87878es8eftsetfwefk  -c AWS  --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "ACCOUNT_NAME",
      "tenant_display_name": "TENANT_NAME",
      "customer_display_name": "CUSTOMER_NAME",
      "cloud": "aws",
      "inherit": true,
      "activation_date": "2021-10-13T21:03:47.535730"
    }
  ]
}
```

    
----------------------------------------------------------------

### customer-activate

**Usage:** `sre customer activate`

_Creates Custodian Service Customer entity._

`-n,--name` `TEXT` Customer name to activate [Required]

`-o,--owner` `TEXT` Customer owner email [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre customer activate -n CUSTOMER_NAME -o oleksandr_hrechenko@epam.com

**Response**

```json
    {
  "items": [
    {
      "display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T21:12:11.242087",
      "owner": "oleksandr_hrechenko@epam.com"
    }
  ]
}
```

----------------------------------------

### customer-deactivate

**Usage:** `sre customer deactivate`

_Deletes the Customer entity by the provided name._

`-n,--name` `TEXT` Customer name to delete [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre customer deactivate -n CUSTOMER_NAME --json

**Response**

```json
    {
  "message": "customer with name 'CUSTOMER_NAME' has been deleted"
}
```

    
----------------------------------------

### customer-describe

**Usage:** `sre customer describe`

_Describes Custodian Service customer entities._

`-n,--name` `TEXT` Customer name to describe.

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre customer describe -n CUSTOMER_NAME  --json

**Response**

````json
    {
  "items": [
    {
      "display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T12:50:03.035546",
      "owner": "oleksandr_hrechenko@epam.com"
    }
  ]
}
````

----------------------------------------

### customer-update

**Usage:** `sre customer update`

_Updates Custodian Service Customer entity._

`-n,--name` `TEXT` Customer name to update [Optional]

`-o,--owner` `TEXT` Customer owner email [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre customer update -n CUSTOMER_NAME -o name_surname@epam.com --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T12:50:03.035546",
      "owner": "name_surname@epam.com"
    }
  ]
}
```

   
----------------------------------------

### job-describe

**Usage:** `sre job describe`

_Describes Custodian Service Scans_

`-id,--job_id` `TEXT` Job id to describe [Optional]

`-acc,--account` `TEXT` Account name to describe job [Optional]

`-cust,--customer` `TEXT` Customer name to describe jobs [Optional]

`-ed,--event_driven` `FLAG` If specified, only event-driven job will be shown [Optional]

`-l,--limit` `INTEGER` Number of records to show. Default=10 [Optional]

`-o,--offset` `INTEGER` Number of first records to skip. Default=0 [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre job describe --json

**Response**

```json
    {
  "items": [
    {
      "job_id": "oisodu98y98y9sd",
      "account_display_name": "ACCOUNT_NAME",
      "job_owner": "Oceanic_Airlines",
      "status": "SUCCEEDED",
      "scan_regions": [
        "eu-west-4"
      ],
      "scan_rulesets": [
        "FULL_AZURE"
      ],
      "submitted_at": "2021-10-13 14:37:32",
      "created_at": "2021-10-13 14:37:32",
      "started_at": "2021-10-13 14:40:41",
      "stopped_at": "2021-10-13 14:45:46",
      "customer_display_name": "CUSTOMER_NAME"
    },
    ...
    {
      ...
    },
    ...
  ]
}
```

----------------------------------------

### job-terminate

**Usage:** `sre job terminate`

_Terminates Custodian Service Scan_

`-id,--job_id` `TEXT` Job id to terminate [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

----------------------------------------

### job-register

**Usage:** `sre job register`

_Register one scheduled job_

`-acc,--account` `TEXT` Account name to describe job [Required]

`-tn,--tenant_name` `TEXT` Name of related tenant [Optional]

`-s,--schedule` `TEXT` Cron or Rate expression: cron(0 20 * * *), rate(2 minutes) [Required]

`-trs,--target_ruleset` `TEXT` Rulesets to scan. If not specified, all available rulesets will be used [Optional]

`-trg,--target_region` `TEXT` Regions to scan. If not specified, all active regions will be used [Optional]

`-n,--name` `TEXT` Name for the scheduled job. Must be unique. If not given, will be generated automatically [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table view [Optional]

`-cust, --customer` `TEXT` Customer name to manager the entity from

----------------------------------------

### job-update

**Usage:** `sre job update`

_Update data of already registered scheduled job_

`-n,--name` `TEXT` Scheduled job name to update [Required]

`-s,--schedule` `TEXT` Cron or Rate expression: cron(0 20 * * *), rate(2 minutes) [Optional]

`-e,--enabled` `BOOLEAN` Param to enable or disable the job temporarily [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table view [Optional]

`-cust, --customer` `TEXT` Customer name to manager the entity from

----------------------------------------

### job-deregister

**Usage:** `sre job deregister`

_Remove one registered scheduled job_

`-n,--name` `TEXT` Scheduled job name to remove [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table view [Optional]

`-cust, --customer` `TEXT` Customer name to manager the entity from

----------------------------------------

### policy-add

**Usage:** `sre policy add`

_Creates a Custodian Service policy for the specified customer._

`-cust, --customer` `TEXT` Customer name to attach policy to [Required]

`-name, --policy_name` `TEXT` Policy name to create [Required]

`-p, --permission` `TEXT` List of permissions to attach to the
policy [Required]

`-padm, --permissions_admin` `FLAG` Adds all admin permissions [Optional]

`-path, --path_to_permissions` `TEXT` Path to .json file that contains list of
permissions to attach to the policy [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre policy add -name POLICY_NAME -p "system:update_meta" -p  "rule:describe_rule" -cust CUSTOMER_NAME --json

**Response**

```json
    {
  "items": [
    {
      "customer": "CUSTOMER_NAME",
      "name": "POLICY_NAME",
      "permissions": [
        "system:update_meta",
        "rule:describe_rule"
      ]
    }
  ]
}
```

----------------------------------------

### policy-clean-cache

**Usage:** `sre policy clean-cache`

_Cleans cached policy from lambda._

`-c,--customer` `TEXT` Customer name to clean policy cache from [Optional]

`-name,--policy_name` `TEXT` Policy name to clean from cache [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre policy clean-cache --customer CUSTOMER_NAME --policy_name POLICY_NAME --json

**Response**

```json
    {
  "message": "Cache for policy with name 'POLICY_NAME' from customer 'CUSTOMER_NAME' has been deleted"
}
```

----------------------------------------

### policy-delete

**Usage:** `sre policy delete`

_Deletes customers policy._

`-cust,--customer` `TEXT` Customer name to delete policy from [Required]

`-name,--policy_name` `TEXT` Policy name to delete [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre policy delete -cust  CUSTOMER_NAME -name POLICY_NAME --json

**Response**

```json
    {
  "message": "policy with name 'POLICY_NAME' from customer: CUSTOMER_NAME has been deleted"
}
```

----------------------------------------

### policy-describe

**Usage:** `sre policy describe`

_Describes a Custodian Service policies for the given customer._

`-cust,--customer` `TEXT` Customer name to describe policy [Optional]

`-name,--policy_name` `TEXT` Policy name to describe [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre policy describe -cust  CUSTOMER_NAME -name POLICY_NAME --json

**Response**

```json
    {
  "items": [
    {
      "customer": "CUSTOMER_NAME",
      "name": "POLICY_NAME",
      "permissions": [
        "system:update_meta",
        "rule:describe_rule"
      ]
    }
  ]
}
```

----------------------------------------

### policy-update

**Usage:** `sre policy update`

_Updates list of permissions attached to the policy._

`-cust,--customer` `TEXT` Customer name to attach policy to [Required]

`-name,--policy_name` `TEXT` Policy name to update [Required]

`-a,--attach_permission` `TEXT` Names of permissions to attach to the
policy [Optional]

`-d,--detach_permission` `TEXT` Names of permissions to detach from the
policy [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre policy update --customer CUSTOMER_NAME --policy_name POLICY_NAME --json --detach_permission rule:describe_rule

**Response**

```json
    {
  "items": [
    {
      "customer": "CUSTOMER_NAME",
      "name": "POLICY_NAME",
      "permissions": [
        "system:update_meta"
      ]
    }
  ]
}
```

----------------------------------------

### report-compliance

**Usage:** `sre report compliance`

_Generates compliance report_

`-s,--start_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format. `Example:` 2021-09-22T00:00:
00.000000 [Required]

`-e,--end_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format. `Example:` 2021-09-22T00:00:
00.000000 [Required]

`-c,--cloud` `TEXT` Cloud name. Possible options: AWS, AZURE, GCP [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre report compliance --start_date 2021-11-08T00:00:00.000000 --end_date 2021-11-09T00:00:00.000000 --cloud AWS --json

**Response**

```json
    {
  "items": [
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.xlsx",
      "presigned_url": "https://URL"
    },
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.xlsx",
      "presigned_url": "https://URL"
    },
    ...
  ]
}
```

----------------------------------------

### report-error

**Usage:** `sre report error`

_Describes error report_

`-s,--start_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format. `Example:` 2021-09-22T00:00:
00.000000 [Required]

`-e,--end_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format.`Example:` 2021-09-22T00:00:
00.000000 [Required]

`-c,--cloud` `TEXT` Cloud name. Possible options: AWS, AZURE, GCP [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre report error --start_date 2021-11-08T00:00:00.000000 --end_date 2021-11-09T00:00:00.000000 --cloud AWS --json

**Response**

```json
    {
  "items": [
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.json",
      "presigned_url": "https://URL"
    },
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.json",
      "presigned_url": "https://URL"
    },
    ...
  ]
}
```

----------------------------------------

### report-job

**Usage:** `sre report job`

_Describes job report_

`-id,--job_id` `TEXT` Job id to describe report. Optional if `account_name` is
specified

`-an,--account_name` `TEXT` Account name to describe report. Optional
if `job_id` is specified

`-d,--detailed` `FLAG` Return detailed report. Strongly recommend use this
parameter only with the `--json` parameter [Optional]

`-url,--get_url` `FLAG` Return presigned urls instead of the actual 
reports. [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre report job --job_id JOB_ID --detailed --json

**Response**

```json
    {
  "items": [
    {
      "job_id": "JOB_ID",
      "account_display_name": "ACCOUNT",
      "total_checks_performed": 703,
      "successful_checks": 585,
      "failed_checks": 118,
      "total_resources_violated_rules": 1521
    }
  ]
}
```

----------------------------------------

### report-rule

**Usage:** `sre report rule`

_Describes rule report_

`-s,--start_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format.`Example:` 2021-09-22T00:00:
00.000000 [Required]

`-e,--end_date` `FROMISOFORMAT` Generate report FROM date. ISO 8601
format.`Example:` 2021-09-22T00:00:
00.000000 [Required]

`-c,--cloud` `TEXT` Cloud name. Possible options: AWS, AZURE, GCP [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre report rule --start_date 2021-11-08T00:00:00.000000 --end_date 2021-11-09T00:00:00.000000 --cloud AWS --json

**Response**

```json
    {
  "items": [
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.json",
      "presigned_url": "https://URL"
    },
    {
      "bucket_name": "BUCKET_NAME",
      "file_key": "FILEKEY.csv",
      "presigned_url": "https://URL"
    },
    ...
  ]
}
```

----------------------------------------

### role-add

**Usage:** `sre role add`

_Creates the Role entity with the given name from Customer with the given id_

`-cust,--customer` `TEXT` Customer name to attach role to [Optional]

`-n,--name` `TEXT` Role name [Required]

`-p,--policies` `TEXT` List of policies to attach to the role [Required]

`-e,--expiration` `TEXT` Expiration date, ISO 8601.
Example:`2021-08-01T15:30:00` [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre role add -n ROLE_NAME -cust CUSTOMER_NAME -p POLICY_NAME --json

**Response**

```json
    {
  "items": [
    {
      "name": "ROLE_NAME",
      "customer": "CUSTOMER_NAME",
      "policies": [
        "POLICY_NAME"
      ],
      "expiration": "2022-01-11T21:08:08.198628"
    }
  ]
}
```

----------------------------------------

### role-clean-cache

**Usage:** `sre role clean-cache`

_Cleans cached role from lambda._

`-cust,--customer` `TEXT` Customer name to clean roles cache from [Optional]

`-n,--name` `TEXT` Role name to clean from cache [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre role clean-cache --json

**Response**

```json
    {
  "message": "Roles cache has been deleted"
}
```

----------------------------------------

### role-delete

**Usage:** `sre role delete`

_Deletes customers role._

`-cust,--customer` `TEXT` Customer name to delete role from [Optional]

`-n,--name` `TEXT` Role name to delete [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre role delete --name ROLE_NAME --customer CUSTOMER_NAME --json

**Response**

```json
    {
  "message": "role with name 'ROLE_NAME' from customer: CUSTOMER_NAME has been deleted"
}
```

----------------------------------------

### role-describe

**Usage:** `sre role describe`

_Describes a Custodian Service roles for the given customer._

`-cust,--customer` `TEXT` Customer name to describe role [Optional]

`-n,--name` `TEXT` Role name to describe [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre role describe -cust CUSTOMER_NAME -n ROLE_NAME --json

**Response**

```json
    {
  "items": [
    {
      "name": "ROLE_NAME",
      "customer": "CUSTOMER_NAME",
      "policies": [],
      "expiration": "2022-01-11T21:08:08.198628"
    }
  ]
}
```

----------------------------------------

### role-update

**Usage:** `sre role update`

_Updates role configuration._

`-cust,--customer` `TEXT` Customer name to update role from [Required]

`-n,--name` `TEXT` Role name to modify [Required]

`-a,--attach_policy` `TEXT` List of policies to attach to the role [Optional]

`-d,--detach_policy` `TEXT` List of policies to detach from role [Optional]

`-e,--expiration` `TEXT` Expiration date, ISO 8601.
Example:`2021-08-01T15:30:00` [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre role update -cust CUSTOMER_NAME -n ROLE_NAME -d POLICY_NAME --json

**Response**

```json
    {
  "items": [
    {
      "name": "ROLE_NAME",
      "customer": "CUSTOMER_NAME",
      "policies": [],
      "expiration": "2022-01-11T21:08:08.198628"
    }
  ]
}
```

----------------------------------------

### rule-describe

**Usage:** `sre rule describe`

_Describes a Custodian Service rules for the given customer._

`-cust,--customer` `TEXT` Customer name to describe rules [Optional]

`-id,--rule_id` `TEXT` Rule id to describe [Optional]

`-c,--cloud` `TEXT` Display only rules of specific cloud. Possible options:AWS,
AZURE, GCP [Optional]

`-pn,--page_number` `INTEGER` Results page number. Default=0 [Optional]

`-l,--limit` `INTEGER` Max number of records per page. Default=10 [Optional]

`-o,--offset` `INTEGER` Number of first records to skip. Default=0 [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre rule describe --json

**Response**

```json
    {
  "items": [
    {
      "id": "epam-aws-002-ensure_mfa_is_enabled_for_all_iam_users_with_console_password_1.0",
      "name": "epam-aws-002-ensure_mfa_is_enabled_for_all_iam_users_with_console_password",
      "version": "1.0",
      "cloud": "AWS",
      "description": "1.10 Ensure multi-factor authentication (MFA) is enabled for all IAM users that have a console password\n",
      "updated_date": "2021-04-22T11:11:48.000+03:00",
      "customer": "EPAM Systems",
      "standard": {
        "ISO 27017": [
          "6.2.2",
          "9.1.2",
          "9.2.3",
          "9.3.1",
          "9.4.2"
        ],
        "ISO 27018": [
          "6.2",
          "9.1",
          "9.2.3",
          "9.3.1",
          "9.4.2"
        ],
        "ISO 27002": [
          "10.1.1",
          "10.1.2",
          "9.3.1"
        ],
        "NIST 800-53 Rev5": [
          "IA-3",
          "SC-18"
        ],
        "ISO 27001 : 2013": [
          "A.10.1.1",
          "A.10.1.2",
          "A.9.3.1"
        ],
        "HIPAA": [
          "164.312(a)(1)"
        ],
        "CIS Controls v7": [
          "4.5"
        ],
        "NIST CSF v1.1": [
          "PR.AC-1",
          "PR.AC-3",
          "PR.AC-7"
        ],
        "ISO 27701": [
          "6.6.1.2",
          "6.6.2.3",
          "6.6.3.1",
          "6.6.4.2"
        ],
        "HITRUST": [
          "01.q"
        ]
      }
    },
    ...
    {
      ...
    },
    ...
  ]
}
```

----------------------------------------

### rule-delete

**Usage:** `sre rule delete`

_Deletes a Custodian Service rules for the given customer._

`-cust,--customer` `TEXT` Customer name to delete rules

`-id,--rule_id` `TEXT` Rule id to delete

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

If `rule_id` not specified, all customer rules will be deleted

If `rule_id` and `customer` not specified, all available rules will be
deleted (system admin only)

**Request**

    sre rule delete --customer CUSTOMER_NAME --rule_id RULE_ID --json

**Response**

```json
    {
  "message": "Rule with id 'RULE_ID' has been deleted"
}
```

----------------------------------------

### rule-update-rules

**Usage:** `sre rule update-rules`

_Updates Customer's rules meta_

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Response**

```json
    {
  "message": "Rule update event has been submitted"
}
```

----------------------------------------

### tenant-activate

**Usage:** `sre tenant activate`

_Creates Custodian Service Tenant entity._

`-n,--name` `TEXT` [Required]

`-cust,--customer` `TEXT` Name of related customer [Required]

`-inh,--inherit` `BOOLEAN` Inherit configuration from customer

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre tenant activate -n TENANT_NAME -cust CUSTOMER_NAME --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "TENANT_NAME",
      "customer_display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T20:39:52.037409",
      "inherit": true
    }
  ]
}
```

----------------------------------------

### tenant-deactivate

**Usage:** `sre tenant deactivate`

_Deletes the Tenant entity by the provided name._

`-n,--name` `TEXT` Tenant name to delete [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre tenant deactivate -n TENANT_NAME --json

**Response**

```json
    {
  "message": "tenant with name 'TENANT_NAME' has been deleted"
}
```

----------------------------------------

### tenant-describe

**Usage:** `sre tenant describe`

_Describes Custodian Service tenant entities._

`-n,--name` `TEXT` Tenant name to describe.

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre tenant describe -n TENANT_NAME --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "TENANT_NAME",
      "customer_display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T20:39:52.037409",
      "inherit": true
    }
  ]
}
```

----------------------------------------

### tenant-update

**Usage:** `sre tenant update`

_Updates Custodian Service Tenant entity._

`-n,--name` `TEXT` [Required]

`-cust,--customer` `TEXT` Name of related customer

`-inh,--inherit` `BOOLEAN` Inherit configuration from tenant/customer

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre tenant update -n TENANT_NAME -inh false --json

**Response**

```json
    {
  "items": [
    {
      "display_name": "TENANT_NAME",
      "customer_display_name": "CUSTOMER_NAME",
      "activation_date": "2021-10-13T20:39:52.037409",
      "inherit": false
    }
  ]
}
```

-------------------------------------------------

### account-region

**Usage:** `sre account region  COMMAND [ARGS]...`

_Manages Account Region entity_

[`activate`](#account-region-activate) Creates Account region.

[`delete`](#account-region-delete) Deletes Account region.

[`describe`](#account-region-describe) Describes Accounts regions.

[`update`](#account-region-update) Updates Account region.

---------------------------------------------------

### account-region-activate

**Usage:** `sre account region activate`

_Creates Account region._

`-acc,--account` `TEXT` Account name to create region in [Required]

`-name,--region_name` `TEXT` Region name to create [Required]

`-ina,--inactive` Pass this flag to deactivate the region [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account region activate -acc ACCOUNT_NAME -name us-west1 --json

**Response**

```json
    {
  "items": [
    {
      "name": "us-west1",
      "state": "ACTIVE",
      "activation_date": "2021-10-13T21:41:46.492996"
    }
  ]
}
```

----------------------------------------

### account-region-delete

**Usage:** `sre account region delete`

_Deletes Account region._

`-acc,--account` `TEXT` Account name to update region in [Required]

`-name,--region_name` `TEXT` Region name to update [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

     sre account region delete -acc ACCOUNT_NAME -name us-west1 --json

**Response**

```json
    {
  "message": "Region with name 'us-west1' has been removed from account with display name 'ACCOUNT_NAME'"
}
```

----------------------------------------

### account-region-describe

**Usage:** `sre account region describe`

_Describes Accounts regions._

`-acc,--account` `TEXT` Account name to describe regions [Required]

`-name,--region_name` `TEXT` Region name to describe [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account region describe -acc ACCOUNT_NAME --json

**Response**

```json
    {
  "items": [
    {
      "name": "us-west1",
      "state": "ACTIVE",
      "activation_date": "2021-10-13T21:45:25.954302"
    }
  ]
}
```

----------------------------------------

### account-region-update

**Usage:** `sre account region update`

_Updates Account region._

`-acc,--account` `TEXT` Account name to update region in. [Required]

`-name,--region_name` `TEXT` Region name to update. [Required]

`-ina,--inactive` Pass this flag to deactivate the region [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account region update -acc ACCOUNT_NAME -name us-west1 -ina --json

**Response**

```json
    {
  "items": [
    {
      "name": "us-west1",
      "state": "INACTIVE",
      "activation_date": "2021-10-13T21:45:25.954302"
    }
  ]
}
```

----------------------------------------

### account-findings

**Usage:** `sre account findings COMMAND [ARGS]...`

_Manages Account Findings state_

[`describe`](#account-findings-describe) Describes Findings state of a
derived Account entity.
[`delete`](#account-findings-delete) Clears Findings state of a 
derived Account entity.

----------------------------------------

### account-findings-describe

**Usage:** `sre account findings describe`

_Describes Findings state of a derived Account entity_

`-acc,--account` `TEXT` Account name to get Findings for. [Required]

`-trl,--target_rule` `TEXT` Rule ids to include in a Findings
state. [Optional]

`-trg,--target_region` `TEXT` Regions to include in a Findings 
state. [Optional]

`-trt,--target_region` `TEXT` Resource types to include in a Findings
state. [Optional]

`-ts,--target_severity` `TEXT` Severity values to include in a Findings
state. [Optional]

`-st,--subset_targets` `BOOLEAN` Applies dependent subset inclusion. 
[Optional]

`-exp,--expand` `TEXT` Expansion parameter to invert Findings collection on.
Default value: `resources`. [Required]

`-map,--mapped'` `TEXT` Applies mapping format of an expanded Findings 
collection, by a given key, rather than a listed one [Optional]

`-url,--get_url` `BOOLEAN` 'Returns a presigned URL rather than a raw 
Findings collection. [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account findings describe --account ACCOUNT_NAME --json

**Response**

```json
{
  "items": [
    {
      "rule_id": "Rule",
      "region": "Region",
      "resourceType": "Resource type",
      "description": "Description...",
      "severity": "Severity level"
    }
  ]
}
```

----------------------------------------

### account-findings-delete

**Usage:** `sre account findings delete`

_Clears Findings state of a derived Account entity._

`-acc,--account` `TEXT` Account name to remove Findings for. [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre account findings delete --account ACCOUNT_NAME --json

**Response**

```json
{
  "message": "Findings state bound to 'ACCOUNT_NAME' account has been cleared."
}
```

----------------------------------------

### job-submit

**Usage:** `sre job submit  COMMAND [ARGS]...`

_Manages Job submit action_

[`aws`](#job-submit-aws) Initiates Custodian Service Scan on AWS account

[`azure`](#job-submit-azure) Initiates Custodian Service Scan on AZURE account

[`gcp`](#job-submit-gcp) Initiates Custodian Service Scan on GCP account

----------------------------------------

### job-submit-aws

**Usage:** `sre job submit aws`

_Initiates Custodian Service Scan on AWS account_

`-acc,--account` `TEXT` Account name to initiate scan [Required]

`-trs,--target_ruleset` `TEXT` Rulesets to scan. If not specified, all
available rulesets will be used [Optional]

`-trg,--target_region` `TEXT` Regions to scan. If not specified, all active
regions will be used [Optional]

`-ak,--aws_access_key` `TEXT` AWS Account access key. Can be entered
interactively without displaying credentials [Required]

`-sk,--aws_secret_access_key` `TEXT` AWS Account secret access key. Can be
entered interactively without displaying credentials [Required]

`-df,--aws_default_region` `TEXT` AWS Account default region. Can be entered
interactively [Required]

`-st,--aws_session_token` `TEXT` AWS Account session token. Can be entered
interactively without displaying credentials [Optional]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre job submit aws --account ACCOUNT_NAME --target_ruleset FULL_AWS --target_region eu-central-1 --json

**Response**

```json
    {
  "items": [
    {
      "job_id": "11111111-1111-1111-1111-111111111111",
      "account_display_name": "ACCOUNT_NAME",
      "job_owner": "name",
      "submitted_at": "2021-11-08 13:00:00",
      "customer_display_name": "CUSTOMER_NAME"
    }
  ]
}
```

----------------------------------------

### job-submit-azure

**Usage:** `sre job submit azure`

_Initiates Custodian Service Scan on AZURE account_

`-acc,--account` `TEXT` Account name to initiate scan [Required]

`-trs,--target_ruleset` `TEXT` Rulesets to scan. If not specified, all
available rulesets will be used [Optional]

`-trg,--target_region` `TEXT` Regions to scan. If not specified, all active
regions will be used [Optional]

`-ati,--azure_tenant_id` `TEXT` Azure account tenant id. Can be entered
interactively without displaying credentials [Required]

`-asi,--azure_subscription_id` `TEXT` Azure account subscription id. Can be
entered interactively without displaying credentials [Required]

`-aci,--azure_client_id` `TEXT` Azure account client id. Can be entered
interactively without displaying credentials [Required]

`-acs,--azure_client_secret` `TEXT` Azure account client secret. Can be entered
interactively without displaying credentials [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre job submit azure --account ACCOUNT_NAME --target_ruleset FULL_AZURE --target_region eu-central-1 --json

**Response**

```json
    {
  "items": [
    {
      "job_id": "11111111-1111-1111-1111-111111111111",
      "account_display_name": "ACCOUNT_NAME",
      "job_owner": "name",
      "submitted_at": "2021-11-08 13:00:00",
      "customer_display_name": "CUSTOMER_NAME"
    }
  ]
}
```

----------------------------------------

### job-submit-gcp

**Usage:** `sre job submit gcp`

_Initiates Custodian Service Scan on GCP account_

`-acc,--account` `TEXT` Account name to initiate scan [Required]

`-trs,--target_ruleset` `TEXT` Rulesets to scan. If not specified, all
available rulesets will be used [Optional]

`-trg,--target_region` `TEXT` Regions to scan. If not specified, all active
regions will be used [Optional]

`-cf,--credentials_file` `PATH` Path to Google application credentials JSON
file [Required]

`--json` `TEXT` Response as a JSON. The default response view is a table
view [Optional]

**Request**

    sre job submit gcp --account ACCOUNT_NAME --target_ruleset FULL_GCP --credentials_file /path/to/the/file --json

**Response**

```json
    {
  "items": [
    {
      "job_id": "11111111-1111-1111-1111-111111111111",
      "account_display_name": "ACCOUNT_NAME",
      "job_owner": "name",
      "submitted_at": "2021-11-08 13:00:00",
      "customer_display_name": "CUSTOMER_NAME"
    }
  ]
}
```

----------------------------------------

### `siem-delete`

**Usage:** `sre siem delete [OPTIONS]`

_Deletes SIEM configuration_

`-type, --configuration_type` `[security_hub|dojo]` Configuration type  [Required]

`-cust, --customer` `TEXT` Customer name from which the configuration will be removed. Only for the SYSTEM customer

`--json` Response as a JSON [Optional]

`--help`  Show this message and exit [Optional]

---------------------------

### `siem-describe`

**Usage:** `sre siem describe [OPTIONS]`

_Describes a SIEM manager configuration for the given customer_

`-type, --configuration_type` `[security_hub|dojo]` Configuration type

`-cust, --customer` `TEXT` Customer name to which the configuration applies. Only for the SYSTEM customer

`--json` Response as a JSON [Optional]

`--help`  Show this message and exit [Optional]

---------------------------

### `siem-add`

**Usage:** `sre siem add [OPTIONS] COMMAND [ARGS]...`

_Manages SIEM configuration create action_

### Commands

[`dojo`](#siem-add-dojo) Adds dojo configuration

[`security_hub`](#siem-add-security_hub) Adds security hub configuration

----------------------------

### siem-add-dojo

Usage: sre siem add dojo[OPTIONS]

_Adds dojo configuration_

`-h, --host` `TEXT` DefectDojo host:port  [Required]

`-key, --api_key` `TEXT`  DefectDojo api key  [Required]

`-u, --user` `TEXT`       DefectDojo user name

`-ALL, --display_all_fields` `Flag`       Flag for displaying all fields

`-U, --upload_files` `Flag`           Flag for displaying a file for each resource with its full description in the "file" field

`-cust, --customer` `TEXT`     Customer name to which the configuration will be added. Only for the SYSTEM customer

`--json `               Response as a JSON

`--help`                Show this message and exit.

--------------------

### siem-add-security_hub

Usage: sre siem add security_hub[OPTIONS]

_Adds security hub configuration_

`-r, --region` `TEXT` AWS region name [Required]

`-p, --product_arn` `TEXT`  ARN of security product [Required]

`-tra, --trusted_role_arn` `TEXT`       Role that will be assumed to upload findings

`-cust, --customer` `TEXT`     Customer name to which the configuration will be added. Only for the SYSTEM customer

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### `siem-update`

**Usage:** `sre siem update [OPTIONS] COMMAND [ARGS]...`

_Manages SIEM configuration update action_

### Commands

[`dojo`](#siem-update-dojo) Updates dojo configuration

[`security_hub`](#siem-update-security_hub) Updates security hub configuration

----------------------------

### siem-update-dojo

Usage: sre siem update dojo[OPTIONS]

_Adds dojo configuration_

`-h, --host` `TEXT` DefectDojo host:port

`-key, --api_key` `TEXT`  DefectDojo api key

`-u, --user` `TEXT`       DefectDojo user name

`-ALL, --display_all_fields` `Flag`       Flag for displaying all fields

`-U, --upload_files` `Flag`           Flag for displaying a file for each resource with its full description in the "file" field

`-cust, --customer` `TEXT`     Customer name to which the configuration will be added. Only for the SYSTEM customer

`--json `               Response as a JSON

`--help`                Show this message and exit.

--------------------

### siem-update-security_hub

Usage: sre update add security_hub[OPTIONS]

_Adds security hub configuration_

`-r, --region` `TEXT` AWS region name

`-p, --product_arn` `TEXT`  ARN of security product

`-tra, --trusted_role_arn` `TEXT`       Role that will be assumed to upload findings

`-cust, --customer` `TEXT`     Customer name to which the configuration will be added. Only for the SYSTEM customer

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### lm-add

Usage: sre lm add[OPTIONS]

_Creates a Custodian Service License for the specified customer_

`-cust, --customer` `TEXT` Customer name to attach license to. Required for SYSTEM customer

`-lk, --license_key` `TEXT` License key to create [Required]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### lm-delete

Usage: sre lm delete [OPTIONS]

_Deletes license_

`-cust, --customer` `TEXT` Customer name to delete license from

`-lk, --license_key` `TEXT` License key to delete [Required]

`--json `               Response as a JSON

`--help`                Show this message and exit.

--------------------

### lm-describe

Usage: sre lm describe [OPTIONS]

_Describes a Custodian Service Licences for the given customer_

`-cust, --customer` `TEXT` Customer name to delete license from

`-lk, --license_key` `TEXT` License key to delete

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### lm-license-sync

Usage: sre lm license-sync [OPTIONS]

_Synchronises licenses_

`-lk, --license_key` `TEXT` [Required]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### `account-credentials-manager`

**Usage:** `sre account credentials-manager [OPTIONS] COMMAND [ARGS]...`

_Upload report to DefectDojo_

### Commands

[`add`](#account-credentials-manager-add) Creates Custodian Service Credentials Manager configuration

[`update`](#account-credentials-manager-update) Updates Custodian Service Credentials Manager configuration

[`delete`](#account-credentials-manager-delete) Terminates Custodian Service Credentials Manager configuration

[`describe`](#account-credentials-manager-describe) Describes Custodian Service Credentials Manager configuration

-------------------------------

### account-credentials-manager-add

Usage: sre account credentials-manager add[OPTIONS]

_Creates Custodian Service Credentials Manager configuration_

`-c, --cloud` `TEXT` The cloud to which the credentials configuration belongs [Required]

`-cid, --cloud_identifier` `TEXT` Cloud identifier to which the credentials configuration belongs [Required]

`-tra, --trusted_role_arn` `TEXT` Account role to assume [Optional]

`-e, --enabled` `BOOLEAN` Enable or disable credentials actuality [Optional]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### account-credentials-manager-update

Usage: sre account credentials-manager update[OPTIONS]

_Updates Credentials Manager configuration_

`-c, --cloud` `TEXT` The cloud to which the credentials configuration belongs [Required]

`-cid, --cloud_identifier` `TEXT` Cloud identifier to which the credentials configuration belongs [Required]

`-tra, --trusted_role_arn` `TEXT` Account role to assume [Optional]

`-e, --enabled` `BOOLEAN` Enable or disable credentials actuality [Optional]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### account-credentials-manager-describes

Usage: sre account credentials-manager describes[OPTIONS]

_Describes Credentials Manager configuration_

`-c, --cloud` `TEXT` The cloud to which the credentials configuration belongs [Optional]

`-cid, --cloud_identifier` `TEXT` Cloud identifier to which the credentials configuration belongs [Optional]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### account-credentials-manager-delete

Usage: sre account credentials-manager delete[OPTIONS]

_Deletes Credentials Manager configuration_

`-c, --cloud` `TEXT` The cloud to which the credentials configuration belongs [Required]

`-cid, --cloud_identifier` `TEXT` Cloud identifier to which the credentials configuration belongs [Required]

`--json `               Response as a JSON

`--help`                Show this message and exit.

-------------------------------

### `ruleset`

**Usage:** `sre ruleset [OPTIONS] COMMAND [ARGS]...`

_Manages Customer rulesets_

### Commands

[`add`](#ruleset-add) Creates Customers ruleset

[`delete`](#ruleset-delete) Deletes Customer ruleset

[`describe`](#ruleset-describe) Describes Customer rulesets

[`update`](#ruleset-update) Updates Customers ruleset

-------------------------------

### ruleset-add

Usage: sre ruleset add [OPTIONS]

_Creates Customers ruleset_

`-n,--name` `TEXT`         Ruleset name  [Required]

`-v,--version` `FLOAT`     Ruleset version. Default value: 1.0 [Optional]

`-c,--cloud` `TEXT`        The name of the cloud to which the rules in the ruleset belong. Possible values: AWS/AZURE/GCP [Required]

`-r,--rule` `TEXT`         Rule ids to attach to the ruleset [Optional]

`-act,--active` `FLAG`     Force set ruleset version as active [Optional]

`-st,--standard` `TEXT`    Filter rules by the security standard name. Available standards: PCI DSS, CIS Controls,
ISO, GDPR, HIPAA [Optional]

`-fc,--full_cloud` `FLAG`  Assemble all available rules for specific cloud provider [Optional]

`-ed, --event_driven` `FLAG`    Marks the ruleset as applicable for event-driven [Optional]

`-rv,--rule_version` `TEXT` Rule version to choose in case of duplication (the highest version by default).
Used with --full_cloud or --standard flags [Optional]

`-rta,--restrict_tenant_account` `TEXT` Tenant-Account relation inside which the ruleset is 
restricted. To declare accounts within a tenant, please use the following scheme: 
-rta $tenant:$account_1,$account2. [Optional]

`-cust,--customer` `TEXT`   Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                    Response as a JSON

`--help`                    Show this message and exit.
**Request**

    sre ruleset add -n test_ruleset -c GCP -st GDPR --json

**Response**

```json
{
    "items": [
        {
            "customer": "$CUSTOMER",
            "name": "test_ruleset",
            "version": "1.0",
            "cloud": "GCP",
            "rules_number": 10,
            "event_driven": false,
            "active": false
        }
    ]
}
```
-------------------------------

### ruleset-update

Usage: sre ruleset update [OPTIONS]

_Updates Customers ruleset_

`-n,--name` `TEXT`         Ruleset name  [Required]

`-v,--version` `FLOAT`     Ruleset version [Required]

`-c,--cloud` `TEXT`        The name of the cloud to which the rules in the ruleset belong. Possible values: AWS/AZURE/GCP [Required]

`-a,--rule_to_attach` `TEXT`    Rule ids to attach to the ruleset [Optional]

`-d,--rule_to_detach` `TEXT`    Rule ids to detach from the ruleset [Optional]

`-act,--active` `FLAG`     Force set ruleset version as active [Optional]

`-ed, --event_driven` `FLAG`    Marks the ruleset as applicable for event-driven [Optional]

`-rta,--restrict_tenant_account` `TEXT` Tenant-Account relation inside which the ruleset is 
restricted. To declare accounts within a tenant, please use the following scheme: 
-rta $tenant:$account_1,$account2. [Optional]

`-eta,--exclude_tenant_account` `TEXT` Tenant-Account relation to remove from the restricted 
relation. To declare accounts within a tenant, please use the following scheme:
 -eta $tenant:$account_1,$account2. If you wish to remove an account of the same 
 name withing multiple tenants, you may use the following 
notation: -eta :$account_1. [Optional]

`-cust,--customer` `TEXT`   Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                    Response as a JSON

`--help`                    Show this message and exit.

**Request**

    sre ruleset update -n test_ruleset -v 1.0 -c GCP -ed True --json

**Response**

```json
{
    "items": [
        {
            "customer": "$CUSTOMER",
            "name": "test_ruleset",
            "version": "1.0",
            "cloud": "GCP",
            "rules_number": 10,
            "status_code": "READY_TO_SCAN",
            "status_reason": "Assembled successfully",
            "event_driven": true,
            "active": false,
            "status_last_update_time": "2022-05-26T09:02:29.189994"
        }
    ]
}
```
-------------------------------

### ruleset-delete

Usage: sre ruleset delete [OPTIONS]

_Deletes Customer ruleset. For successful deletion, the ruleset must be inactive_

`-n,--name` `TEXT`         Ruleset name  [Required]

`-v,--version` `FLOAT`     Ruleset version [Required]

`-c,--cloud` `TEXT`        The name of the cloud to which the rules in the ruleset belong. Possible values: AWS/AZURE/GCP [Required]

`-cust,--customer` `TEXT`   Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                    Response as a JSON

`--help`                    Show this message and exit.


**Request**

    sre ruleset delete -n test_ruleset -v 1.0 -c GCP --json

**Response**

```json
{
    "message": "Ruleset 'test_ruleset' version 1.0 for GCP cloud has been deleted from customer CUSTOMER"
}
```
-------------------------------

### ruleset-describe

Usage: sre ruleset describe [OPTIONS]

_Describes Customer rulesets_

`-n,--name` `TEXT`         Ruleset name  [Optional]

`-v,--version` `FLOAT`     Ruleset version [Optional]

`-c,--cloud` `TEXT`        Cloud name to filter rulesets. Possible values: AWS/AZURE/GCP [Optional]

`-act,--active` `BOOL`     Filter only active rulesets[Optional]

`-cust,--customer` `TEXT`   Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                    Response as a JSON

`--help`                    Show this message and exit.

**Request**

    sre ruleset describe -act --json

**Response**

```json
{
    "items": [
        {
            "customer": "$CUSTOMER",
            "name": "FULL_AWS",
            "version": "1.0",
            "cloud": "AWS",
            "rules_number": 330,
            "status_code": "READY_TO_SCAN",
            "status_reason": "Assembled successfully",
            "event_driven": false,
            "active": true,
            "status_last_update_time": "2022-05-25T12:04:57.703215"
        },
        {
            ...
        }
    ]
}
```
-------------------------------

### `rule-source`

**Usage:** `sre rule-source [OPTIONS] COMMAND [ARGS]...`

_Manages Rule Source entity_

### Commands

[`add`](#rule-source-add) Creates rule source

[`delete`](#rule-source-delete) Deletes rule source

[`describe`](#rule-source-describe) Describes rule source

[`update`](#rule-source-update) Updates rule source

-------------------------------

### rulesource-add

Usage: sre rulesource add [OPTIONS]

_Creates rule source_

`-pid,--git_project_id` `TEXT`      GitLab Project id [Required]

`-url,--git_url` `TEXT`             Link to GitLab repository with sre rules [Required]

`-ref,--git_ref` `TEXT`             Name of the branch to grab rules from [Required]

`-prefix,--git_rules_prefix` `TEXT` Rules path prefix. Default: / [Optional]

`-type,--git_access_type` `TEXT`    Git access type. Now supports only TOKEN [Optional]

`-secret,--git_access_secret` `TEXT`  Secret token to be able to access the repository [Required]

`-cust,--customer` `TEXT`           Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`-rta,--restrict_tenant_account` `TEXT` Tenant-Account relation inside which the ruleset is 
restricted. To declare accounts within a tenant, please use the following scheme: 
-rta $tenant:$account_1,$account2. [Optional]

`--json`                            Response as a JSON

`--help`                            Show this message and exit.

**Request**

    sre rulesource add -prefix policies -pid 00000 -url https://gitlab.com/PATH -secret SECRET --json

**Response**

```json
{
    "items": [
        {
            "customer": "CUSTOMER",
            "git_url": "https://gitlab.com/PATH",
            "git_ref": "master",
            "git_rules_prefix": "policies"
        }
    ]
}
```
-------------------------------

### rulesource-update

Usage: sre rulesource update [OPTIONS]

_Updates rule source_

`-pid,--git_project_id` `TEXT`      GitLab Project id [Required]

`-url,--git_url` `TEXT`             Link to GitLab repository with sre rules [Optional]

`-ref,--git_ref` `TEXT`             Name of the branch to grab rules from [Optional]

`-prefix,--git_rules_prefix` `TEXT` Rules path prefix [Optional]

`-type,--git_access_type` `TEXT`    Git access type. Now supports only TOKEN [Optional]

`-secret,--git_access_secret` `TEXT`  Secret token to be able to access the repository [Optional]

`-cust,--customer` `TEXT`           Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`-rta,--restrict_tenant_account` `TEXT` Tenant-Account relation inside which the ruleset is 
restricted. To declare accounts within a tenant, please use the following scheme: 
-rta $tenant:$account_1,$account2. [Optional]

`-eta,--exclude_tenant_account` `TEXT` Tenant-Account relation to remove from the restricted 
relation. To declare accounts within a tenant, please use the following scheme:
 -eta $tenant:$account_1,$account2. If you wish to remove an account of the same 
 name withing multiple tenants, you may use the following 
notation: -eta :$account_1. [Optional]

`--json`                            Response as a JSON

`--help`                    Show this message and exit.

**Request**

    sre rulesource update -rsid 00000 -prefix new_prefix --json


**Response**

```json
{
    "items": [
        {
            "customer": "CUSTOMER",
            "git_url": "https://gitlab.com/PATH",
            "git_ref": "master",
            "git_rules_prefix": "new_prefix"
        }
    ]
}
```
-------------------------------

### rulesource-delete

Usage: sre rulesource delete [OPTIONS]

_Deletes rule source_

`-rsid,--rule_source_id` `TEXT`  Git project id to delete rule source [Required]

`-cust,--customer` `TEXT`       Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                        Response as a JSON

`--help`                        Show this message and exit.


**Request**

    sre rulesource delete -rsid 00000 --json

**Response**

```json
{
    "message": "Rule source with id '00000' has been removed from customer 'CUSTOMER'"
}
```
-------------------------------

### rulesource-describe

Usage: sre rulesource describe [OPTIONS]

_Describes rule source_

`-rsid,--rule_source_id` `TEXT` Git project id to describe rule source [Required]

`-cust,--customer` `TEXT`       Customer name which specifies whose entity to manage. Only for the SYSTEM customer [Optional]

`--json`                        Response as a JSON

`--help`                        Show this message and exit.

**Request**

   sre rulesource describe -rsid 00000 --json

**Response**

```json
{
    "items": [
        {
            "customer": "CUSTOMER",
            "git_url": "https://gitlab.com/PATH",
            "git_ref": "master",
            "git_rules_prefix": "policies"
        }
    ]
}
```
-------------------------------

### user-tenants-assign

Usage: sre user tenants assign [OPTIONS]

_Assign new tenants to user_

`-u,--username` `TEXT`              Username to update [Required]

`-t,--tenant` `TEXT`               Tenant names to assign to user [Required]

`--json`                            Response as a JSON

`--verbose`                         Save detailed information to the log file

`--help`                            Show this message and exit.

**Request**

    sre user tenants assign --username admin --tenant tenant_1 --tenant tenant_2 --json

**Response**

```json
{
    "tenants": "tenant_1,tenant_2"
}
```
-------------------------------

### user-tenants-describe

Usage: sre user tenants describe [OPTIONS]

_Describe the tenants that the user is allowed to access_

`-u,--username` `TEXT`              User whose tenants to describe [Required]

`--json`                            Response as a JSON

`--verbose`                         Save detailed information to the log file

`--help`                            Show this message and exit.

**Request**

    sre user tenants describe --username admin --json

**Response**

```json
    {
        "items": [{
            "tenants": "tenant_1,tenant_2"
        }]
    }
```
-------------------------------

### user-tenants-unassign

Usage: sre user tenants unassign [OPTIONS]

_Describe the tenants that the user is allowed to access_

`-u,--username` `TEXT`              Username to update [Required]

`-t,--tenant` `TEXT`                Tenant names to unassign from user [Required]

`-ALL,--all_tenants` `TEXT`         Remove all tenants from user. This will allow the user to interact with all
tenants within the customer

`--json`                            Response as a JSON

`--verbose`                         Save detailed information to the log file

`--help`                            Show this message and exit.

**Request**

    sre user tenants unassign --username admin -ALL --json

**Response**

```json
{
    "message": "Attribute tenants for user admin has been deleted."
}
```

---------------------------

### `setting-lm`

**Usage:** `sre setting lm [OPTIONS] COMMAND [ARGS]...`

_Manages License Manager Setting(s)_

### Commands

[`client`](#setting-lm-client) Manages License Manager Client data

[`config`](#setting-lm-config) Manages License Manager Config data

----------------------------

### `setting-lm-client`

**Usage:** `sre setting lm client[OPTIONS] COMMAND [ARGS]...`

_Manages License Manager Client data_

### Command groups

[`add`](#setting-lm-client-add) Adds License Manager provided client-key data

[`delete`](#setting-lm-client-delete) Removes current License Manager client-key data

[`describe`](#setting-lm-client-describe) Describe installed License Manager client-key data

----------------------------

### setting-lm-client-add

Usage: sre setting lm client add [OPTIONS]

_Adds License Manager provided client-key data_

`-kid, --key_id` `TEXT` Key-id granted by the License Manager [Required]

`-alg, --algorithm` `TEXT` Algorithm granted by the License Manager [Required]

`-prk, --private_key` `TEXT` Private-key granted by the License Manager

`-f, --format` `TEXT`   Format of the private-key

`-b64, --bs64encoded` `TEXT` Denotes whether the private is b64encoded

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-lm-client-delete

Usage: sre setting lm client delete [OPTIONS]

_Removes current License Manager client-key data_

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-lm-client-describe

Usage: sre setting lm client describe [OPTIONS]

_Describe installed License Manager client-key data_

`-f, --format` `TEXT`   Format of the public-key [Required]

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### `setting-lm-config`

**Usage:** `sre setting lm config[OPTIONS] COMMAND [ARGS]...`

_Manages License Manager Client data_

### Command groups

[`add`](#setting-lm-config-add) Adds License Manager access configuration data

[`delete`](#setting-lm-config-delete) Removes current License Manager access configuration data

[`describe`](#setting-lm-config-describe) Describes current License Manager access configuration data

----------------------------

### setting-lm-config-add

Usage: sre setting lm config add [OPTIONS]

_Adds License Manager access configuration data_

`-h, --host` `TEXT` License Manager host [Required]

`-p, --port` `INTEGER` License Manager port [Required]

`-v, --version` `FLOAT` License Manager version. [Required]

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-lm-client-delete

Usage: sre setting lm config delete [OPTIONS]

_Removes current License Manager access configuration data_

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-lm-client-delete

Usage: sre setting lm config describe [OPTIONS]

_Describes current License Manager access configuration data_

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### `setting-mail`

**Usage:** `sre setting mail [OPTIONS] COMMAND [ARGS]...`

_Manages Custodian Service Mail configuration_

### Commands

[`add`](#setting-mail-add) Creates Custodian Service Mail configuration

[`delete`](#setting-mail-delete) Deletes Custodian Service Mail configuration

[`describe`](#setting-mail-describe) Describes Custodian Service Mail configuration

-----------------------------

### setting-mail-add

Usage: sre setting mail add [OPTIONS]

_Creates Custodian Service Mail configuration_

`-u, --username` `TEXT` Username of mail account [Required]

`-pd, --password` `TEXT` Password of mail account [Required]

`-pl, --password_label` `TEXT` Label of a password under [Required]

`-h, --host` `TEXT` Host of a mail server [Required]

`-pt, --port` `INTEGER` Port of a mail server [Required]

`-tls, --use_tls` Denote to whether utilize TLS

`-s, --sender_name` `TEXT` Sender name to use for each email, defaults to given `username`

`-eps, --emails_per_session` `INTEGER` Amount of emails to send per session

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-mail-delete

Usage: sre setting mail delete [OPTIONS]

_Deletes Custodian Service Mail configuration_

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

### setting-mail-describe

Usage: sre setting mail describe [OPTIONS]

_Describes Custodian Service Mail configuration_

`-dp, --display_password` `FLAG` Denote to whether display a configured password

`--json`                Response as a JSON

`--verbose`             Save detailed information to the log file

`--help`                Show this message and exit.

-------------------------------

## Commands Formatting for Compatibility with M3MODULAR
### General Rules
* Command names should consist of 1 word without any `-` or `_`.
* Commands docstrings should be on a separate new line.
* Hide values of secured parameters using `secured_params` attribute in `@cli_response` decorator.
* `@group_name.command()` decorator must contain `name` attribute.

### Examples of Proper Command Formatting
| Do's                                                                         | Don'ts                                                   |
|------------------------------------------------------------------------------|----------------------------------------------------------|
| sre caas add                                                                 | sre linked_caas add                                      |
| @ruleset.command(cls=ViewCommand, name='describe')                           | @ruleset.command(cls=ViewCommand)                        |
| @cli_response(secured_params=['password'])<br>def login(..., password:str):  | @cli_response()<br>def login(..., password: str):        |
| """<br>Describes Custodian Service policies of a customer<br>"""             | """Describes Custodian Service policies of a customer""" |
