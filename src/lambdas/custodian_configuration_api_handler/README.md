## custodian-configuration-api-handler

This lambda is designed to handle the API for Customers, Tenants, Accounts,
Rulesets, Rule Sources and Account Regions configurations

### API reference

### Customer API: /customers:

- `GET`: Get customer (s) data
    * Get customer with a specific name:

      Request url: `/customers/display_name=EPAM Systems`

      Response body:
      ```json
      {
          "items": [
              {
                  "activation_date": "2021-08-23T09:13:58.520262",
                  "owner": "oleksandr_onsha@epam.com",
                  "display_name": "EPAM Systems"
              }
          ]
      }
      ```
    * Get all customers available:

      Request url: `/customers`

      Response body:
      ```json
      {
          "items": [
              {
                  "activation_date": "2021-08-23T10:08:27.775913",
                  "owner": "bohdan_onsha@epam.com",
                  "display_name": "TEST_CUSTOMER"
              },
              {
                  "activation_date": "2021-08-23T09:13:58.520262",
                  "owner": "oleksandr_onsha@epam.com",
                  "display_name": "EPAM Systems"
              }
          ]
      }
      ```  

- `POST`: Create new customer

  Request body:
  ```json5
  {
    "display_name": "TEST_CUSTOMER_3",
    "owner": "bohdan_onsha@epam.com",
    "contacts": { // Optional
        "primary": [
            "contact1", "contact2"
        ],
        "manager": [
            "contact3"
        ]
    }
  }
  ``` 
  Response body:
  ```json
  {
      "items": [
          {
              "display_name": "TEST_CUSTOMER_2",
              "activation_date": "2021-08-26T08:45:35.670257",
              "owner": "bohdan_onsha@epam.com"
          }
      ]
  }
  ```
- `PATCH` Update customer  
  Request body:

  ```json5
  {
    "display_name": "TEST_CUSTOMER_2",
    "owner": "new owner", // Optional
    "contacts": { // Optional
        "primary": [
            "contact1", "contact2", "contact3"
        ],
        "manager": [
            "contact3"
        ]
    }
  }
  ```  
  Response Body:

  ```json5
  {
      "items": [
          {
              "activation_date": "2021-08-26T08:48:17.929975",
              "owner": "new owner",
              "display_name": "TEST_CUSTOMER_2"
          }
      ]
  }
  ```
- `DELETE` Delete customer Request body:

  ```json5
  {
      "display_name": "TEST_CUSTOMER_2"
  }
  ```
  Response body:

    ```json5
    {
        "message": "customer with name 'TEST_CUSTOMER_2' has been deleted"
    }
    ```

### Tenant API /tenants

- `GET`: Get tenant(s) data
    * Get tenant with a specific name:

      Request url: `/tenants/display_name=TEST_TENANT`

      Response body:

        ```json5
        {
            "items": [
                {
                    "inherit": true,
                    "activation_date": "2021-08-23T09:15:37.241766",
                    "customer_display_name": "EPAM Systems",
                    "display_name": "TEST_TENANT"
                }
            ]
        }
        ```

    * Get all tenants:

      Request url: `/tenants/display_name=TEST_TENANT`

      Response body:

        ```json5
        {
            "items": [
                {
                    "inherit": true,
                    "activation_date": "2021-08-23T09:15:37.241766",
                    "customer_display_name": "EPAM Systems",
                    "display_name": "TEST_TENANT"
                },
                {
                    "inherit": true,
                    "activation_date": "2021-08-23T13:45:02.435954",
                    "customer_display_name": "EPAM Systems",
                    "display_name": "AWS-MSTR-DEV2"
                },
                {
                    "inherit": true,
                    "activation_date": "2021-08-23T13:46:41.676827",
                    "customer_display_name": "EPAM Systems",
                    "display_name": "AWS-MSTR-RES"
                }
            ]
        }
        ```

- `POST`: Create new tenant

  Request body:

    ```json5
    {
      "display_name": "TEST_TENANT_2",
      "customer": "EPAM Systems",
      "inherit": true
    }
    ```  

  Response body:

    ```json5
    {
        "items": [
            {
                "inherit": true,
                "display_name": "TEST_TENANT_2",
                "activation_date": "2021-08-26T09:07:24.986016",
                "customer_display_name": "EPAM Systems"
            }
        ]
    }
    ```

- `PATCH`: Update tenant

  Request body:

    ```json5
    {
        "display_name": "TEST_TENANT_2",
        "inherit": false
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "inherit": false,
                "activation_date": "2021-08-26T09:07:24.986016",
                "customer_display_name": "EPAM Systems",
                "display_name": "TEST_TENANT_2"
            }
        ]
    }
    ```

- `DELETE`: Delete tenant

  Request body:

    ```json5
    {
        "display_name": "TEST_TENANT_2"
    }
    ```

  Response body:

    ```json5
    {
        "message": "tenant with id 'TEST_TENANT_2' has been deleted"
    }
    ```

### Accounts API /accounts

- `GET`: Get account(s) data
    * Get accounts with a specific name:

      Request url: `/accounts/display_name=AWS-MSTR-DEV2`

      Response body:

        ```json5
        {
            "items": [
                {
                    "inherit": true,
                    "regions": [
                        "ap-northeast-1",
                        "eu-west-1",
                        "eu-central-1"
                    ],
                    "customer_display_name": "EPAM Systems",
                    "tenant_display_name": "AWS-MSTR-DEV2",
                    "display_name": "AWS-MSTR-DEV2",
                    "cloud": "aws",
                    "activation_date": "2021-08-23T13:45:03.875694"
                }
            ]
        }
        ```

    * Get all accounts:

      Request url: `/accounts`

      Response body:

        ```json5
        {
            "items": [
                {
                    "inherit": true,
                    "regions": [
                        "ap-northeast-1",
                        "eu-west-1",
                        "eu-central-1"
                    ],
                    "customer_display_name": "EPAM Systems",
                    "tenant_display_name": "AWS-MSTR-DEV2",
                    "cloud": "aws",
                    "display_name": "AWS-MSTR-DEV2",
                    "activation_date": "2021-08-23T13:45:03.875694"
                },
                {
                    "inherit": true,
                    "activation_date": "2021-08-23T13:46:42.456993",
                    "regions": [
                        "ap-northeast-1",
                        "eu-west-1"
                    ],
                    "customer_display_name": "EPAM Systems",
                    "tenant_display_name": "AWS-MSTR-RES",
                    "cloud": "aws",
                    "display_name": "AWS-MSTR-RES"
                }
            ]
        }
        ```      

- `POST` Create account

  Request body:

    ```json5
    {
      "display_name": "TEST_ACCOUNT",
      "tenant": "TEST_TENANT", // Existing Tenant name 
      "customer": "EPAM Systems", // Existing Customer name
      "inherit": true, // marks to inherit configuration from tenant
      "cloud": "aws" // Available options: aws/azure/gcp
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "inherit": true,
                "display_name": "TEST_ACCOUNT",
                "activation_date": "2021-08-26T09:14:22.208535",
                "cloud": "aws",
                "customer_display_name": "EPAM Systems",
                "tenant_display_name": "TEST_TENANT"
            }
        ]
    }
    ```  

- `PATCH` Update account Request body:

    ```json5
    {
        "display_name": "TEST_ACCOUNT", 
        "cloud": "gcp",
        "inherit": false
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "inherit": false,
                "activation_date": "2021-08-26T09:14:22.208535",
                "customer_display_name": "EPAM Systems",
                "tenant_display_name": "TEST_TENANT",
                "display_name": "TEST_ACCOUNT",
                "cloud": "gcp"
            }
        ]
    }
    ```

- `DELETE` Delete account Request body:

    ```json5
    {
        "display_name": "TEST_ACCOUNT"
    }
    ```

  Response body:

    ```json5
    {
        "message": "account with name 'TEST_ACCOUNT' has been deleted"
    }
    ```

### Rule Source API

Request paths:

* `rule-source`

- #### `GET` Get rule source

  Request query:
    ```
    display_name=EPAM Systems
    ```
  Response body:

    ```json5
    {
        "items": [
            {
                "git_ref": "master",
                "git_rules_prefix": "/",
                "git_access_type": "TOKEN",
                "git_url": "https://git.epam.com/epmc-sec/cloudlab/cloud_custodian/poc/custodian-epam-cloud",
                "git_project_id": "102030",
                "git_access_secret": "caas.3aa91ba4-4d10-4a04-b66f-a88e1f4f5335.2021.08.25.08.43.45.rules_repo_secret"
            }
        ]
    }
    ```

- #### `POST` Create rule source

  Request body:

    ```json5
    {
    ,   "git_access_secret": "{SECRET_VALUE}",
        "git_access_type" : "TOKEN", 
        "git_project_id": "102030",
        "git_ref": "master",
        "git_rules_prefix": "/",
        "git_url": "https://git.epam.com/epmc-sec/cloudlab/cloud_custodian/poc/custodian-epam-cloud"
    }
    ```

  Response body:

    ```json5
    {
      "items": [
        {
          "customer": "CUSTOMER",
          "git_ref": "master",
          "git_rules_prefix": "/",
          "git_url": "https://git.epam.com/epmc-sec/cloudlab/cloud_custodian/poc/custodian-epam-cloud",
        }
      ]
    }
    ```

- #### `PATCH` Update rule source

  Request body:

    ```json5
    {
      "git_access_secret": "{NEW_SECRET_VALUE}", //Optional
      "git_access_type": "TOKEN", //Optional
      "git_project_id": "102031", //Optional
      "git_ref": "develop", //Optional
      "git_rules_prefix": "/rules", //Optional
      "git_url": "https://git.epam.com/epmc-sec/cloudlab/cloud_custodian/poc/custodian-epam-cloud" //Optional
    }
    ```

  Response body:

    ```json5
    {
      "items": [
        {
          "customer": "CUSTOMER",
          "git_ref": "develop",
          "git_rules_prefix": "/rules",
          "git_url": "https://git.epam.com/epmc-sec/cloudlab/cloud_custodian/poc/custodian-epam-cloud"
        }
      ]
    }
    ```

- #### `DELETE` Delete rule source
  Request body:

    ```json5
    {
      "id": "00000"
    }
    ```

  Response body:

    ```json5
    {
        "message": "Rule source with id '00000' has been removed from customer 'CUSTOMER'"
    }
    ```

### Ruleset API

Request paths:

* `rulesets`
* `rulesets/content`

- #### `GET` Get Ruleset

    * Get all rulesets available
      Response Body:

        ```json5
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
                },
                {
                    ...
                }
            ]
        }
        ```

    * Get all rulesets with the given name  
      Request query:
        ```
        name=FULL_AWS
        ```      
      Response body:

        ```json5
        {
            "items": [
                {
                    "customer": "$CUSTOMER",
                    "name": "FULL_AWS",
                    "version": "2.0",
                    "cloud": "AWS",
                    "rules_number": 300,
                    "status_code": "READY_TO_SCAN",
                    "status_reason": "Assembled successfully",
                    "event_driven": false,
                    "active": true,
                    "status_last_update_time": "2022-05-25T12:04:57.703215"
                },
                {
                    "customer": "$CUSTOMER",
                    "name": "FULL_AWS",
                    "version": "1.0",
                    "cloud": "AWS",
                    "rules_number": 330,
                    "status_code": "READY_TO_SCAN",
                    "status_reason": "Assembled successfully",
                    "event_driven": false,
                    "active": false,
                    "status_last_update_time": "2022-04-25T11:04:53.000215"
                }
            ]
        }
        ```

    * Get specific version of ruleset:

      Request query
        ```
        name=FULL_AWS
        version=2
        ```    
      Response body:

        ```json5
        {
            "items": [
                {
                    "customer": "$CUSTOMER",
                    "name": "FULL_AWS",
                    "version": "2.0",
                    "cloud": "AWS",
                    "rules_number": 300,
                    "status_code": "READY_TO_SCAN",
                    "status_reason": "Assembled successfully",
                    "event_driven": false,
                    "active": true,
                    "status_last_update_time": "2022-05-25T12:04:57.703215"
                }
            ]
        }
        ```

    * Get only active rulesets:

      Request query
        ```
        active=true
        ```
      Response body:

        ```json5
        {
            "items": [
                {
                    "customer": "$CUSTOMER",
                    "name": "FULL_AWS",
                    "version": "2.0",
                    "cloud": "AWS",
                    "rules_number": 300,
                    "status_code": "READY_TO_SCAN",
                    "status_reason": "Assembled successfully",
                    "event_driven": false,
                    "active": true,
                    "status_last_update_time": "2022-05-25T12:04:57.703215"
                },
                {
                ...
                    "active": true,
                ...
                }
            ]
        }
        ```
   * Get rulesets with the given cloud:

      Request query
        ```
        cloud=AZURE
        ```
      Response body:

        ```json5
        {
            "items": [
                {
                    "customer": "$CUSTOMER",
                    "name": "FULL_AZURE",
                    "version": "3.0",
                    "cloud": "AZURE",
                    "rules_number": 100,
                    "status_code": "READY_TO_SCAN",
                    "status_reason": "Assembled successfully",
                    "event_driven": false,
                    "active": false,
                    "status_last_update_time": "2022-05-11T11:01:11.703215"
                }
            ]
        }
        ```

- #### `POST` Create ruleset

  Request body:

    ```json5
    {
        "name": "test_ruleset", // Required
        "version": 1.0, // Required
        "cloud": "AWS", // Required
        "rules": [ // Optional
            "epam-aws-088-http_load_balancer_certificate_expire_in_one_week_1.0",
            "epam-aws-090_use_secure_ciphers_in_cloudfront_distribution_1.0"
        ],
        "active": true, // Optional
        "event_driven": false, // Optional
        "standard": "HIPAA", // Optional, indicates to grab all rules for specific security standard
        "full_cloud": True // Optional, indicate to grab all available rules for specified CP
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "name": "test_ruleset",
                "version": "1.0",
                "active": true,
                "cloud": "aws",
                "rules_number": 2,
                "event_driven": false,
            }
        ]
    }
    ```

- #### `PATCH` Update ruleset

  Request body:

    ```json5
    {
        "name": "test_ruleset", // Required
        "version": 1.0, // Required
        "cloud": "AWS", // Required
        "active": false, // Optional
        "rules_to_attach":[ // Optional
            "epam-aws-094-ensure_mfa_is_enabled_for_the_root_account_1.0"
        ],
        "rules_to_detach": [ // Optional
            "epam-aws-088-http_load_balancer_certificate_expire_in_one_week_1.0"
        ]
    }
    ```
  Response body:

    ```json5
    {
      "items": [
        {
          "name": "test_ruleset",
          "active": false,
          "version": "1.0",
          "cloud": "aws",
          "rules_number": 2,
          "event_driven": false,
        }
      ]
    }
    ```

- #### `DELETE` Delete ruleset

  Request body:

    ```json5
    {
        "name": "test_ruleset", // Required
        "version": 1.0 // Required
    }
    ```

  Response body:

    ```json5
    {
        "message": "Ruleset with id 'test_ruleset_1.0' has been deleted from AWS cloud configuration of customer with display name EPAM Systems"
    }
  
    ```

- #### `GET` Get Rulesets' content

    Request path: `rulesets/content`

    Request body:

        ```json5
        {
            "name": "test_ruleset", // Required
            "version": 1.0 // Required
        }
        ```

    Response Body:

        ```json5
        {
            "message": "https://bucket-name.s3.amazonaws.com/PRESIGNED_URL"
        }
        ```

### Region API

Request path: `accounts/regions`

- #### `GET` Get account region(s)
    * Get all available account regions Request query:
        ```
        display_name=AWS-MSTR-DEV2
        ```
      Response body:

        ```json5
        {
            "items": [
                {
                    "name": "ap-northeast-1",
                    "activation_date": "2021-08-23T13:45:04.475283",
                    "state": "ACTIVE"
                },
                {
                    "name": "eu-west-1",
                    "activation_date": "2021-08-23T13:45:05.015372",
                    "state": "ACTIVE"
                },
                {
                    "name": "eu-central-1",
                    "activation_date": "2021-08-23T13:45:05.515418",
                    "state": "ACTIVE"
                }
            ]
        }
        ```

    * Get specific region Request query:
        ```json5
        display_name=AWS-MSTR-DEV2
        name=eu-west-1
        ```
  Response body:

    ```json5
    {
        "items": [
            {
                "name": "ap-northeast-1",
                "activation_date": "2021-08-23T13:45:04.475283",
                "state": "ACTIVE"
            },
            {
                "name": "eu-west-1",
                "activation_date": "2021-08-23T13:45:05.015372",
                "state": "ACTIVE"
            },
            {
                "name": "eu-central-1",
                "activation_date": "2021-08-23T13:45:05.515418",
                "state": "INACTIVE"
            }
        ]
    }
    ```

- #### `POST` Activate account region

  Request body:

    ```json5
    {
        "display_name": "AWS-MSTR-DEV2",
        "name": "eu-central-1",
        "state": "ACTIVE"
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "name": "eu-central-1",
                "state": "ACTIVE",
                "activation_date": "2021-08-26T14:06:37.791951"
            }
        ]
    }
    ```

- #### `PATCH` Update account region

  Request body:

    ```json5
    {
        "display_name": "AWS-MSTR-DEV2",
        "name": "eu-central-1",
        "state": "INACTIVE"
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "name": "eu-central-1",
                "activation_date": "2021-08-26T14:06:37.791951",
                "state": "INACTIVE"
            }
        ]
    }
    ```

- #### `DELETE` Deactivate account region

  Request body:

    ```json5
    {
        "display_name": "AWS-MSTR-DEV2",
        "name": "eu-central-1",
    }
    ```

  Response body:

    ```json5
    {
        "message": "Region with name 'eu-central-1' has been removed from account with display name 'AWS-MSTR-DEV2'"
    }
    ```

### Rules API /rules

- #### `GET` Describe rules
  Request query:
    ```
    customer=EPAM Systems // Customer name to describe rules
    limit=3 // Optional, max number of rules in the response
    offset=10 // Optional, offset results
    ```
  Response body:

    ```json5
    {
        "items": [
            {
                "version": "1.0",
                "customer": "EPAM Systems",
                "description": "Ensure that SSL/TLS certificates stored in AWS IAM are renewed month before expiry.\n",
                "id": "epam-aws-089-http_load_balancer_certificate_expire_in_one_month_1.0",
                "name": "epam-aws-089-http_load_balancer_certificate_expire_in_one_month",
                "cloud": "AWS",
                "updated_date": "2021-07-08T20:45:49.000+00:00"
            },
            {
                "version": "1.0",
                "customer": "EPAM Systems",
                "description": "Enforce the use of secure ciphers TLS v1.2 in a CloudFront Distribution certificate configuration\n",
                "id": "epam-aws-090_use_secure_ciphers_in_cloudfront_distribution_1.0",
                "name": "epam-aws-090_use_secure_ciphers_in_cloudfront_distribution",
                "cloud": "AWS",
                "updated_date": "2021-05-26T16:48:06.000+00:00"
            },
            {
                "version": "1.0",
                "customer": "EPAM Systems",
                "description": "Remove Weak Ciphers for Load Balancer\n",
                "id": "epam-aws-092-remove_weak_ciphers_for_load_balancer_1.0",
                "name": "epam-aws-092-remove_weak_ciphers_for_load_balancer",
                "cloud": "AWS",
                "updated_date": "2021-06-02T14:49:26.000+00:00"
            }
        ]
    }
    ```

- #### `DELETE` Delete rules
  Request body:

    ```
    {
      "customer": "EPAM Systems",
      "rule_id": "epam-aws-080-bucket_policy_allows_https_requests_1.0"
    }
    ```

  If `rule_id` not specified, all customer rules will be deleted.

  If `customer` and `rule_id` not specified, all available rules will be
  deleted. Only for system admin user.

  Response body:

    ```
    {
      "message": "Rule with id \'epam-aws-080-bucket_policy_allows_https_requests_1.0\' has been deleted"
    }
    ```

### Policies API /policies

- #### `GET` Get policy (ies)
  Request query:

    ```buildoutcfg
    customer=EPAM Systems
    name=admin_policy // Optional
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "customer": "EPAM Systems",
                "name": "admin_policy",
                "permissions": [
                    "system:update_meta",
                    "system:create_backup",
                    "system:update_metrics",
                    "iam:describe_policy",
                    "iam:create_policy",
                    "iam:update_policy",
                    "iam:remove_policy",
                    "iam:describe_role",
                    "iam:create_role",
                    "iam:update_role",
                    "iam:remove_role",
                    "iam:remove_policy_cache",
                    "iam:remove_role_cache",
                    "rule:describe_rule",
                    "rule:create_rule",
                    "rule:update_rule",
                    "rule:remove_rule",
                    "run:initiate_run",
                    "run:terminate_run",
                    "run:get_report",
                    "run:describe_report",
                    "run:describe_job",
                    "user:describe_role",
                    "user:assign_role",
                    "user:update_role",
                    "user:unassign_role",
                    "user:describe_customer",
                    "user:assign_customer",
                    "user:update_customer",
                    "user:unassign_customer",
                    "account:describe_account",
                    "account:create_account",
                    "account:update_account",
                    "account:remove_account",
                    "account:describe_region",
                    "account:create_region",
                    "account:update_region",
                    "account:remove_region",
                    "tenant:describe_tenant",
                    "tenant:create_tenant",
                    "tenant:update_tenant",
                    "tenant:remove_tenant",
                    "customer:describe_customer",
                    "ruleset:describe_ruleset",
                    "ruleset:create_ruleset",
                    "ruleset:update_ruleset",
                    "ruleset:remove_ruleset",
                    "rule_source:describe_rule_source",
                    "rule_source:create_rule_source",
                    "rule_source:update_rule_source",
                    "rule_source:remove_rule_source"
                ]
            },
            {
                "customer": "EPAM Systems",
                "name": "policy_name",
                "permissions": [
                    "customer:update_customer",
                    "account:describe_account",
                    "account:create_account",
                    "account:update_account",
                    "account:remove_account",
                    "account:describe_rule_source",
                    "account:create_rule_source",
                    "account:update_rule_source",
                    "account:remove_rule_source"
                ]
            }
        ]
    }
    ```

- #### `POST` Create policy

  Request body:

    ```json5
    {
        "name": "test_policy",
        "customer": "EPAM Systems",
        "permissions": ["customer:describe_customer"]
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "customer": "EPAM Systems",
                "name": "test_policy",
                "permissions": [
                    "customer:describe_customer"
                ]
            }
        ]
    }
    ```

- #### `PATCH` Update policy

  Request body:

    ```json5
    {
        "customer": "EPAM Systems",
        "name": "test_policy",
        "permissions_to_attach": [
            "account:describe_account"
        ],
        "permissions_to_detach": [
            "customer:describe_customer"
        ]
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "customer": "EPAM Systems",
                "name": "test_policy",
                "permissions": [
                    "account:describe_account"
                ]
            }
        ]
    }
    ```

- #### `DELETE` Delete policy

    ```json5
    {
      "customer": "EPAM Systems",
      "name": "test_policy"
    }
    ```

    ```json5
    {
      "message": "policy with name 'test_policy' from customer 'EPAM Systems' has been deleted"
    }
    ```

### Role API /roles

- #### `GET` Get role (s)

Request query:

```buildoutcfg
customer=EPAM Systems
name=admin_role // Optional
```

Response body:

```json5

{
  "items": [
    {
      "expiration": "2021-11-21T09:14:42.938267",
      "customer": "EPAM Systems",
      "policies": [
        "admin_policy"
      ],
      "name": "admin_role"
    }
  ]
}
```

- #### `POST` Create role
  Request body:

    ```json5
    {
        "customer": "EPAM Systems",
        "policies": [
            "admin_policy",
            "test_policy"
        ],
        "name": "test_role"
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "customer": "EPAM Systems",
                "name": "test_role",
                "policies": [
                    "admin_policy",
                    "test_policy"
                ],
                "expiration": "2021-11-24T14:29:47.862558"
            }
        ]
    }
    ```

- #### `PATCH` Update role

  Request body:

    ```json5
    {
        "customer": "EPAM Systems",
        "name": "test_role",
        "policies_to_attach": [
            "test_policy2"
        ],
        "policies_to_detach": [
            "test_policy"
        ]
    }
    ```

  Response body:

    ```json5
    {
        "items": [
            {
                "expiration": "2021-11-24T14:29:47.862558",
                "customer": "EPAM Systems",
                "policies": [
                    "test_policy2",
                    "admin_policy"
                ],
                "name": "test_role"
            }
        ]
    }
    ```

- #### `DELETE` Delete role

  Request body:

   ```json5
   {
       "customer": "EPAM Systems",
       "name": "test_role"  
   }
   ```

  Response body:
   ```json5
   {
       "message": "role with name 'test_role' from customer 'EPAM Systems' has been deleted"
   }
   ```
### Credential Manager API /accounts/credential_manager

- #### `GET` Get Credentials Manager configuration (s)

Request query:

```buildoutcfg
cloud=AWS|GCP|AZURE //Optional
cloud_identifier=ID // Optional
```

Response body:

```json5

{
    "items": [
        {
            "cloud_identifier": "111111111111",
            "cloud": "aws",
            "enabled": true,
            "trusted_role_arn": "arn:aws:iam::111111111111:role/Test"
        },
        ...
        {
            "cloud_identifier": "222222222222",
            "cloud": "aws",
            "enabled": false,
            "trusted_role_arn": "arn:aws:iam::222222222222:role/Role"
        }
    ]
}
```
- #### `POST` Create Credentials Manager configuration

  Request body:

   ```json5
   {
       "cloud": "AWS"|"GCP"|"AZURE",
       "cloud_identifier": "ID",
       "trusted_role_arn": "ARN", // Optional
       "enabled": true|false //Optional
   }
   ```
- #### `Patch` Update Credentials Manager configuration

  Request body:

   ```json5
   {
       "cloud": "AWS"|"GCP"|"AZURE",
       "cloud_identifier": "ID",
       "trusted_role_arn": "ARN", // Optional
       "enabled": true|false //Optional
   }
   ```

- #### `DELETE` Delete Credentials Manager configuration

  Request body:

   ```json5
   {
       "cloud": "AWS"|"GCP"|"AZURE",
       "cloud_identifier": "ID",
   }
   ```

  Response body:
   ```json5
    {
        "message": "credentials-manager with next fields: AWS, ID has been deleted"
    }
   ```
### Licenses API /license

- #### `GET` Get License(s)

Request query:

```buildoutcfg
customers=[customers] //Optional
license_key=KEY // Optional
```

- #### `POST` Create new License

  Request body:

   ```json5
   {
       "customer": "AWS"|"GCP"|"AZURE", //required
       "license_key": "ID" //required
   }
   ```

- #### `DELETE` Delete License

  Request body:

   ```json5
   {
       "license_key": "KEY", //required
       "customer": "CUSTOMER"
   }
   ```
### Licenses API /license/sync

- #### `POST` Sync Licenses with License Manager

  Request body:

   ```json5
   {
       "license_key": "KEY" //required
   }
   ```
### User Tenants API /users/tenants

- #### `POST` Assign tenants to user (Only within these tenants user can perform actions)

  Request body:

   ```json5
   {
       "target_user": "username", //required
       "tenants": ["tenant1", "tenant2"] //required
   }
   ```

- #### `GET` Describe the tenants that the user is allowed to access

  Request body:

   ```json5
   {
       "target_user": "username" //required
   }
   ```

- #### `DELETE` Unassign tenants from user

  Request body:

   ```json5
   {
       "target_user": "username", //required
       "tenants": ["tenant1", "tenant2"], //required if "all" not specified
       "all": true|false
   }
   ```

This lambda uses the following resource:

#### DynamoDB

* CaaSRules - the table used to store rules data;
* Customers, Tenants, CaaSRulesets, CaaSRuleSources - tables with customer, tenant, account,
 ruleset and rule source configurations
* CaaSRoles, CaaSPolicies - the tables used by service to get and store data about roles and policies;
* CaaSJobs - the table used by service to store data about scans;
* CaaSSettings - the table used by service to get and store settings data;
* CaaSUsers - the table used by service to store user data;

#### Systems Manager Parameter Store

Lambda can create and remove SSM parameters with `git_access_secret` token for
rule sources

#### Env Variables:

* `caas_user_pool_name` - Cognito user pool name for custodian users;


#### Should have next permission actions:
- Allow: batch:SubmitJob
- Allow: batch:TerminateJob
- Allow: lambda:InvokeFunction
- Allow: sts:AssumeRole
- Allow: ssm:PutParameter
- Allow: ssm:DeleteParameter
- Allow: ssm:GetParameter
- Allow: xray:PutTraceSegments
- Allow: xray:PutTelemetryRecords
- Allow: logs:CreateLogGroup
- Allow: logs:CreateLogStream
- Allow: logs:PutLogEvents
- Allow: cognito-idp:AdminDeleteUserAttributes
- Allow: cognito-idp:ListUsers
- Allow: cognito-idp:AdminRespondToAuthChallenge
- Allow: cognito-idp:SignUp
- Allow: cognito-idp:ListUserPoolClients
- Allow: cognito-idp:ListUserPools
- Allow: cognito-idp:AdminCreateUser
- Allow: cognito-idp:AdminUpdateUserAttributes
- Allow: cognito-idp:AdminInitiateAuth
- Allow: cognito-idp:AdminSetUserPassword
- Allow: dynamodb:GetItem
- Allow: dynamodb:*
- Allow: s3:Get*
- Allow: s3:List*