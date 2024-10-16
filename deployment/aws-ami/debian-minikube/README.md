

## Creating AMI ami

**Start EC2 instance and log in**

Instance must have `Debian 12` OS and `ARM` architecture, though `x64` should work as well as long as 
corresponding docker images are built. Select at least 16Gb for storage (depends on your further workload). Other settings
don't matter here


**Create entrypoint script**

Create `sre-run.sh` (name does not matter) script by some path, say `/usr/local/bin/` and put the content of [sre/deployment/aws-ami/debian-minikube/sre-run.sh](./sre-run.sh) there:

```bash
cat <<EOF | sudo tee /usr/local/bin/sre-run.sh > /dev/null
#!/bin/bash

...

EOF
```

**Allow to execute that script for root:**

```bash
sudo chmod 110 /usr/local/bin/sre-run.sh
```

**Create systemd service**

The service will execute the script above when the instance is starting

```bash
cat <<EOF | sudo tee /etc/systemd/system/sre-run.service > /dev/null
[Unit]
Description=sre-run script running at bootstrap
 
[Service]
ExecStart=/usr/local/bin/sre-run.sh
 
[Install]
WantedBy=multi-user.target
EOF
```

**Enable the created service:**

```bash
sudo systemctl enable sre-run.service
```

**Remove your key from authorized keys:**

```bash
> ~/.ssh/authorized_keys
```

**Create AMI from the instance**

Log out and create AWS AMI image from the instance. Terminate the instance when AMI becomes `Available`.


## AMI Envs


### Initialization

The listed envs can be specified via ec2 user data and will impact the way ami is initialized. Make sure to export envs 
inside user data:

**Right:**
```bash
export DO_NOT_ACTIVATE_LICENSE=y
export TENANT_NAME=EXAMPLE
```

**Wrong:**
```bash
DO_NOT_ACTIVATE_LICENSE=y
TENANT_NAME=EXAMPLE
```

**Installation preferences**
- `LOG_PATH` - path to file where to write logs (default `/var/log/sre-init.log`)
- `ERROR_LOG_PATH` - path to file where to write errors logs (default `/var/log/sre-init.log`)
- `SYNDICATE_HELM_REPOSITORY` - (default `s3://charts-repository/syndicate/`)
- `DOCKER_VERSION` - (default `5:27.1.1-1~debian.12~bookworm`)
- `MINIKUBE_VERSION` - (default `v1.33.1`)
- `KUBERNETES_VERSION` - (default `v1.30.0`)
- `KUBECTL_VERSION` - (default `v1.30.3`)
- `HELM_VERSION` - (default `3.15.3-1`)
- `MODULAR_SDK_ROLE_ARN` - can be specified to configure modular-sdk to use tables from DynamoDB
- `MODULAR_SDK_REGION` - region where DynamoDB tables are located

**Configuration preferences**
- `LM_API_LINK` - link to license manager (default `https://lm.syndicate.team`)
- `MODULAR_SERVICE_USERNAME` - username for admin user that will be created for modular-service (default `admin`)
- `RULE_ENGINE_USERNAME` - username for admin user that will be created for rule-engine (default `admin`)
- `CUSTOMER_NAME` - customer name to activate in case `DO_NOT_ACTIVATE_LICENSE` is set. Otherwise, `CUSTOMER_NAME` will be inherited from the license. If `DO_NOT_ACTIVATE_LICENSE` is set and `CUSTOMER_NAME` is not set then the name will be `CUSTOMER_1`
- `TENANT_NAME` - tenant name for the one created automatically. Default value is the AWS Account Alias if it is set. If AWS Account Alias is not set the name will be `TENANT_1`
- `TENANT_AWS_REGIONS` - aws regions to activate for the automatically created tenant (default `<all regions>`). Specify regions split by space. Example: `eu-central-1 eu-west-1`
- `ADMIN_EMAILS` - customer admin emails split by space (default ``). Example: `admin1@yourcorp.com admin2@yourcorp.com`
- `TENANT_PRIMARY_CONTACTS` - tenant primary emails split by space (default ``). Example: `primary_contact1@yourcorp.com primary_contact2@yourcorp.com`
- `TENANT_SECONDARY_CONTACTS` - tenant secondary emails split by space (default ``). Example: `secondry_contact1@yourcorp.com secondary_contact2@yourcorp.com`
- `TENANT_MANAGER_CONTACTS` - tenant manager contacts split by space (default ``). Example: `tenant_manager1@yourcorp.com tenant_manager2@yourcorp.com`
- `TENANT_OWNER_EMAIL` - one tenant owner email (default ``). Example: `tenant_owner@yourcorp.com`
- `DO_NOT_ACTIVATE_LICENSE` - specify any value to skip license activation step
- `DO_NOT_ACTIVATE_TENANT` - specify any value to disable tenant creation

**Developer preferences**

These are environment variables that affect the installation and configuration process and can break some AMI features. 
They are intended for development purposes so do not touch them.

- `SRE_LOCAL_PATH` - directory where to collect all artifacts (default `/usr/local/sre`)
- `FIRST_USER` - linux username to install rule-engine for. Must have sudo without password (default `admin`, more precisely user with id 1000)
- `HELM_RELEASE_NAME` - name of helm release for Rule Engine application (default `rule-engine`)
- `DEFECTDOJO_HELM_RELEASE_NAME` - name of helm release for DefectDojo application (default `defectdojo`)
- `GITHUB_REPO` - Rule Engine GitHub repository (default `epam/syndicate-rule-engine`)
