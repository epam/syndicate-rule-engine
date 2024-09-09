## Starting compose locally

Move to the folder containing `compose.yaml`:

```bash
cd deployment/aws-ami/debian-compose/artifacts  # or specify your folder
```

Optionally generate random envs:

```bash
python generate_random_envs.py --rule-engine >> .env  # optionally
```

Start docker compose. Do not specify `--env-file` parameter in case you've skipped the previous step

```bash
docker compose --profile modular-service --profile rule-engine --profile modular-api --env-file .env up -d
```

If all profiles are included those API will be available:
- vault: [http://127.0.0.1:8200](http://127.0.0.1:8200)
- mongo: [http://127.0.0.1:27017](http://127.0.0.1:27017)
- minio console: [http://127.0.0.1:9001](http://127.0.0.1:9001)
- rule-engine: [http://127.0.0.1:8000/api/doc](http://127.0.0.1:8000/api/doc)
- modular-service: [http://127.0.0.1:8040/api/doc](http://127.0.0.1:8040/api/doc)
- modular-api: [http://127.0.0.1:8085](http://127.0.0.1:8085)

In case you didn't generate random envs the default ones you can peek inside `compose.yaml`. Only for development

## Starting defect dojo

Both defect dojo and rule engine must be started in the save docker network. So, be aware that `dojo-compose.yaml` 
expected an external network called `rule-engine` to exist

Move to the folder containing `dojo-compose.yaml`

```bash
cd deployment/aws-ami/debian-compose/artifacts  # or specify your folder
```

Optionally generate random envs for dojo:

```bash
python generate_random_envs.py --dojo >> .env
```

Start dojo docker compose.

```bash
docker compose --file dojo-compose.yaml --env-file .env up -d
```

Wait for a while because dojo takes some time to initialize. Dojo will print its system password to stdout of 
initializer container logs. You can fetch the password using this command:

```bash
docker compose -f dojo-compose.yaml logs initializer | grep -oP "Admin password: \K\w+"
```

**Note:** if the command outputs nothing it probably means that dojo is still initializing and you should wait


Defect dojo ui should become available on [http://127.0.0.1:8080](http://127.0.0.1:8080)


## Quickstart


There are two APIs: rule engine API and modular-service api. The first allows to manage rules, rulesets, jobs and reports.
The second one allows to manage admin entities. You can use provided CLIs to interact with entities.

### Log in as system users

```bash
sre --version
modular-service --version
```

Use bash alias for convenience:
```bash
alias ms="modular-service"
```

Configure api links:

```bash
sre configure --api_link http://0.0.0.0:8000/caas
```

```bash
ms configure --api_link http://0.0.0.0:8040/dev
```

Login as system users:
```bash
sre login --username system_user --password "$CAAS_SYSTEM_USER_PASSWORD"
```

```bash
ms login --username system_user --password "$MODULAR_SERVICE_SYSTEM_USER_PASSWORD"
```


### Activate license, part 1
To activate license such information must be provided to you:

- license activation key in `uuid4` format - `$LICENSE_ACTIVATION_KEY`
- License Manager API link - `$LM_API_LINK`
- License Manager private key id - `$LM_PRIVATE_KEY_ID`
- base64-encoded License Manager private key - `$LM_PRIVATE_KEY`
- Customer Name `$CUSTOMER_NAME`


Configure LM api link
```bash
sre setting lm config add --host "$LM_API_LINK" --json
```

Configure LM private key
```bash
sre setting lm client add --key_id "$LM_PRIVATE_KEY_ID" --private_key "$LM_PRIVATE_KEY" --b64encoded --json
```

Other license configuration steps will be performed from a customer user.

### Log in as customer users

Register new user for modular-service
```bash
export MODULAR_SERVICE_USERNAME=admin
export MODULAR_SERVICE_PASSWORD="$(python generate_random_password.py)"
ms signup --username $MODULAR_SERVICE_USERNAME --password $MODULAR_SERVICE_PASSWORD --customer_name $CUSTOMER_NAME --customer_display_name "$CUSTOMER_NAME" --customer_admin admin@example.com --json
```

From system user create role and policy and admin user for rule engine:
```bash
export RULE_ENGINE_USERNAME=admin
export RULE_ENGINE_PASSWORD="$(python generate_random_envs.py)"
sre policy add --name admin_policy --permissions_admin --effect allow --tenant '*' --description "Full admin access policy for customer" --customer_id "$CUSTOMER_NAME" --json
sre role add --name admin_role --policies admin_policy --description "Admin customer role" --customer_id "$CUSTOMER_NAME" --json
sre users create --username "$RULE_ENGINE_USERNAME" --password "$RULE_ENGINE_PASSWORD" --role_name admin_role --customer_id "$CUSTOMER_NAME" --json
```

Login as admin users into modular service and rule engine
```bash
sre login --username "$RULE_ENGINE_USERNAME" --password "$RULE_ENGINE_PASSWORD"
```

```bash
ms login --username "$MODULAR_SERVICE_USERNAME" --password "$MODULAR_SERVICE_PASSWORD"
```

### Activate license, part 2

Add license
```bash
sre license add --tenant_license_key "$LICENSE_ACTIVATION_KEY" --json
```

Make sure license and new rulesets exist:

```bash
sre license describe
```

```bash
sre ruleset describe
```


### Activate tenant

Activate a tenant that will represent your cloud account:

```bash
export TENANT_NAME='AWS-DEV-ACCOUNT'
export ACCOUNT_ID='123456789012'
ms tenant create --name "$TENANT_NAME" --display_name "Tenant $ACCOUNT_ID" --cloud AWS --account_id $ACCOUNT_ID --primary_contacts admin@example.com --secondary_contacts admin@example.com --tenant_manager_contacts admin@example.com --default_owner admin@example.com --json
```

Activate tenant regions
```bash
ms tenant regions activate -tn $TENANT_NAME -rn eu-west-1  # or some other region
```


### Link defect dojo
Link defect dojo installation if you have one. First get dojo api token using the command or via UI:

```bash
export DOJO_TOKEN=$(curl -X POST -H 'content-type: application/json' "http://127.0.0.1:8080/api/v2/api-token-auth/" -d "{\"username\":\"admin\",\"password\":\"$DOJO_SYSTEM_PASSWORD\"}" | jq -r ".token")
```

Add dojo installation:

```bash
sre integrations dojo add --url http://nginx:8080/api/v2 --api_key $DOJO_TOKEN --description "Main dojo installation"
```
**Note:** `nginx` is used as host because the services are inside docker compose

Activate dojo installation:

```bash
sre integrations dojo activate -id "here put id of integration from the previous command" --all_tenants --send_after_job
```


### Adding your own rulesets (optional)

Add some rule-source

```bash
sre rulesource add -gpid epam/ecc-aws-rulepack -t GITHUB_RELEASE -d "Main open source aws rule source" -gprefix policies/
```

Pull rules

```bash
sre rulesource sync -rsid "ID from the previous command"
```

Wait some time till the rule are pulled. You can use `sre rulesource describe` to see the syncing status
Create ruleset with pulled rules

```bash
sre ruleset add --name FULL_AWS --cloud AWS
```

### Submitting jobs
Execute AWS scan with the created ruleset. Provide cloud credentials to the corresponding command or ec2 instance provide credentials will be used by default


```bash
sre job submit --tenant_name $TENANT_NAME
```

You can see job's status by describing it:

```bash
sre job describe -id "id from previous command"
```

Also, visit DefectDojo UI to see findings when the job is finished. For questions or further usage contact support