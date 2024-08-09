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


```bash
c7n --version
modular-service --version
```

Use bash alias for convenience:
```bash
alias ms="modular-service"
```

Configure api links:

```bash
c7n configure --api_link http://0.0.0.0:8000/caas
```

```bash
ms configure --api_link http://0.0.0.0:8040/dev
```

Login as system users:
```bash
c7n login --username system_user --password "$CAAS_SYSTEM_USER_PASSWORD"
```

```bash
ms login --username system_user --password "$MODULAR_SERVICE_SYSTEM_USER_PASSWORD"
```

Register new user for modular-service
```bash
export MODULAR_SERVICE_USERNAME=admin
export MODULAR_SERVICE_PASSWORD="$(python generate_random_password.py)"
export CUSTOMER_NAME="EPAM Systems"
ms signup --username $MODULAR_SERVICE_USERNAME --password $MODULAR_SERVICE_PASSWORD --customer_name $CUSTOMER_NAME --customer_display_name "$CUSTOMER_NAME" --customer_admin admin@example.com --json
```


From system user create role and policy and admin user for rule engine:
```bash
export RULE_ENGINE_USERNAME=admin
export RULE_ENGINE_PASSWORD="$(python generate_random_envs.py)"
c7n policy add --name admin_policy --permissions_admin --effect allow --tenant '*' --description "Full admin access policy for customer" --customer_id "$CUSTOMER_NAME" --json
c7n role add --name admin_role --policies admin_policy --description "Admin customer role" --customer_id "$CUSTOMER_NAME" --json
c7n users create --username "$RULE_ENGINE_USERNAME" --password "$RULE_ENGINE_PASSWORD" --role_name admin_role --customer_id "$CUSTOMER_NAME" --json
```

Login as admin users into modular service and rule engine
```bash
c7n login --username "$RULE_ENGINE_USERNAME" --password "$RULE_ENGINE_PASSWORD"
```

```bash
ms login --username "$MODULAR_SERVICE_USERNAME" --password "$MODULAR_SERVICE_PASSWORD"
```



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


Link defect dojo installation if you have one. First get dojo api token using the command or via UI:

```bash
export DOJO_TOKEN=$(curl -X POST -H 'content-type: application/json' "http://127.0.0.1:8080/api/v2/api-token-auth/" -d "{\"username\":\"admin\",\"password\":\"$DOJO_SYSTEM_PASSWORD\"}" | jq -r ".token")
```

Add dojo installation:

```bash
c7n integrations dojo add --url http://nginx:8080/api/v2 --api_key $DOJO_TOKEN --description "Main dojo installation"
```
**Note:** `nginx` is used as host because the services are inside docker compose

Activate dojo installation:

```bash
c7n integrations dojo activate -id "here put id of integration from the previous command" --all_tenants --send_after_job
```


Add some rule-source

```bash
c7n rulesource add -gpid epam/ecc-aws-rulepack -t GITHUB_RELEASE -d "Main open source aws rule source" -gprefix policies/
```

Pull rules

```bash
c7n rulesource sync -rsid "ID from the previous command"
```

Wait some time till the rule are pulled. You can use `c7n rulesource describe` to see the syncing status
Create ruleset with pulled rules

```bash
c7n ruleset add --name FULL_AWS --cloud AWS
```

Execute AWS scan with the created ruleset. Provide cloud credentials to the corresponding command or ec2 istance provide credentials will be used by default


```bash
c7n job submit --tenant_name $TENANT_NAME --ruleset FULL_AWS
```

You can see job's status by describing it:

```bash
c7n job describe -id "id from previous command"
```

Also, visit DefectDojo UI to see findings when the job is finished. For questions or further usage contact support