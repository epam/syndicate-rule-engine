# Custodian as a service smoke tests

These smoke tests should be executed against a configured env. They check the 
general condition of the environment.

**Note:** all the smoke tests use `c7ncli` to execute commands. So, the CLI 
must be installed in the local execution environment.


## Main flow

This smoke tests case does not create any new entities, except jobs. It checks
most describe actions and starts jobs for specified tenants:

```bash
python tests/smoke/main_flow.py --username $CAAS_USERNAME --password $CAAS_PASSWORD --api_link $CAAS_API_LINK --tenants TEST_TENANT:eu-west-1,eu-west-2 TEST_TENANT2
```

Such a command will check all the describe actions and will execute two jobs:
one to scan `TEST_TENANT` in `eu-west1` & `eu-west-2` regions, another to 
scan `TEST_TENANT2` in all the regions


## Rules management flow

This smoke tests case checks rules & rulesets -bound actions. It requires 
at least one rulesource's data. Necessary envs:

```bash
export SMOKE_CAAS_USERNAME=
export SMOKE_CAAS_PASSWORD=
export SMOKE_CAAS_CUSTOMER=
export SMOKE_CAAS_API_LINK=

# SMOKE_TEST_[AWS|AZURE|GCP]_...
export SMOKE_CAAS_AWS_RULE_SOURCE_SECRET=
export SMOKE_CAAS_AWS_RULE_SOURCE_PID=
export SMOKE_CAAS_AWS_RULE_SOURCE_REF=
export SMOKE_CAAS_AWS_RULE_SOURCE_URL=
export SMOKE_CAAS_AWS_RULE_SOURCE_PREFIX=
```

Example:

```bash
export SMOKE_CAAS_USERNAME=test
export SMOKE_CAAS_PASSWORD=password
export SMOKE_CAAS_CUSTOMER=TEST
export SMOKE_CAAS_API_LINK=http://127.0.0.1:8000

export SMOKE_CAAS_AWS_RULE_SOURCE_PID=epam/ecc-aws-rulepack
export SMOKE_CAAS_AWS_RULE_SOURCE_REF=main
export SMOKE_CAAS_AWS_RULE_SOURCE_PREFIX=policies/

python tests/smoke/rules_management.py
```

