# CaaS Exported module

It must be used via Kubernetes, but if you are a developer, you can use 
it locally to make the development easier


### Installation

```bash
pip install -r src/onprem/requirements.txt
```


### Configuration
Create `.env` file with such contents:

```bash
# exclusively on-prem envs
# name=value  # [syndicate alias name]
# if alias name is not specified, it's the same as env name.
# Commented envs are necessary only for saas

SERVICE_MODE=docker

# <buckets>
CAAS_REPORTS_BUCKET_NAME=reports  # reports-bucket
CAAS_RULESETS_BUCKET_NAME=rulesets
CAAS_STATISTICS_BUCKET_NAME=statistics
CAAS_METRICS_BUCKET_NAME=metrics
# </buckets>

# <General settings>
feature_skip_cloud_identifier_validation=false
batch_job_log_level=DEBUG
# batch_job_def_name=caas-job-definition  # reports-submit-job-definition
# batch_job_queue_name=caas-job-queue  # reports-submit-job-queue
# event_bridge_service_role_to_invoke_batch=  # event-bridge-service-role-to-invoke-batch
# lambdas_alias_name=dev
# </General settings>

# <Custodian secrets>
MONGO_DATABASE=custodian_as_a_service
MONGO_USER=
MONGO_PASSWORD=
MONGO_URL=
VAULT_URL=
VAULT_SERVICE_SERVICE_PORT=
VAULT_TOKEN=
MINIO_HOST=
MINIO_PORT=
MINIO_ACCESS_KEY=
MINIO_SECRET_ACCESS_KEY=
# </Custodian secrets>

# <On-prem executor>
# EXECUTOR_PATH: /custodian-as-a-service/docker/executor.py
# VENV_PATH: /custodian-as-a-service/docker/.executor_venv/bin/python
# </On-prem executor>

# <MODULAR SDK>
modular_service_mode=
modular_mongo_db_name=
modular_mongo_user=
modular_mongo_password=
modular_mongo_url=
# modular_assume_role_arn=
# MODULAR_AWS_REGION=
# </MODULAR SDK>
```
Fill in the empty values with valid credentials. 


Execute
```bash
python src/main.py init_vault
python src/main.py create_buckets
python src/main.py create_indexes
```

```bash
python /custodian-as-a-service/scripts/configure_environment.py init --username admin --api_link http://127.0.0.1:8000/caas $lm_api_link_param
python /custodian-as-a-service/scripts/configure_environment.py create_customer --customer_name "${CUSTOMER_NAME:-$DEFAULT_CUSTOMER_NAME}" --admins "${CUSTOMER_OWNER:-$DEFAULT_CUSTOMER_OWNER}"
python /custodian-as-a-service/scripts/configure_environment.py create_user --username "${USERNAME:-$DEFAULT_USERNAME}" --customer_name "${CUSTOMER_NAME:-$DEFAULT_CUSTOMER_NAME}"
```