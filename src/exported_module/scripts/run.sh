#!/bin/bash

DEFAULT_CUSTOMER_NAME="EPAM Systems"
DEFAULT_CUSTOMER_OWNER=demo@epam.com
DEFAULT_USERNAME=demouser

echo "Creating the necessary buckets in Minio"
python main.py create_buckets

echo "Creating the necessary engine and token in Vault"
python main.py init_vault

echo "Creating indexes in MongoDB"
python main.py create_indexes

echo "Creating the SYSTEM customer"
lm_api_link_param=""
if [ ! -z "$LM_API_LINK" ]; then
  lm_api_link_param+="--lm_api_link $LM_API_LINK"
fi
# TODO think about using private ip addresses instead of MINIKUBE_IP and expose the API via kubectl port-forward
python main.py env update_settings $lm_api_link_param

echo "Creating the system user"
python main.py env create_system_user --username admin --api_link http://${MINIKUBE_IP}:30300/caas
echo "Creating the standard customer"
python main.py env create_customer --customer_name "${CUSTOMER_NAME:-$DEFAULT_CUSTOMER_NAME}" --admins "${CUSTOMER_OWNER:-$DEFAULT_CUSTOMER_OWNER}"

echo "Creating a user within the customer"
python main.py env create_user --username "${USERNAME:-$DEFAULT_USERNAME}" --customer_name "${CUSTOMER_NAME:-$DEFAULT_CUSTOMER_NAME}"

echo "Starting the server"

python main.py run --gunicorn