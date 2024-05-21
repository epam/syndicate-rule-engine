#!/bin/bash

# Constants
LM_API_LINK=""
RULE_ENGINE_RELEASE=5.1.0

SRE_LOCAL_PATH=/usr/local/sre
SRE_ARTIFACTS_PATH=$SRE_LOCAL_PATH/artifacts
SRE_SECRETS_PATH=$SRE_LOCAL_PATH/secrets

DEFECT_DOJO_SYSTEM_PASSWORD_FILENAME=defect-dojo-pass
LM_RESPONSE_FILENAME=lm-response
LM_API_LINK_FILENAME=lm-link

RULE_ENGINE_ENVS_FILENAME=rule-engine.env
DEFECT_DOJO_ENVS_FILENAME=defect-dojo.env

RULE_ENGINE_IMAGE_FILENAME=rule-engine.tar.gz
MODULAR_SERVICE_IMAGE_FILENAME=modular-service.tar.gz
MODULAR_API_IMAGE_FILENAME=modular-api.tar.gz
DEFECT_DOJO_DJANGO_IMAGE_FILENAME=defectdojo-django.tar.gz
DEFECT_DOJO_NGINX_IMAGE_FILENAME=defectdojo-nginx.tar.gz

RULE_ENGINE_COMPOSE_FILENAME=compose.yaml
DEFECT_DOJO_COMPOSE_FILENAME=dojo-compose.yaml

GENERATE_RANDOM_ENVS_SCRIPT_FILENAME=generate_random_envs.py

FIRST_USER=$(getent passwd 1000 | cut -d : -f 1)

# Functions
log() { logger -s "$1"; }
log_err() { logger -s -p user.err "$1"; }
get_imds_token () {
  duration="10"  # must be an integer
  if [ -n "$1" ]; then
    duration="$1"
  fi
  curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: $duration"
}
identity_document() { curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/document; }
document_signature() { curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/signature | tr -d '\n'; }
request_to_lm() {
    curl -s -X POST -d "{\"signature\":\"$(document_signature)\",\"document\":\"$(identity_document | base64 -w 0)\"}" "$LM_API_LINK/marketplace/custodian/init"
}


if [ -f $SRE_LOCAL_PATH/success ]; then
  log "Not the first run. Exiting"
  exit 0
else
  log "The first run. Configuring sre"
fi

# Prerequisite
log "Installing docker and other necessary packages"
sudo DEBIAN_FRONTEND=noninteractive apt update -y
sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y
sudo DEBIAN_FRONTEND=noninteractive apt install -y git jq python3-pip unzip locales-all

# Add Docker's official GPG key: from https://docs.docker.com/engine/install/debian/
sudo DEBIAN_FRONTEND=noninteractive apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
# Add git apt repo
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo DEBIAN_FRONTEND=noninteractive apt update -y
sudo DEBIAN_FRONTEND=noninteractive apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


log "Creating temp directory"
mkdir sre-build-temp-dir/ && cd sre-build-temp-dir/
mkdir artifacts/
mkdir secrets/

log "Pulling artifacts"
wget -O rule-engine-artifacts.zip "https://github.com/epam/ecc/releases/download/$RULE_ENGINE_RELEASE/rule-engine-ami-artifacts.linux-$(dpkg --print-architecture).zip"
unzip rule-engine-artifacts.zip -d artifacts/
cd artifacts/

log "Loading docker images from artifacts"
sudo docker load -i $MODULAR_API_IMAGE_FILENAME
sudo docker load -i $MODULAR_SERVICE_IMAGE_FILENAME
sudo docker load -i $RULE_ENGINE_IMAGE_FILENAME
sudo docker load -i $DEFECT_DOJO_DJANGO_IMAGE_FILENAME
sudo docker load -i $DEFECT_DOJO_NGINX_IMAGE_FILENAME


# TODO maybe remove these steps. It depends.
sudo docker image tag localhost/m3-modular-admin:latest m3-modular-admin:latest
sudo docker image tag localhost/modular-service:latest modular-service:latest
sudo docker image tag localhost/caas-custodian-k8s-dev:latest caas-custodian-k8s-dev:latest


log "Generating random passwords for docker compose"
python3 $GENERATE_RANDOM_ENVS_SCRIPT_FILENAME --rule-engine > ../secrets/$RULE_ENGINE_ENVS_FILENAME
python3 $GENERATE_RANDOM_ENVS_SCRIPT_FILENAME --dojo > ../secrets/$DEFECT_DOJO_ENVS_FILENAME
source ../secrets/$RULE_ENGINE_ENVS_FILENAME


log "Starting Rule Engine docker compose"
sudo docker compose --project-directory ./ --file $RULE_ENGINE_COMPOSE_FILENAME --env-file ../secrets/$RULE_ENGINE_ENVS_FILENAME --profile modular-service --profile custodian-service --profile modular-api up -d


while : ; do
  # loop because sometimes it fails if we make "docker compose up" immediately after another "docker compose up"
  log "Starting Defect Dojo docker compose"
  sleep 2
  sudo docker compose --file $DEFECT_DOJO_COMPOSE_FILENAME --env-file ../secrets/$DEFECT_DOJO_ENVS_FILENAME up -d
  code=$?
  if [ $code -eq 0 ]; then
    break
  fi
  log_err "Failed to start Defect Dojo docker compose"
done


log "Going to make request to license manager"
lm_response=$(request_to_lm)
code=$?
if [ $code -ne 0 ];
then
  log_err "Unsuccessful response from the license manager"
  exit 1
fi
lm_response=$(echo "$lm_response" | jq --indent 0 ".items[0]")
echo "$lm_response" > ../secrets/$LM_RESPONSE_FILENAME
echo $LM_API_LINK > ../secrets/$LM_API_LINK_FILENAME
log "License information was received"


# here wait for dojo via health check
# do this almost in the end because dojo starts slowly
log "Getting Defect dojo password"
while [ -z "$dojo_pass" ]; do
  sleep 2
  dojo_pass=$(sudo docker compose --file $DEFECT_DOJO_COMPOSE_FILENAME logs initializer | grep -oP "Admin password: \K\w+")
done
echo "$dojo_pass" > ../secrets/$DEFECT_DOJO_SYSTEM_PASSWORD_FILENAME


log "Copying artifacts to $SRE_LOCAL_PATH"
log "Making $FIRST_USER an owner of generated secrets"
sudo mkdir -p $SRE_LOCAL_PATH

cd ..
sudo cp -R secrets/ $SRE_SECRETS_PATH
sudo cp -R artifacts/ $SRE_ARTIFACTS_PATH
sudo cp artifacts/sre-init.sh /usr/local/bin/sre-init
sudo chmod +x /usr/local/bin/sre-init
sudo chmod +x $SRE_ARTIFACTS_PATH/$GENERATE_RANDOM_ENVS_SCRIPT_FILENAME
sudo chown -R $FIRST_USER:$FIRST_USER $SRE_LOCAL_PATH
sudo chmod -R o-r $SRE_SECRETS_PATH

cd ..
log "Cleaning temp directory"
rm -rf sre-build-temp-dir/

log "Cleaning apt cache"
sudo apt clean

# Lock
sudo touch $SRE_LOCAL_PATH/success
sudo chmod 000 $SRE_LOCAL_PATH/success
