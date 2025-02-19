#!/bin/bash

LOG_PATH="${LOG_PATH:-/var/log/sre-init.log}"
ERROR_LOG_PATH="${ERROR_LOG_PATH:-/var/log/sre-init.log}"

SYNDICATE_HELM_REPOSITORY="${SYNDICATE_HELM_REPOSITORY:-https://charts-repository.s3.eu-west-1.amazonaws.com/syndicate/}"
HELM_RELEASE_NAME="${HELM_RELEASE_NAME:-rule-engine}"
DEFECTDOJO_HELM_RELEASE_NAME="${DEFECTDOJO_HELM_RELEASE_NAME:-defectdojo}"

DOCKER_VERSION="${DOCKER_VERSION:-5:27.1.1-1~debian.12~bookworm}"
MINIKUBE_VERSION="${MINIKUBE_VERSION:-v1.33.1}"
KUBERNETES_VERSION="${KUBERNETES_VERSION:-v1.30.0}"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.30.3}"
HELM_VERSION="${HELM_VERSION:-3.15.3-1}"

SRE_LOCAL_PATH="${SRE_LOCAL_PATH:-/usr/local/sre}"
LM_API_LINK="${LM_API_LINK:-https://lm.syndicate.team}"
GITHUB_REPO="${GITHUB_REPO:-epam/syndicate-rule-engine}"

FIRST_USER="${FIRST_USER:-$(getent passwd 1000 | cut -d : -f 1)}"
DO_NOT_ACTIVATE_LICENSE="${DO_NOT_ACTIVATE_LICENSE:-}"


log() { echo "[INFO] $(date) $1" >> "$LOG_PATH"; }
log_err() { echo "[ERROR] $(date) $1" >> "$ERROR_LOG_PATH"; }
# shellcheck disable=SC2120
get_imds_token () {
  duration="10"  # must be an integer
  if [ -n "$1" ]; then
    duration="$1"
  fi
  curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: $duration"
}
get_from_metadata() { curl -sf -H "X-aws-ec2-metadata-token: $(get_imds_token)" "http://169.254.169.254/latest$1"; }
identity_document() { get_from_metadata "/dynamic/instance-identity/document"; }
document_signature() { get_from_metadata "/dynamic/instance-identity/signature" | tr -d '\n'; }
region() { get_from_metadata "/dynamic/instance-identity/document" | jq -r ".region"; }
request_to_lm() { curl -sf -X POST -d "{\"signature\":\"$(document_signature)\",\"document\":\"$(identity_document | base64 -w 0)\"}" "$LM_API_LINK/marketplace/custodian/init"; }
generate_password() {
  chars="20"
  typ='-base64'
  if [ -n "$1" ]; then
    chars="$1"
  fi
  if [ -n "$2" ]; then
    typ="$2"
  fi
  openssl rand "$typ" "$chars"
}
minikube_ip(){ sudo su "$FIRST_USER" -c "minikube ip"; }
enable_minikube_service() {
  sudo tee /etc/systemd/system/rule-engine-minikube.service <<EOF > /dev/null
[Unit]
Description=Rule engine minikube start up
After=docker.service

[Service]
Type=oneshot
ExecStart=/usr/bin/minikube start --profile rule-engine --force --interactive=false
ExecStop=/usr/bin/minikube stop --profile rule-engine
User=$FIRST_USER
Group=$FIRST_USER
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
  sudo systemctl enable rule-engine-minikube.service
}
upgrade_and_install_packages() {
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
  # sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y jq curl python3-pip locales-all nginx pipx
}
install_docker() {
  # Add Docker's official GPG key: from https://docs.docker.com/engine/install/debian/
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  # Add git apt repo
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce="$1" docker-ce-cli="$1" containerd.io
}
install_minikube() {
  # https://minikube.sigs.k8s.io/docs/start
  curl -LO "https://storage.googleapis.com/minikube/releases/$1/minikube_latest_$(dpkg --print-architecture).deb"
  sudo dpkg -i "minikube_latest_$(dpkg --print-architecture).deb" && rm "minikube_latest_$(dpkg --print-architecture).deb"
}
install_kubectl() {
  # https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/#install-kubectl-binary-with-curl-on-linux
  curl -LO "https://dl.k8s.io/release/$1/bin/linux/$(dpkg --print-architecture)/kubectl"
  curl -LO "https://dl.k8s.io/release/$1/bin/linux/$(dpkg --print-architecture)/kubectl.sha256"
  echo "$(cat kubectl.sha256) kubectl" | sha256sum --check || exit 1
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl kubectl.sha256
}
install_helm() {
  # https://helm.sh/docs/intro/install/
  curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
  sudo apt-get install apt-transport-https --yes
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
  sudo apt-get update
  sudo apt-get install helm="$1"
}
nginx_conf() {
  cat <<EOF
#load_module /usr/lib/nginx/modules/ngx_stream_module.so;  # for mongo
worker_processes auto;
pid /run/nginx.pid;
error_log /var/log/nginx/error.log;
worker_rlimit_nofile 8192;
events {
    worker_connections 4096;
}
http {
    access_log off;
    server_tokens off;
    gzip on;
    gzip_min_length 10240;
    gzip_disable msie6;
    gzip_types application/json;

    client_body_timeout 5s;
    client_header_timeout 5s;
    limit_req_zone \$binary_remote_addr zone=req_per_ip:10m rate=30r/s;
    limit_req_status 429;

    include /etc/nginx/mime.types;
    include /etc/nginx/sites-enabled/*;
}
#stream {
#    include /etc/nginx/streams-enabled/*;
#}
EOF
}
nginx_defectdojo_conf() {
  cat <<EOF
server {
    listen 80;
    location / {
        include /etc/nginx/proxy_params;
        proxy_set_header X-NginX-Proxy true;
        real_ip_header X-Real-IP;
        proxy_pass http://$(minikube_ip):32107;  # dojo
    }
}
EOF
}
nginx_minio_api_conf() {
  cat <<EOF
server {
    listen 9000;
    ignore_invalid_headers off;
    client_max_body_size 0;
    proxy_buffering off;
    proxy_request_buffering off;
    location / {
        include /etc/nginx/proxy_params;
        proxy_connect_timeout 300;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        chunked_transfer_encoding off;
        proxy_pass http://$(minikube_ip):32102; # minio api
   }
}
EOF
}
nginx_minio_console_conf() {
  cat <<EOF
server {
    listen 9001;
    ignore_invalid_headers off;
    client_max_body_size 0;
    proxy_buffering off;
    proxy_request_buffering off;
    location / {
        include /etc/nginx/proxy_params;
        proxy_set_header X-NginX-Proxy true;
        real_ip_header X-Real-IP;
        proxy_connect_timeout 300;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        # proxy_set_header Origin '';
        chunked_transfer_encoding off;
        proxy_pass http://$(minikube_ip):32103; # minio ui
   }
}
EOF
}
nginx_vault_conf() {
  # just for debugging purposes
  cat <<EOF
server {
    listen 8200;
    location / {
        include /etc/nginx/proxy_params;
        proxy_pass http://$(minikube_ip):32100;
    }
}
EOF
}
nginx_mongo_conf() {
  # just for debugging purposes
  cat <<EOF
server {
    listen 27017;
    proxy_connect_timeout 1s;
    proxy_timeout 3s;
    proxy_pass $(minikube_ip):32101;
}
EOF
}
nginx_sre_conf() {
  cat <<EOF
server {
    listen 8000;
    location /re {
        include /etc/nginx/proxy_params;
        proxy_set_header X-Original-URI \$request_uri;
        proxy_redirect off;
        proxy_pass http://$(minikube_ip):32106/caas;
        limit_req zone=req_per_ip burst=5 nodelay;
    }
    location /ms {
        include /etc/nginx/proxy_params;
        proxy_set_header X-Original-URI \$request_uri;
        proxy_redirect off;
        proxy_pass http://$(minikube_ip):32104/dev;
        limit_req zone=req_per_ip burst=5 nodelay;
    }
}
EOF
}
nginx_modular_api_conf() {
  cat <<EOF
server {
    listen 8085;
    location / {
        include /etc/nginx/proxy_params;
        proxy_redirect off;
        proxy_pass http://$(minikube_ip):32105;
        limit_req zone=req_per_ip burst=5 nodelay;
    }
}
EOF
}
build_helm_values() {
  # builds values for modularSdk role
  if [ -z "$MODULAR_SDK_ROLE_ARN" ]; then
    return
  fi
  local modular_region
  modular_region="${MODULAR_SDK_REGION:-$(region)}"
  echo -n "--set=modular-service.modularSdk.serviceMode=saas,modular-service.modularSdk.awsRegion=${modular_region},modular-service.modularSdk.assumeRoleArn=${MODULAR_SDK_ROLE_ARN//,/\\,} --set=modularSdk.serviceMode=saas,modularSdk.awsRegion=${modular_region},modularSdk.assumeRoleArn=${MODULAR_SDK_ROLE_ARN//,/\\,}"
}

if [ -z "$RULE_ENGINE_RELEASE" ]; then
  error_log "RULE_ENGINE_RELEASE env is required"
  exit 1
fi
# some steps that are better to be done before user tries to log in, so put them first
sudo -u "$FIRST_USER" mkdir -p "$(getent passwd "$FIRST_USER" | cut -d: -f6)/.local/bin" || true

log "Adding user $FIRST_USER to docker group"
sudo groupadd docker || true
sudo usermod -aG docker "$FIRST_USER" || true

log "Script is executed on behalf of $(id)"
log "The first run. Configuring sre for user $FIRST_USER"

log "Downloading artifacts"
sudo mkdir -p "$SRE_LOCAL_PATH/backups" || true
sudo mkdir -p "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE" || true
sudo wget -q -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre-init.sh" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/sre-init.sh"
sudo cp "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre-init.sh" /usr/local/bin/sre-init
sudo chmod +x /usr/local/bin/sre-init
sudo wget -q -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/modular_cli.tar.gz" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/modular_cli.tar.gz"  # todo get from modular-cli repo
sudo wget -q -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre_obfuscator.tar.gz" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/sre_obfuscator.tar.gz"
sudo chown -R "$FIRST_USER":"$FIRST_USER" "$SRE_LOCAL_PATH"

if [ -z "$DO_NOT_ACTIVATE_LICENSE" ]; then
  log "Going to make request to license manager"
  if ! lm_response="$(request_to_lm)"; then
    log_err "Unsuccessful response from the license manager"
    exit 1
  fi
  lm_response=$(jq --indent 0 '.items[0]' <<<"$lm_response")
  log "License information was received"
else
  log "Skipping license activation step"
  lm_response=""
fi

# Prerequisite
log "Upgrading system and installing some necessary packages"
upgrade_and_install_packages

log "Installing docker $DOCKER_VERSION"
install_docker "$DOCKER_VERSION"

log "Installing minikube $MINIKUBE_VERSION"
install_minikube "$MINIKUBE_VERSION"

log "Installing kubectl $KUBECTL_VERSION"
install_kubectl "$KUBECTL_VERSION"

log "Installing helm $HELM_VERSION"
install_helm "$HELM_VERSION"

log "Starting minikube and installing helm releases on behalf of $FIRST_USER"
sudo su - "$FIRST_USER" <<EOF
minikube start --driver=docker --container-runtime=containerd -n 1 --force --interactive=false --memory=max --cpus=max --profile rule-engine --kubernetes-version=$KUBERNETES_VERSION
minikube profile rule-engine  # making default
kubectl create secret generic minio-secret --from-literal=username=miniouser --from-literal=password=$(generate_password)
kubectl create secret generic mongo-secret --from-literal=username=mongouser --from-literal=password=$(generate_password 30 -hex)
kubectl create secret generic vault-secret --from-literal=token=$(generate_password 30)
kubectl create secret generic redis-secret --from-literal=password=$(generate_password 30 -hex)
kubectl create secret generic rule-engine-secret --from-literal=system-password=$(generate_password 30)
kubectl create secret generic modular-api-secret --from-literal=system-password=$(generate_password 20 -hex) --from-literal=secret-key="$(generate_password 50)"
kubectl create secret generic modular-service-secret --from-literal=system-password=$(generate_password 30)
kubectl create secret generic defectdojo-secret --from-literal=secret-key="$(generate_password 50)" --from-literal=credential-aes-256-key=$(generate_password) --from-literal=db-username=defectdojo --from-literal=db-password=$(generate_password 30 -hex)

helm repo add syndicate "$SYNDICATE_HELM_REPOSITORY"
helm repo update syndicate

helm install "$HELM_RELEASE_NAME" syndicate/rule-engine --version $RULE_ENGINE_RELEASE $(build_helm_values)
helm install "$DEFECTDOJO_HELM_RELEASE_NAME" syndicate/defectdojo
EOF


if [ -z "$lm_response" ]; then
  sudo su - "$FIRST_USER" <<EOF
kubectl create secret generic lm-data --from-literal=api-link='$LM_API_LINK'
EOF
else
  sudo su - "$FIRST_USER" <<EOF
kubectl create secret generic lm-data --from-literal=api-link='$LM_API_LINK' --from-literal=lm-response='$lm_response'
EOF
fi

log "Getting Defect dojo password (usually takes 3-4 minutes)"
while ! dojo_pass="$(sudo su "$FIRST_USER" -c "kubectl logs job.batch/defectdojo-initializer" 2>/dev/null | grep -oP "Admin password: \K\w+")"; do
  sleep 5
done
dojo_pass="$(base64 <<< "$dojo_pass")"

sudo su - "$FIRST_USER" <<EOF
kubectl patch secret defectdojo-secret -p="{\"data\":{\"system-password\":\"$dojo_pass\"}}"
EOF
log "Defect dojo secret was saved"

log "Enabling minikube service"
enable_minikube_service

log "Configuring nginx"
sudo rm -f /etc/nginx/sites-enabled/*
sudo rm -f /etc/nginx/sites-available/*
sudo mkdir /etc/nginx/streams-available || true
sudo mkdir /etc/nginx/streams-enabled || true

nginx_conf | sudo tee /etc/nginx/nginx.conf > /dev/null
nginx_defectdojo_conf | sudo tee /etc/nginx/sites-available/defectdojo > /dev/null
nginx_minio_api_conf | sudo tee /etc/nginx/sites-available/minio > /dev/null
nginx_minio_console_conf | sudo tee /etc/nginx/sites-available/minio-console > /dev/null
nginx_vault_conf | sudo tee /etc/nginx/sites-available/vault > /dev/null
nginx_mongo_conf | sudo tee /etc/nginx/streams-available/mongo > /dev/null
nginx_sre_conf | sudo tee /etc/nginx/sites-available/sre > /dev/null  # rule-engine + modular-service
nginx_modular_api_conf | sudo tee /etc/nginx/sites-available/modular-api > /dev/null

sudo ln -sf /etc/nginx/sites-available/defectdojo /etc/nginx/sites-enabled/
sudo ln -sf /etc/nginx/sites-available/modular-api /etc/nginx/sites-enabled/
sudo ln -sf /etc/nginx/sites-available/minio /etc/nginx/sites-enabled/

sudo nginx -s reload

log "Cleaning apt cache"
sudo apt-get clean || true