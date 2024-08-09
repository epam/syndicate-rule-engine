#!/bin/bash

LOG_PATH=/var/log/sre-init.log
ERROR_LOG_PATH=$LOG_PATH
HELM_RELEASE_NAME=rule-engine

log() { echo "[INFO] $(date) $1" >> $LOG_PATH; }
log_err() { echo "[ERROR] $(date) $1" >> $ERROR_LOG_PATH; }
# shellcheck disable=SC2120
get_imds_token () {
  duration="10"  # must be an integer
  if [ -n "$1" ]; then
    duration="$1"
  fi
  curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: $duration"
}
identity_document() { curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/document; }
document_signature() { curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/signature | tr -d '\n'; }
request_to_lm() { curl -s -X POST -d "{\"signature\":\"$(document_signature)\",\"document\":\"$(identity_document | base64 -w 0)\"}" "$LM_API_LINK/marketplace/custodian/init"; }
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
  sudo DEBIAN_FRONTEND=noninteractive apt update -y
  sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y
  sudo DEBIAN_FRONTEND=noninteractive apt install -y jq python3-pip locales-all nginx
}
install_docker() {
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
  sudo DEBIAN_FRONTEND=noninteractive apt install -y docker-ce docker-ce-cli containerd.io
}
install_minikube() {
  # https://minikube.sigs.k8s.io/docs/start
  log "Installing minikube"
  curl -LO "https://storage.googleapis.com/minikube/releases/latest/minikube_latest_$(dpkg --print-architecture).deb"
  sudo dpkg -i "minikube_latest_$(dpkg --print-architecture).deb" && rm "minikube_latest_$(dpkg --print-architecture).deb"
}
install_kubectl() {
  # https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/#install-kubectl-binary-with-curl-on-linux
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl"  # todo specify concrete release
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl.sha256"
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check || exit 1
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl kubectl.sha256
}
install_helm() {
  # https://helm.sh/docs/intro/install/
  curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
  sudo apt-get install apt-transport-https --yes
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
  sudo apt-get update
  sudo apt-get install helm
}
nginx_conf() {
  cat <<EOF
worker_processes auto;
pid /run/nginx.pid;
error_log /var/log/nginx/error.log;
# error_log /dev/null emerg;
events {
    worker_connections 1024;
}
http {
    include /etc/nginx/mime.types;
    include /etc/nginx/sites-enabled/*;
}
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
nginx_sre_conf() {
  cat <<EOF
server {
    listen 8000;
    location /sre {
        include /etc/nginx/proxy_params;
        proxy_set_header X-Original-URI \$request_uri;
        proxy_redirect off;
        proxy_pass http://$(minikube_ip):32106/caas;
    }
    location /ms {
        include /etc/nginx/proxy_params;
        proxy_set_header X-Original-URI \$request_uri;
        proxy_redirect off;
        proxy_pass http://$(minikube_ip):32104/dev;
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
    }
}
EOF
}

# $SRE_LOCAL_PATH $LM_API_LINK, $RULE_ENGINE_RELEASE, $FIRST_USER will be provided from outside
if [ -z "$SRE_LOCAL_PATH" ] || [ -z "$LM_API_LINK" ] || [ -z "$RULE_ENGINE_RELEASE" ] || [ -z "$FIRST_USER" ] || [ -z "$GITHUB_REPO" ]; then
  error_log "SRE_LOCAL_PATH=$SRE_LOCAL_PATH LM_API_LINK=$LM_API_LINK RULE_ENGINE_RELEASE=$RULE_ENGINE_RELEASE FIRST_USER=$FIRST_USER. Something is not provided"
  exit 1
fi
log "Script is executed on behalf of $(id)"

log "The first run. Configuring sre for user $FIRST_USER"

# Prerequisite
log "Upgrading system and installing some necessary packages"
upgrade_and_install_packages

log "Installing docker"
install_docker

log "Installing minikube"
install_minikube

log "Installing kubectl"
install_kubectl

log "Installing helm"
install_helm

log "Adding user $FIRST_USER to docker group"
sudo usermod -aG docker "$FIRST_USER"

log "Starting minikube and installing helm releases on behalf of $FIRST_USER"
sudo su - "$FIRST_USER" <<EOF
minikube start --driver=docker --container-runtime=containerd -n 1 --force --interactive=false --memory=max --cpus=max --profile rule-engine
minikube profile rule-engine  # making default
kubectl create secret generic minio-secret --from-literal=username=miniouser --from-literal=password=$(generate_password)
kubectl create secret generic mongo-secret --from-literal=username=mongouser --from-literal=password=$(generate_password 30 -hex)
kubectl create secret generic vault-secret --from-literal=token=$(generate_password 30)
kubectl create secret generic rule-engine-secret --from-literal=system-password=$(generate_password 30)
kubectl create secret generic modular-api-secret --from-literal=system-password=$(generate_password 20 -hex) --from-literal=secret-key="$(generate_password 50)"
kubectl create secret generic modular-service-secret --from-literal=system-password=$(generate_password 30)
kubectl create secret generic defect-dojo-secret --from-literal=secret-key="$(generate_password 50)" --from-literal=credential-aes-256-key=$(generate_password) --from-literal=db-username=defectdojo --from-literal=db-password=$(generate_password 30 -hex)

helm plugin install https://github.com/hypnoglow/helm-s3.git
helm repo add sre s3://charts-repository/syndicate/
helm repo update

helm install "$HELM_RELEASE_NAME" sre/rule-engine
helm install defectdojo sre/defectdojo
EOF

log "Downloading artifacts"
sudo mkdir -p "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE"
sudo wget -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/modular_cli.tar.gz" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/modular_cli.tar.gz"  # todo get from modular-cli repo
sudo wget -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre_obfuscator.tar.gz" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/sre_obfuscator.tar.gz"
sudo wget -O "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre-init.sh" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/sre-init.sh"
sudo cp "$SRE_LOCAL_PATH/releases/$RULE_ENGINE_RELEASE/sre-init.sh" /usr/local/bin/sre-init
sudo chmod +x /usr/local/bin/sre-init
sudo chown -R $FIRST_USER:$FIRST_USER "$SRE_LOCAL_PATH"


log "Going to make request to license manager"
lm_response=$(request_to_lm)
code=$?
if [ $code -ne 0 ];
then
  log_err "Unsuccessful response from the license manager"
  exit 1
fi
lm_response=$(echo "$lm_response" | jq --indent 0 ".items[0]")
sudo su - "$FIRST_USER" <<EOF
kubectl create secret generic lm-data --from-literal=api-link='$LM_API_LINK' --from-literal=lm-response='$lm_response'
EOF
log "License information was received"


log "Getting Defect dojo password"
while [ -z "$dojo_pass" ]; do
  sleep 5
  dojo_pass=$(sudo su "$FIRST_USER" -c "kubectl logs job.batch/defectdojo-initializer" | grep -oP "Admin password: \K\w+")
done
dojo_pass=$(base64 <<< "$dojo_pass")

sudo su - "$FIRST_USER" <<EOF
kubectl patch secret defect-dojo-secret -p="{\"data\":{\"system-password\":\"$dojo_pass\"}}"
EOF
log "Defect dojo secret was saved"

log "Enabling minikube service"
enable_minikube_service

log "Configuring nginx"
nginx_conf | sudo tee /etc/nginx/nginx.conf > /dev/null
nginx_defectdojo_conf | sudo tee /etc/nginx/sites-available/defectdojo > /dev/null
nginx_minio_api_conf | sudo tee /etc/nginx/sites-available/minio_api > /dev/null
nginx_minio_console_conf | sudo tee /etc/nginx/sites-available/minio_console > /dev/null
nginx_sre_conf | sudo tee /etc/nginx/sites-available/sre > /dev/null
nginx_modular_api_conf | sudo tee /etc/nginx/sites-available/modular > /dev/null

sudo rm /etc/nginx/sites-enabled/*
sudo ln -s /etc/nginx/sites-available/defectdojo /etc/nginx/sites-enabled/
sudo ln -s /etc/nginx/sites-available/modular /etc/nginx/sites-enabled/

sudo nginx -s reload

log "Cleaning apt cache"
sudo apt clean
log 'Done'