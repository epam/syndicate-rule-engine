#!/bin/bash

cmd_usage() {
  cat <<EOF
Manage Rule Engine installation

Usage:
  $PROGRAM [command]

Available Commands:
  init     Initializes Rule Engine installation
  update   Updates the installation
  help     Show help message
  version  Print versions information
EOF
}

cmd_init_usage() {
  cat <<-EOF
Initializes Rule Engine

Description:
  Initializes Rule Engine for the first time or for a specified user. Includes installing CLIs, configuring passwords and other

Usage:
  $PROGRAM $COMMAND [options]

Examples:
  $PROGRAM $COMMAND --system
  $PROGRAM $COMMAND --user example --public-ssh-key "ssh-rsa AAA..."

Options:
  -h, --help        Show this message and exit
  --system          Initialize SRE for the first time. Only possible for $FIRST_USER user. Creates necessary system entities
  --user            Initialize SRE for the given user
  --public-ssh-key  If specified will be added to user's authorized_keys.
  --re-username     Rule Engine username to configure. Must be specified together with --re-password
  --re-password     Rule Engine password to configure. Must be specified together with --re-username
  --admin-username  Modular Service username to configure. Must be specified together with --admin-password
  --admin-password  Modular Service password to configure. Must be specified together with --admin-username
EOF
}


cmd_version() {
  echo "$VERSION"
}
die() {
  echo "$@" >&2
  exit 1
}
cmd_unrecognized() {
  cat <<EOF
Error: unrecognized command \`$PROGRAM $COMMAND\`
Try '$PROGRAM --help' for more information
EOF
}


# helper functions
get_latest_local_release() { ls "$SRE_RELEASES_PATH" | sort -r | head -n 1; }
get_helm_release_version() {
  # currently the version of rule engine chart corresponds to the version of app inside
  helm get metadata "$HELM_RELEASE_NAME" -o json | jq -r '.version'
}
ensure_in_path() {
  if [[ ":$PATH:" != *":$1:"* ]]; then
    export PATH=$PATH:$1
  fi
}
# shellcheck disable=SC2120
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
get_imds_token () {
  curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 20"
}
account_id() { curl -s curl -s -H "X-aws-ec2-metadata-token: $(get_imds_token)" http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r ".accountId"; }
user_exists() { id "$1" &>/dev/null; }
get_kubectl_secret() {
  kubectl get secret "$1" -o jsonpath="{.data.$2}" | base64 --decode
}
minikube_ip(){
  # user may exist in docker group but re-login wasn't made
  sudo su "$FIRST_USER" -c "minikube ip"
}

initialize_system() {
  # creates:
  # - non-system admin users for Rule Engine & Modular Service
  # - license entity based on LM response
  # - customer based on LM response
  # - tenant within the customer which represents this AWS account
  # - entity that represents defect dojo installation
  local mip=$(minikube_ip)

  ensure_in_path "$HOME/.local/bin"

  if pip freeze | grep sre-obfuscator > /dev/null; then
    echo "Obfuscation manager is already installed. Try: sreobf --help"
  else
    echo "Installing deobfuscation manager"
    pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/$OBFUSCATOR_ARTIFACT_NAME[xlsx]"
  fi
  if pip freeze | grep modular-cli > /dev/null; then
    echo "Modular cli is already installed. Try: syndicate --help"
  else
    echo "Installing modular-cli"
    MODULAR_CLI_ENTRY_POINT=syndicate pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/$MODULAR_CLI_ARTIFACT_NAME"
  fi

  echo "Logging in to modular-cli"
  syndicate setup --username admin --password "$(get_kubectl_secret modular-api-secret system-password)" --api_path "http://$mip:32105" --json
  syndicate login --json

  echo "Logging in to Rule engine using system user"
  syndicate re configure --api_link http://rule-engine:8000/caas --json
  syndicate re login --username system_user --password "$(get_kubectl_secret rule-engine-secret system-password)" --json

  echo "Logging in to Modular Service using system user"
  syndicate admin configure --api_link http://modular-service:8040/dev --json
  syndicate admin login --username system_user --password "$(get_kubectl_secret modular-service-secret system-password)" --json

  local lm_response=$(get_kubectl_secret lm-data lm-response)
  local customer_name=$(echo $lm_response | jq ".customer_name" -r)

  echo "Generating passwords for modular-service and rule-engine non-system users"
  local modular_service_password="$(generate_password)"
  local rule_engine_password="$(generate_password)"  # todo maybe save them to k8s as well

  echo "Creating modular service customer and its user"
  syndicate admin signup --username "$MODULAR_SERVICE_USERNAME" --password "$modular_service_password" --customer_name "$customer_name" --customer_display_name "$customer_name" --customer_admin admin@example.com --json

  echo "Creating custodian customer users"
  syndicate re meta update_meta --json
  syndicate re setting lm config add --host "$(get_kubectl_secret lm-data api-link)" --json
  syndicate re setting lm client add --key_id $(echo $lm_response | jq ".private_key.key_id" -r) --algorithm $(echo $lm_response | jq ".private_key.algorithm" -r) --private_key $(echo $lm_response | jq ".private_key.value" -r) --b64encoded --json
  syndicate re policy add --name admin_policy --permissions_admin --effect allow --tenant '*' --description "Full admin access policy for customer" --customer_id "$customer_name" --json
  syndicate re role add --name admin_role --policies admin_policy --description "Admin customer role" --customer_id "$customer_name" --json
  syndicate re users create --username "$RULE_ENGINE_USERNAME" --password "$rule_engine_password" --role_name admin_role --customer_id "$customer_name" --json


  echo "Logging in as customer users"
  syndicate admin login --username "$MODULAR_SERVICE_USERNAME" --password "$modular_service_password" --json
  syndicate re login --username "$RULE_ENGINE_USERNAME" --password "$rule_engine_password" --json

  echo "Adding tenant license"
  license_key=$(syndicate re license add --tenant_license_key $(echo $lm_response | jq ".tenant_license_key" -r) --json | jq ".items[0].license_key" -r)
  syndicate re license activate --license_key "$license_key" --all_tenants --json  # can be removed with new version of sre


  echo "Activating tenant for the current aws account"
  syndicate admin tenant create --name "$CURRENT_ACCOUNT_TENANT_NAME" --display_name "Tenant $(account_id)" --cloud AWS --account_id "$(account_id)" --primary_contacts admin@example.com --secondary_contacts admin@example.com --tenant_manager_contacts admin@example.com --default_owner admin@example.com --json
  echo "Activating region for tenant"
  for r in $AWS_REGIONS;
  do
    echo "Activating $r for tenant"
    syndicate admin tenant regions activate --tenant_name "$CURRENT_ACCOUNT_TENANT_NAME" --region_name "$r" --json
  done

  echo "Getting Defect dojo token"
  while [ -z "$dojo_token" ]; do
    sleep 2
    dojo_token=$(curl -X POST -H 'content-type: application/json' "http://$mip:32107/api/v2/api-token-auth/" -d "{\"username\":\"admin\",\"password\":\"$(get_kubectl_secret defect-dojo-secret system-password)\"}" | jq ".token" -r)
  done

  echo "Activating dojo installation for rule engine"
  activation_id=$(syndicate re integrations dojo add --url http://defectdojo:8080/api/v2 --api_key "$dojo_token" --description "Global dojo installation" --json | jq ".items[0].id" -r)
  syndicate re integrations dojo activate --integration_id "$activation_id" --all_tenants --scan_type "Generic Findings Import" --send_after_job --json
}

cmd_init() {
  local opts init_system="" target_user="" public_ssh_key="" re_username="" re_password="" admin_username="" admin_password=""
  opts="$(getopt -o h --long help,system,user:,public-ssh-key:,re-username:,re-password:,admin-username:,admin-password: -n "$PROGRAM" -- "$@")"
  local err="$?"
  [ "$err" -ne 0 ] && die "$(cmd_init_usage)"
  eval set -- "$opts"
  while true; do
    case "$1" in
      '-h'|'--help') cmd_init_usage; exit 0 ;;
      '--system') init_system="true"; shift ;;
      '--user') target_user="$2"; shift 2 ;;
      '--public-ssh-key') public_ssh_key="$2"; shift 2 ;;
      '--re-username') re_username="$2"; shift 2 ;;
      '--re-password') re_password="$2"; shift 2 ;;
      '--admin-username') admin_username="$2"; shift 2 ;;
      '--admin-password') admin_password="$2"; shift 2 ;;
      '--') shift; break ;;
    esac
  done

  if [ -z "$init_system" ] && [ -z "$target_user" ]; then
    die "Error: either --system or --user must be specified"
  fi

  if [ -n "$init_system" ]; then
    if [ "$FIRST_USER" != "$(whoami)" ]; then
      die "Error: system configuration can be performed only by '$FIRST_USER' user"
    fi
    if [ -f "$SRE_LOCAL_PATH/success" ]; then
      die "Error: Rule Engine was already initialized. Cannot do that again"
    fi
    echo "Initializing Rule Engine for the first time"
    initialize_system
    echo "Done"
    return
  fi

  # target_user must exist here
  local _username _password
  test -n "$re_username"; _username=$?
  test -n "$re_password"; _password=$?
  if [ "$(( _username ^ _password ))" -eq 1 ]; then
    die "Error: --re-username and --re-password must be specified together"
  fi

  test -n "$admin_username"; _username=$?
  test -n "$admin_password"; _password=$?
  if [ "$(( _username ^ _password ))" -eq 1 ]; then
    die "Error: --admin-username and --admin-password must be specified together"
  fi

  echo "Initializing Rule Engine for user $target_user"
  if user_exists "$target_user"; then
    echo "User already exists"
  else
    echo "User does not exist. Creating..."
    sudo useradd --create-home --shell /bin/bash --user-group "$target_user" || die "Error: could not create a user"
  fi

  if [ -n "$public_ssh_key" ]; then
    echo "Public SSH key was given. Adding this key to user's authorized_keys"
    sudo su - "$target_user" <<EOF
    mkdir -p .ssh
    chmod 700 .ssh
    echo "$public_ssh_key" >> .ssh/authorized_keys
    chmod 600 .ssh/authorized_keys
EOF
  fi
  echo "Installing CLIs for $target_user"
  sudo su - "$target_user" <<EOF
  pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/$OBFUSCATOR_ARTIFACT_NAME[xlsx]"
  MODULAR_CLI_ENTRY_POINT=syndicate pip3 install --user --break-system-packages "$SRE_RELEASES_PATH/$(get_latest_local_release)/$MODULAR_CLI_ARTIFACT_NAME"
EOF

  kubectl exec service/modular-api -- ./modular.py user describe --username "$target_user"

  if [ "$?" -ne 0 ]; then
    echo "Creating new modular-api user"
    local new_password="$(generate_password)"
    local api_path="http://$(minikube_ip):32105"
    kubectl exec service/modular-api -- ./modular.py user add --username "$target_user" --group admin_group --password "$new_password"
    sudo su - "$target_user" <<EOF
    echo "Logging in to modular-cli"
    ~/.local/bin/syndicate setup --username "$target_user" --password "$new_password" --api_path "$api_path" --json
    ~/.local/bin/syndicate login --json
EOF
  else
    echo "Modular api user has been initialized before"
  fi


  if [ -n "$re_username" ]; then
    echo "Logging in to Rule Engine"
    sudo su - "$target_user" <<EOF
    ~/.local/bin/syndicate re configure --api_link http://rule-engine:8000/caas --json
    ~/.local/bin/syndicate re login --username "$re_username" --password "$re_password" --json
EOF
  fi

  if [ -n "$admin_username" ]; then
    echo "Logging in to Modular Service"
    sudo su - "$target_user" <<EOF
    ~/.local/bin/syndicate admin configure --api_link http://modular-service:8040/dev --json
    ~/.local/bin/syndicate admin login --username "$admin_username" --password "$admin_password" --json
EOF
  fi
  echo "Done"
}

cmd_update() {
  echo "update"
}


# Start
VERSION="1.0.0"
PROGRAM="${0##*/}"
COMMAND="$1"

# Some global constants
SRE_LOCAL_PATH=/usr/local/sre
SRE_RELEASES_PATH=$SRE_LOCAL_PATH/releases
GITHUB_REPO=epam/ecc
HELM_RELEASE_NAME=rule-engine
MODULAR_SERVICE_USERNAME="customer_admin"
RULE_ENGINE_USERNAME="customer_admin"
CURRENT_ACCOUNT_TENANT_NAME="CURRENT_ACCOUNT"
# regions that will be allowed to activate
AWS_REGIONS="us-east-1 us-east-2 us-west-1 us-west-2 af-south-1 ap-east-1 ap-south-2 ap-southeast-3 ap-southeast-4 ap-south-1 ap-northeast-3 ap-northeast-2 ap-southeast-1 ap-southeast-2 ap-northeast-1 ca-central-1 ca-west-1 eu-central-1 eu-west-1 eu-west-2 eu-south-1 eu-west-3 eu-south-2 eu-north-1 eu-central-2 il-central-1 me-south-1 me-central-1 sa-east-1 us-gov-east-1 us-gov-west-1"

MODULAR_CLI_ARTIFACT_NAME=modular_cli.tar.gz
OBFUSCATOR_ARTIFACT_NAME=sre_obfuscator.tar.gz
FIRST_USER=$(getent passwd 1000 | cut -d : -f 1)

case "$1" in
  help|-h|--help) shift; cmd_usage "$@" ;;
  version|--version) shift; cmd_version "$@" ;;
  update) shift; cmd_update "$@" ;;
  init) shift; cmd_init "$@" ;;
  --system|--user) cmd_init "$@" ;;  # redirect to init as default one
  '') shift; cmd_usage ;;
  *) die "$(cmd_unrecognized)" ;;
esac
exit 0

