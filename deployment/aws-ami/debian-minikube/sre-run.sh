#!/bin/bash

set -eo pipefail

get_imds_token() { curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300"; }
get_from_metadata() {
  local token="$2"
  [ -z "$token" ] && token="$(get_imds_token)"
  curl -sf -H "X-aws-ec2-metadata-token: $token" "http://169.254.169.254/latest$1"
}

cf_signal() {
  # first parameter is either "SUCCESS" or "FAILURE". The second one is stack name
  local url query region instance_id doc sig token
  declare -A region_to_endpoint
  region_to_endpoint["eu-isoe-west-1"]="https://cloudformation.eu-isoe-west-1.cloud.adc-e.uk/"
  region_to_endpoint["us-iso-east-1"]="https://cloudformation.us-iso-east-1.c2s.ic.gov/"
  region_to_endpoint["us-iso-west-1"]="https://cloudformation.us-iso-west-1.c2s.ic.gov/"
  region_to_endpoint["us-isob-east-1"]="https://cloudformation.us-isob-east-1.sc2s.sgov.gov/"
  region_to_endpoint["us-isof-east-1"]="https://cloudformation.us-isof-east-1.csp.hci.ic.gov/"
  region_to_endpoint["us-isof-south-1"]="https://cloudformation.us-isof-south-1.csp.hci.ic.gov/"
  region_to_endpoint["cn-north-1"]="https://cloudformation.cn-north-1.amazonaws.com.cn/"
  region_to_endpoint["cn-northwest-1"]="https://cloudformation.cn-northwest-1.amazonaws.com.cn/"

  token="$(get_imds_token)"

  region="$(get_from_metadata "/dynamic/instance-identity/document" "$token" | jq -r ".region")"
  doc="$(get_from_metadata "/dynamic/instance-identity/document" "$token" | base64 -w 0)"
  sig="$(get_from_metadata "/dynamic/instance-identity/signature" "$token" | tr -d '\n')"
  instance_id="$(get_from_metadata "/meta-data/instance-id" "$token")"

  if [ -n "${region_to_endpoint[$region]}" ]; then
    url="${region_to_endpoint[$region]}"
  else
    url="https://cloudformation.$region.amazonaws.com/"
  fi
  query="Action=SignalResource&LogicalResourceId=SyndicateRuleEngineInstance&StackName=$2&UniqueId=$instance_id&Status=$1&ContentType=JSON&Version=2010-05-15"
  curl -sf -X GET --header 'Accept: application/json' --header "Authorization: CFN_V1 $doc:$sig" --header "User-Agent: CloudFormation Tools" "$url?$query"
}

send_cf_signal() {
  if [ -n "$CF_STACK_NAME" ]; then
    log "Sending $1 signal to CloudFormation"
    if ! cf_signal "$1" "$CF_STACK_NAME"; then
      log_err "Failed to send signal to Cloud Formation"
    fi
  else
    log "Not sending signal to Cloud Formation because CF_STACK_NAME is not set"
  fi
}
on_exit() {
  local status=$?
  [ "$status" -ne 0 ] && send_cf_signal "FAILURE"
}
trap on_exit EXIT

# here we load possible envs provided from outside. Those are explained in README.md. No difficult logic must be there
if user_data="$(get_from_metadata /user-data/)"; then
  # shellcheck disable=SC1090
  source <(echo "$user_data")
fi

# these can be provided from outside
export GITHUB_REPO="${GITHUB_REPO:-epam/syndicate-rule-engine}"
export SRE_LOCAL_PATH="${SRE_LOCAL_PATH:-/usr/local/sre}"
export LOG_PATH="${LOG_PATH:-/var/log/sre-init.log}"
export FIRST_USER="${FIRST_USER:-$(getent passwd 1000 | cut -d: -f1)}"

log() { echo "[INFO] $(date) $1" >>"$LOG_PATH"; }
log_err() { echo "[ERROR] $(date) $1" >>"$LOG_PATH"; }

if [ -f "$SRE_LOCAL_PATH/.success" ]; then
  log "Rule Engine was already initialized. Skipping"
  exit 0
fi

log "-----------------------------------------------------"
log "Initializing Syndicate Rule Engine for the first time"
log "-----------------------------------------------------"
log "Creating ~/.local/bin for $FIRST_USER" # wish to do it faster than user manages to log in to trigger ~/.profile
sudo -u "$FIRST_USER" mkdir -p "$(getent passwd "$FIRST_USER" | cut -d: -f6)/.local/bin" || true
log "Adding user $FIRST_USER to docker group"
sudo groupadd docker || true
sudo usermod -aG docker "$FIRST_USER" || true

log "Installing jq and curl"
sudo apt update -y && sudo apt install -y jq curl

# this one is required for ami-initialize.sh, so we must resolve it if not provided
log "Going to resolve latest release from Github api"
export RULE_ENGINE_RELEASE="${RULE_ENGINE_RELEASE:-$(curl -fLs "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | jq -r '.tag_name')}"
if [ -z "$RULE_ENGINE_RELEASE" ]; then
  log_err "Could not find latest release"
  exit 1
fi

log "Downloading ami-initialize.sh from release $RULE_ENGINE_RELEASE"
ami_initialize="$(mktemp)"
if ! wget -q -O "$ami_initialize" "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/ami-initialize.sh"; then
  log_err "Failed to download ami-initialize.sh"
  rm "$ami_initialize"
  exit 1
fi

{
  # shellcheck disable=SC1090
  source "$ami_initialize"
} 2>&1 | sudo tee -a "$LOG_PATH" >/dev/null
rm "$ami_initialize"

log "Executing sre-init --system"
# all exported envs will be preserved for sre-init
sudo -EH -u "$FIRST_USER" sre-init --system 2>&1 | sudo tee -a "$LOG_PATH" >/dev/null

send_cf_signal "SUCCESS"
log "Creating $SRE_LOCAL_PATH/.success"
sudo touch "$SRE_LOCAL_PATH/.success"
sudo chmod 000 "$SRE_LOCAL_PATH/.success"
