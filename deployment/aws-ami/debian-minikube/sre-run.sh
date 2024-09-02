#!/bin/bash

# shellcheck disable=SC2034
LM_API_LINK="https://lm.api.link"
GITHUB_REPO=epam/syndicate-rule-engine
FIRST_USER=$(getent passwd 1000 | cut -d : -f 1)
SRE_LOCAL_PATH=/usr/local/sre
LOG_PATH=/var/log/sre-init.log

log() { echo "[INFO] $(date) $1" >> $LOG_PATH; }

if [ -f $SRE_LOCAL_PATH/success ]; then
  log "Rule Engine was already initialized. Skipping"
  exit 0
fi

log "Installing jq and curl"
sudo apt update -y && sudo apt install -y jq curl

RULE_ENGINE_RELEASE="$(curl -fLs "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | jq -r '.tag_name')"
if [ -z "$RULE_ENGINE_RELEASE" ]; then
  log "Could not find latest release"
  exit 1
fi

log "Executing ami-initialize from release $RULE_ENGINE_RELEASE"
# shellcheck disable=SC1090
source <(wget -O - "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/ami-initialize.sh")

# will be downloaded by line above
log "Executing sre-init --system"
sudo -u "$FIRST_USER" sre-init --system | sudo tee -a $LOG_PATH >/dev/null

log "Creating $SRE_LOCAL_PATH/success"
sudo touch $SRE_LOCAL_PATH/success
sudo chmod 000 $SRE_LOCAL_PATH/success