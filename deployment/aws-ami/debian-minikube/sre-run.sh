#!/bin/bash


# here we load possible envs provided from outside. Those are explained in README.md. No difficult logic must be there
# shellcheck disable=SC1090
_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
source <(curl -s -H "X-aws-ec2-metadata-token: $_TOKEN" http://169.254.169.254/latest/user-data/)

# these can be provided from outside
export GITHUB_REPO="${GITHUB_REPO:-epam/syndicate-rule-engine}"
export SRE_LOCAL_PATH="${SRE_LOCAL_PATH:-/usr/local/sre}"
export LOG_PATH="${LOG_PATH:-/var/log/sre-init.log}"
export FIRST_USER="${FIRST_USER:-$(getent passwd 1000 | cut -d : -f 1)}"


log() { echo "[INFO] $(date) $1" >> "$LOG_PATH"; }

if [ -f "$SRE_LOCAL_PATH/.success" ]; then
  log "Rule Engine was already initialized. Skipping"
  exit 0
fi

log "Installing jq and curl"
sudo apt update -y && sudo apt install -y jq curl

# this one is required for ami-initialize.sh, so we must resolve it if not provided
export RULE_ENGINE_RELEASE="${RULE_ENGINE_RELEASE:-$(curl -fLs "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | jq -r '.tag_name')}"
if [ -z "$RULE_ENGINE_RELEASE" ]; then
  log "Could not find latest release"
  exit 1
fi

log "Executing ami-initialize from release $RULE_ENGINE_RELEASE"
# shellcheck disable=SC1090
source <(wget -O - "https://github.com/$GITHUB_REPO/releases/download/$RULE_ENGINE_RELEASE/ami-initialize.sh")

log "Executing sre-init --system"
# all exported envs will be preserved for sre-init
sudo -EH -u "$FIRST_USER" sre-init --system | sudo tee -a "$LOG_PATH" >/dev/null

log "Creating $SRE_LOCAL_PATH/.success"
sudo touch "$SRE_LOCAL_PATH/.success"
sudo chmod 000 "$SRE_LOCAL_PATH/.success"