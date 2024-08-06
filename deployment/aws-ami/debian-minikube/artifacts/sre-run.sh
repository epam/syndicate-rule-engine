#!/bin/bash

LM_API_LINK="https://lm.api.link"
GITHUB_REPO=epam/ecc
FIRST_USER=$(getent passwd 1000 | cut -d : -f 1)
SRE_LOCAL_PATH=/usr/local/sre

if [ -f $SRE_LOCAL_PATH/success ]; then
  exit 0
fi

get_latest_release_tag() {
  curl -fL "https://api.github.com/repos/$GITHUB_REPO/releases/latest" | jq -r '.tag_name'
}

sudo apt update -y && sudo apt install -y jq curl

RULE_ENGINE_RELEASE="$(get_latest_release_tag)"
if [ -z "$RULE_ENGINE_RELEASE" ]; then
  exit 1
fi


source <(wget -O - "https://github.com/epam/ecc/releases/download/$RULE_ENGINE_RELEASE/ami-initialize.sh")

# will be downloaded by line above
sudo -u "$FIRST_USER" sre-init --system

sudo touch $SRE_LOCAL_PATH/success
sudo chmod 000 $SRE_LOCAL_PATH/success