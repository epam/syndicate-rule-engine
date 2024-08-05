#!/bin/bash

LM_API_LINK="https://lm.syndicate.team"
RULE_ENGINE_RELEASE=5.4.0
FIRST_USER=$(getent passwd 1000 | cut -d : -f 1)
SRE_LOCAL_PATH=/usr/local/sre

if [ -f $SRE_LOCAL_PATH/success ]; then
  exit 0
fi

source <(wget -O - "https://github.com/epam/ecc/releases/download/$RULE_ENGINE_RELEASE/ami-initialize.sh")

# will be downloaded by line above
sudo -u "$FIRST_USER" sre-init --system

sudo touch $SRE_LOCAL_PATH/success
sudo chmod 000 $SRE_LOCAL_PATH/success