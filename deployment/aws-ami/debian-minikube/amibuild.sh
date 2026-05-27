#!/bin/bash

sudo chmod +x /usr/local/bin/sre-run.sh

sudo systemctl daemon-reload
sudo systemctl enable sre-run.service
