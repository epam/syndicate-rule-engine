

## Creating AMI ami

**Start EC2 instance and log in**

Instance must have `Debian 12` OS and `ARM` architecture, though `x64` should work as well as long as 
corresponding docker images are built. Select at least 16Gb for storage (depends on your further workload). Other settings
don't matter here


**Create entrypoint script**

Create `sre-run.sh` (name does not matter) script by some path, say `/usr/local/bin/` and put the content of [sre/deployment/aws-ami/debian-minikube/sre-run.sh](./sre-run.sh) there:

```bash
cat <<EOF | sudo tee /usr/local/bin/sre-run.sh > /dev/null
#!/bin/bash

LM_API_LINK="https://lm.api.link"  # just example link
GITHUB_REPO=epam/syndicate-rule-engine

...

EOF
```

**Note:** specify a valid link to the licence manager. Other parameters also can be adjusted but keep the default
if you don't know what you are doing.

**Allow to execute that script for root:**

```bash
sudo chmod 750 /usr/local/bin/sre-run.sh
```

**Create systemd service**

The service will execute the script above when the instance is starting

```bash
cat <<EOF | sudo tee /etc/systemd/system/sre-run.service > /dev/null
[Unit]
Description=sre-run script running at bootstrap
 
[Service]
ExecStart=/usr/local/bin/sre-run.sh
 
[Install]
WantedBy=multi-user.target
EOF
```

**Enable the created service:**

```bash
sudo systemctl enable sre-run.service
```


**Create AMI from the instance**

Log out and create AWS AMI image from the instance. Terminate the instance when AMI becomes `Available`.
