
# Rule Engine AMI Testing

This document contains Rule Engine AMI test plan divided into sections starting with the most important cases.


## Verifying the installation

Rule Engine AMI setup currently takes about 8 minutes. Installation logs are written to `/var/log/sre-init.log`. You can 
watch those logs in real time using the command below (in case the setup is still in progress, and you want to see what's happening).

```bash
tail -f /var/log/sre-init.log
```

When setup is finished a file `/usr/local/sre/success` will be created. So if you see this file, it generally means that 
setup was successful:

```bash
# ls /usr/local/sre/.success
test -f /usr/local/sre/.success && echo "Success" || echo "Fail"
```

When setup is finished you must have three cli entrypoints available: `syndicate`, `sreobf`, `sre-init`. 
Consider that you may need to re-login your linux session to be able to use those CLIs:

**Main entrypoint to Rule Engine and Modular Service APIs:**

```bash
syndicate version
```

**Obfuscation manager for Rule Engine reports:**

```bash
sreobf --help
```

**Auxiliary tool for initializing, updating and making backups**

```bash
sre-init version
```

In addition, Rule Engine AMI installation must have License with rulesets and License manager keys. 
If either is missing, something went wrong. Verify using commands below. All responses must contain data.

**License exists:**

```bash
syndicate re license describe --json
```

**Rulesets exist:**

```bash
syndicate re ruleset describe --json
```

**Private key exists:**

```bash
syndicate re setting lm client describe --json
```

**License Manager link configured:**

```bash
syndicate re setting lm config describe --json
```

You must understand that `syndicate` is actually `modular-cli` entrypoint that interacts with `modular-api` server running 
inside container. Besides `modular-api` there is also `rule-engine` server, `modular-service` server, `defectdojo` server 
and other microservices that are required by the main ones. The whole list of deployments should look like this:

```bash
$ kubectl get deployment
NAME                      READY   UP-TO-DATE   AVAILABLE   AGE
defectdojo-celerybeat     1/1     1            1           38m
defectdojo-celeryworker   1/1     1            1           38m
defectdojo-nginx          1/1     1            1           38m
defectdojo-postgres       1/1     1            1           38m
defectdojo-redis          1/1     1            1           38m
defectdojo-uwsgi          1/1     1            1           38m
minio                     1/1     1            1           38m
modular-api               1/1     1            1           38m
modular-service           1/1     1            1           38m
mongo                     1/1     1            1           38m
rule-engine               1/1     1            1           38m
vault                     1/1     1            1           38m
```

By default, only `defectdojo`, `modular-api` and `minio` services should be exposed to localhost. You can verify this 
by using the next command:

```bash
$ kubectl get service | grep NodePort
defectdojo            NodePort    10.99.147.42     <none>        8080:32107/TCP   41m
minio                 NodePort    10.102.200.247   <none>        9000:32102/TCP   41m
modular-api           NodePort    10.105.222.253   <none>        8085:32105/TCP   41m
```
**Note:** minio service is exposed because there are use cases where we need to download files using minio presigned urls.

The instance must have nginx server configured to access those three services. You can see the sites that are enabled 
for nginx by listing `/etc/nginx/sites-enabled`:

```bash
ls /etc/nginx/sites-enabled
```

The same information can be displayed by using `sre-init`. Be sure that it does no magic but just helps with cumbersome 
tasks. Command to see sites available by nginx:

```bash
sre-init nginx ls
```

So `defectdojo` UI must be available on `http://<ipv4>`. System password can be retrieved using the command below (username is `admin`):
```bash
kubectl get secret defectdojo-secret -o jsonpath='{.data.system-password}' | base64 -d
```


**Advanced (can be skipped):**

All nginx sites are inside `/etc/nginx/sites-available`. There are others that can be exposed. If you want, say, 
to expose Rule Engine and Modular service APIs you should follow the steps:

Expose the corresponding services from Kubernetes by changing service type to `NodePort`

```bash
helm upgrade --reuse-values --set modular-service.service.type=NodePort,service.type=NodePort rule-engine syndicate/rule-engine --version "$(helm get metadata rule-engine -o json | jq -r '.version')"
```

Enable corresponding nginx site:

```bash
sudo ln -s /etc/nginx/sites-available/sre /etc/nginx/sites-enabled/
```

Reload nginx

```bash
sudo nginx -s reload
```

After these steps Rule Engine and Modular Service APIs must be available on 8000 port of instance's public ip. Try 
visiting `http://<ipv4>:8000/re/doc` and `http://<ipv4>:8000/ms/doc`


## Functionality testing

After setup admin users for Rule Engine API and Modular Service API must be configured. Verify by executing commands:

**Rule Engine user**

```bash
syndicate re whoami
```

One customer `Marketplace <account id>` and one tenant within that customer must be created. The tenant 
represents AWS account where instance is launched.

**Describe customer:**

```bash
syndicate re customer describe
```

**Describe tenant:**

```bash
syndicate re tenant describe
```

In case instance profile is configured and hop limit is more than 2 you can submit the job (otherwise specify credentials manually):

```bash
syndicate re job submit --tenant_name TENANT_1 --region eu-west-1  # or specify your region
```

The job will be `RUNNING` for some time. When its status becomes `SUCCEEDED` you verify that findings appeared on Dojo 
UI (`http://<ipv4>`). Also try generating some report:

```bash
syndicate re report resource latest --tenant_name TENANT_1 --format xlsx --json
```

## sre-init testing

`sre-init` is just a bash script that provides convenient ways of perform some tasks. Currently, it can help with updating the 
installation, making backups and configuring Rule Engine for other linux users.

Say you want to configure `syndicate` for another linux user called `margaret`. Use such command from user that have sudo. 
It will create linux user if it does not exist yet, add ssh key to `~margaret/.ssh/authorized_keys` and install CLIs for the new user.

```bash
sre-init init --user margaret --public-ssh-key "<user's ssh key to allow him to login>"
```
**Note:** this command allows to specify other parameter such as `--re-username` and `--re-password`. You can create Rule Engine 
and/or Modular Service users using your first user and then specify their credentials for this command.

Another `sre-init` feature is backups management. To see available backups use the next command:

```bash
sre-init backup ls
```

Create new backup by using the command below. Specify k8s volumes you want to back up

```bash
sre-init backup create --name test-backup --volumes=minio,mongo
```

Now you can create some new entities, maybe try submitting new job. Do anything because after restoring the backup 
the state should be the same when the backup was created

Now restore the backup:

```bash
sre-init backup restore --name test-backup
```
**Note:** you may need to wait some time after restoring till `mongo` container is restarted


## Obfuscation manager testing


## Troubleshooting
