
### AMI access


When Syndicate Rule Engine Cloud Formation stack is deployed and has `CREATE_COMPLETE` status you can access Syndicate Rule Engine AMI externally by using its APIs if the 
necessary ports are open by security groups. By default, those are:
- Defect Dojo on port `80`
- Modular API on port `8085`

Also, you can access Syndicate Rule Engine main CLI called `syndicate` and helper cli called `sre-init`.
To access them you must log in to the Syndicate Rule Engine instance using SSH protocol. Make 
sure 22 port is open by security group and perform this command:
```bash
ssh -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS
```

When you successfully logged in to the instance you should be able to use 
such commands:
```bash
syndicate version
```

```bash
sre-init version
```
