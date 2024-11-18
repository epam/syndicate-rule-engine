# EPAM Syndicate Rule Engine provisioning via AWS CloudFormation

AWS CloudFormation is a recommended way to provision EPAM Syndicate Rule Engine instance. 
It provides ability to specify start parameters for the instance and make it clear and easy to get the configuration outcome.

Please follow these steps to get the ready-to-use product.

## 1. AWS Marketplace Subscription
Subscribe to EPAM Syndicate Rule Engine in AWS Marketplace. Once done you will be redirected to AWS CloudFormation Console 
where the following list of parameters should be specified.

## 2. Specify AWS CloudFormation stack parameters

Required parameters:
- **AWS Subnet id:** сhoose a subnet to launch the instance in;
- **AWS Security groups ids:** specify security groups to be attached to the instance. By default, 
the following ports are used: 80 (Defect dojo), 8085 (API), 9000 (Minio API).
Consider opening these ports if these services should be available outside the instance;
- **EC2 Instance Type:** choose a type for the instance;
- **SSH Key pair name:** choose key pair. Make sure you have access to the selected key. 

Optional parameters:
- **Instance Name:** name for the instance. Stack name will be used as instance name if custom value is not specified;
- **IAM Role Name:** name of AWS IAM Role to be attached to the instance. The role is used by EPAM Syndicate Rule Engine to access accounts resources.
  The service does not mutate resources state by design so the role must be readonly. [Follow this link to access the full list of permissions required by Rules.](https://github.com/epam/ecc-aws-rulepack/tree/main/iam) 
  The role must be created beforehand and must allow EC2 Service to assume it.
  Keep the field empty if you do not want any role to be attached;
- **Tenant Name:** name for the tenant to be activated during service configuration. The 
  tenant represents the AWS Account where the instance is launched. The name can contain ONLY uppercase letters, digits, dashes and underscores.
  If the value is not specified, the default `TENANT_1` will be used;
- **Admin Emails:** list of emails of administrators split by a space. Example: `admin1@yourcorp.com admin2@yourcorp.com`.
  Keep the field empty to not configure any emails.

Click `Next` if all parameters are set.

## 3. Configure stack options

Configure AWS CloudFormation options provided by AWS:

- **Permissions:** specify IAM Role that will be used by AWS CloudFormation to deploy the stack. The role must have 
  permissions to create such AWS resources: EC2 Security Groups, EC2 Launch Templates, EC2 Instance Profiles, EC2 Instances.
  Credentials of the user who deploys the stack are used in case of no Role is specified.
- **Stack failure options:** choose the `Preserve successfully provisioned resources` option. It will ensure that You will
  still be able to access the instance in case the initial setup fails. If such event happens You will be able to access
  initialization logs and share them with EPAM Syndicate Team. If You choose the default value for this option,
  AWS CloudFormation will terminate the instance immediately when the installation fails.
- **Capabilities:** confirm that You `Acknowledge that AWS CloudFormation might create IAM resources.`. It's required
  if You specified IAM Role in the previous section. No custom role will be created but the one You have specified will 
  be attached to the instance profile.

Click `Next` if all options are set.

## 4. Create stack

Review all the parameters and options and click `Submit`

## 5. After the AWS CloudFormation stack creation

The stack will create such resources:
- EC2 Launch Template
- EC2 Instance Profile
- EC2 Instance

The stack will be in `CREATE_IN_PROGRESS` status for as long as it takes the EPAM Syndicate Rule Engine instance be created and initially configured.
Usually it takes up to 10 minutes. 

### In case of success
The `CREATE_COMPLETE` status indicates the creation, configuration and healthcheck are successfully finished and the EPAM Syndicate Rule Engine is ready to be used.
To begin the EPAM Syndicate Rule Engine usage please follow the steps described in the access guide.  

### In case of issues
If something goes wrong during the setup, the AWS CloudFormation stack will change its status to `CREATE_FAILED`.
In case the `Preserve successfully provisioned resources` option was enabled on parameters section it is possible to provide instance initialization log to the EPAM Syndicate Team.

Please follow these steps to download the initialization log file and share it with the support team:
1. Make sure the instance's port 22 is open for your IP address
2. Make sure there is access to the Key Pair file used on instance startup
3. Execute the following command to get the log file from the instance `scp -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS:/var/log/sre-init.log /$YOUR_LOCAL_DIRECTORY/` where:
   - `$SSH_KEY_NAME` is the actual name of the key file;
   - `$INSTANCE_PUBLIC_DNS` is the actual public DNS of the instance;
   - `$YOUR_LOCAL_DIRECTORY` is the path on you local machine where the log file will be saved.
4. After downloading the file the AWS CloudFormation stack can be deleted.
5. Attach the log to the email and send it to [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com) please.


### Support
In case of any issues please contact [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com)
