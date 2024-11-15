# Access Guide

As soon as the AWS CloudFormation stack of EPAM Syndicate Rule Engine changed 
its state to `CREATE_COMPLETE` you can access the product in the following ways:

### Defect Dojo
To access the Defect Dojo Web Interface do the following steps: 
1. Verify if the inbound/outbound HTTPS traffic is allowed to 80 port by the 
Security Group that is applied to the product instance;
2. Open https://INSTANCE-PUBLIC-DNS:80 (replace INSTANCE-PUBLIC-DNS with the actual value) 
in your browser; the Defect Dojo web app will open.  

### CLI
To access the EPAM Syndicate Rule Engine's CLI please follow these steps:
1. Verify if the inbound/outbound SSH traffic is allowed to port 22 by the 
Security Group that is applied to the product instance;
2. Make sure that you have the SSH key used while instance provisioning;
3. Ensure the key file has read-only permission for the owner or file user.
If not, set such permission with the command: `chmod 400 $SSH_KEY_NAME` where 
`$SSH_KEY_NAME` is replaced with the actual ssh key file name. 
4. Connect to the product instance using the SSH key using this command: 
`ssh -i $SSH_KEY_NAME admin@$INSTANCE_PUBLIC_DNS` where:
   - `$SSH_KEY_NAME` is the actual name of the key file;
   - `$INSTANCE_PUBLIC_DNS` is the actual public DNS of the instance.
5. After you successfully log in check if the syndicate CLI is available with the 
command `syndicate --version`.

### API 
Coming Soon.
In case of urgent need please contact [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com)


### Support
In case of any issues please contact [SupportSyndicateTeam@epam.com](mailto:SupportSyndicateTeam@epam.com)