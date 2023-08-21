![Custodian Service logo](../docs/pics/cs_logo.png)

### Custodian Service

The application provides ability to
perform [custodian](https://cloudcustodian.io)
scans for AWS, Azure and GCP accounts.

[Custodian](https://cloudcustodian.io) is a tool for automation cloud security
compliance. It is based on open-source tool Cloud Custodian enhanced by EPAM
Team. The tool allows users to check their infrastructure resources for
compliance to the security policies and standards. Custodian applies the defined
sets of rules against the infrastructure and provides information on the
resources that break the policies. The rulesets are designed specifically for
each of the clouds – AWS, Azure, and GCP.

### Notice

All the technical details described below are actual for the particular version,
or a range of versions of the software.

### Actual for versions: 1.0.0

## Custodian Service diagram

Build docker image (directory custodian-as-a-service/docker) and put it to
   Elastic Container Registry field set to `true`.

## Exit codes
Container can end its execution with the following status codes:
- `0` if code execution completed successfully;
- `2` if it is not allowed to start the job due to the license manager restrictions;
- `126` if retry needed (example - any problems with credentials);
- `1` all other execeptions and errors that do not require a retry.