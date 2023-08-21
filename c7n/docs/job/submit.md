# submit

## Description

Submits a job to scan an infrastructure

## Synopsis

```bash
c7n job submit
    [--cloud <AWS|AZURE|GOOGLE>]
    [--credentials_from_env <boolean>]
    [--not_check_permission <boolean>]
    [--region <text>]
    [--ruleset <text>]
    [--tenant_name <text>]
```

## Options

`--cloud` (AWS, AZURE, GOOGLE) 

Cloud to scan. Required, if `--credentials_from_env` flag is set.

`--credentials_from_env` (boolean) 

Specify to get credentials for scan from environment variables. Requires `--cloud` to be set.

`--not_check_permission` (boolean) 

Force the server not to check execution permissions. Job that is not permitted but has started will eventually fail

`--region` (text) 

Regions to scan. If not specified, all active regions will be used

`--ruleset` (text) 

Rulesets to scan. If not specified, all available by license rulesets will be used

`--tenant_name` (text) 

Name of related tenant


[← job](./index.md)