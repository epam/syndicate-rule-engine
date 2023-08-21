# add

## Description

Creates Customers ruleset.

## Synopsis

```bash
c7n ruleset add
    --cloud <AWS|AZURE|GCP>
    --name <text>
    [--active <boolean>]
    [--all_rules <boolean>]
    [--allow_tenant <text>]
    [--git_project_id <text>]
    [--rule_id <text>]
    [--rule_version <text>]
    [--service_section <text>]
    [--standard <text>]
    [--version <float>]
```

## Options

`--cloud` (AWS, AZURE, GCP) 

None

`--name` (text) 

Ruleset name

`--active` (boolean) 

Force set ruleset version as active

`--all_rules` (boolean) 

Assemble all available rules for specific cloud provider

`--allow_tenant` (text) 

Allow ruleset for tenant. Your user must have access to tenant

`--git_project_id` (text) 

Git project id to build the ruleset

`--rule_id` (text) 

Rule ids to attach to the ruleset. Multiple ids can be specified

`--rule_version` (text) 

Rule version to choose in case of duplication (the highest version by default). Used with --full_cloud or --standard flags

`--service_section` (text) 

Filter rules by the service section

`--standard` (text) 

Filter rules by the security standard name

`--version` (float) [default: 1.0]

None


[← ruleset](./index.md)