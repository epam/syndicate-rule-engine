# update

## Description

Updates rule source

## Synopsis

```bash
c7n rulesource update
    --git_project_id <text>
    [--allow_tenant <text>]
    [--git_access_secret <text>]
    [--git_ref <text>]
    [--git_rules_prefix <text>]
    [--git_url <text>]
    [--restrict_tenant <text>]
```

## Options

`--git_project_id` (text) 

GitLab Project id

`--allow_tenant` (text) 

Allow ruleset for tenant. Your user must have access to tenant

`--git_access_secret` (text) 

None

`--git_ref` (text) 

Name of the branch to grab rules from.

`--git_rules_prefix` (text) 

Rules path prefix.

`--git_url` (text) 

Link to GitLab repository with c7n rules

`--restrict_tenant` (text) 

Restrict ruleset for tenant. Your user must have access to tenant


[← rulesource](./index.md)