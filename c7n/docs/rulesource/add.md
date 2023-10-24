# add

## Description

Creates rule source

## Synopsis

```bash
c7n rulesource add
    --git_access_secret <text>
    --git_project_id <text>
    [--allow_tenant <text>]
    [--git_ref <text>]
    [--git_rules_prefix <text>]
    [--git_url <text>]
```

## Options

`--git_access_secret` (text) 

Secret token to be able to access the repository

`--git_project_id` (text) 

GitLab Project id

`--allow_tenant` (text) 

Allow ruleset for tenant. Your user must have access to tenant

`--git_ref` (text) [default: master]

Name of the branch to grab rules from

`--git_rules_prefix` (text) [default: /]

Rules path prefix

`--git_url` (text) [default: https://git.epam.com]

Link to GitLab repository with c7n rules


[← rulesource](./index.md)