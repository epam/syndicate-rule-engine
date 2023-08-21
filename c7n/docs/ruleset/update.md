# update

## Description

Updates Customers ruleset.

## Synopsis

```bash
c7n ruleset update
    --name <text>
    --version <float>
    [--active <boolean>]
    [--allow_tenant <text>]
    [--attach_rules <text>]
    [--detach_rules <text>]
    [--restrict_tenant <text>]
```

## Options

`--name` (text) 

Ruleset name

`--version` (float) 

Ruleset version

`--active` (boolean) 

Force set/unset ruleset version as active

`--allow_tenant` (text) 

Allow ruleset for tenant. Your user must have access to tenant

`--attach_rules` (text) 

Rule ids to attach to the ruleset. Multiple values allowed

`--detach_rules` (text) 

Rule ids to detach from the ruleset. Multiple values allowed

`--restrict_tenant` (text) 

Restrict ruleset for tenant. Your user must have access to tenant


[← ruleset](./index.md)