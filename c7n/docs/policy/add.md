# add

## Description

Creates a Custodian Service policy for a customer

## Synopsis

```bash
c7n policy add
    --policy_name <text>
    [--admin_permissions <boolean>]
    [--path_to_permissions <text>]
    [--permission <text>]
```

## Options

`--policy_name` (text) 

Policy name to create

`--admin_permissions` (boolean) 

Adds all admin permissions

`--path_to_permissions` (text) 

Local path to .json file that contains list of permissions to attach to the policy

`--permission` (text) 

List of permissions to attach to the policy


[← policy](./index.md)