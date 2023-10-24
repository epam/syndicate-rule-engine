# update

## Description

Updates CUSTODIAN parent of a customer

## Synopsis

```bash
c7n parent update
    --parent_id <text>
    [--cloud <AWS|AZURE|GOOGLE>]
    [--description <text>]
    [--rules_to_exclude <text>]
    [--rules_to_include <text>]
    [--scope <ALL|SPECIFIC_TENANT>]
```

## Options

`--parent_id` (text) 

Parent id to update

`--cloud` (AWS, AZURE, GOOGLE) 

Cloud to connect the parent to

`--description` (text) 

Description for the parent

`--rules_to_exclude` (text) 

Rules to exclude for tenant

`--rules_to_include` (text) 

Rules to include for tenant

`--scope` (ALL, SPECIFIC_TENANT) 

Tenants scope for the parent


[← parent](./index.md)