# add

## Description

Creates a CUSTODIAN parent of a customer

## Synopsis

```bash
c7n parent add
    --cloud <AWS|AZURE|GOOGLE>
    --scope <ALL|SPECIFIC_TENANT>
    [--application_id <text>]
    [--description <text>]
    [--rules_to_exclude <text>]
```

## Options

`--cloud` (AWS, AZURE, GOOGLE) 

Cloud to connect the parent to

`--scope` (ALL, SPECIFIC_TENANT) 

Tenants scope for the parent

`--application_id` (text) 

Id of an application with type CUSTODIAN within your customer. To connect a parent to

`--description` (text) 

Description for the parent

`--rules_to_exclude` (text) 

Rules to exclude for the scope of tenants


[← parent](./index.md)