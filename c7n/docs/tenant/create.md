# create

## Description

Activates a tenant, if the environment does not restrict it.

## Synopsis

```bash
c7n tenant create
    --account_number <text>
    --cloud <AWS|AZURE|GOOGLE>
    --tenant_name <text>
    [--default_owner <text>]
    [--display_name <text>]
    [--primary_contacts <text>]
    [--secondary_contacts <text>]
    [--tenant_manager_contacts <text>]
```

## Options

`--account_number` (text) 

Cloud native account identifier

`--cloud` (AWS, AZURE, GOOGLE) 

Cloud of the tenant

`--tenant_name` (text) 

Name of the tenant

`--default_owner` (text) 

Owner email

`--display_name` (text) 

Tenant display name. If not specified, the value from --name is used

`--primary_contacts` (text) 

Primary emails

`--secondary_contacts` (text) 

Secondary emails

`--tenant_manager_contacts` (text) 

Tenant manager emails


[← tenant](./index.md)