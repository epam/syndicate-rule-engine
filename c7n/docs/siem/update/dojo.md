# dojo

## Description

Updates dojo configuration. When you specify '--product_type_name',
    '--product_name', '--engagement_name', '--test_title', you can use these
    special key-words: 'customer', 'tenant', 'account', 'job_id', 'day_scope',
    'job_scope' inside curly braces to map the entities.
    Example: 'c7n siem add dojo ... --product_name
    "Product {account}: {day_scope}"'

## Synopsis

```bash
c7n siem update dojo
    [--api_key <text>]
    [--clear_existing_mapping <boolean>]
    [--display_all_fields <boolean>]
    [--engagement_name <text>]
    [--host <text>]
    [--product_name <text>]
    [--product_type_name <text>]
    [--resource_per_finding <boolean>]
    [--tenant_name <text>]
    [--test_title <text>]
    [--upload_files <boolean>]
    [--user <text>]
```

## Options

`--api_key` (text) 

DefectDojo API key

`--clear_existing_mapping` (boolean) 

Clear the existing entities mapping configuration so that you can use the default one

`--display_all_fields` (boolean) 

Flag for displaying all fields

`--engagement_name` (text) 

DefectDojo's engagement name. Account name and day's date scope will be used by default: '{account}: {day_scope}'

`--host` (text) 

DefectDojo host:port

`--product_name` (text) 

DefectDojo's product name. Tenant and account names will be used by default: '{tenant} - {account}'

`--product_type_name` (text) 

DefectDojo's product type name. Customer's name will be used by default: '{customer}'

`--resource_per_finding` (boolean) 

Specify if you want each finding to represent a separate violated resource

`--tenant_name` (text) 

Name of related tenant

`--test_title` (text) 

Tests' title name in DefectDojo. Job's date scope and job id will be used by default: '{job_scope}: {job_id}'

`--upload_files` (boolean) 

Flag for displaying a file for each resource with its full description in the "file" field

`--user` (text) [default: admin]

DefectDojo user name


[← update](./index.md)