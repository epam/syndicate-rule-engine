# describe

## Description

Describes Findings state of a tenant.

## Synopsis

```bash
c7n tenant findings describe
    [--expand <resources>]
    [--get_url <boolean>]
    [--mapped <text>]
    [--region <text>]
    [--resource_type <text>]
    [--rule <text>]
    [--severity <High|Medium|Low|Info>]
    [--subset_targets <boolean>]
    [--tenant_name <text>]
```

## Options

`--expand` (resources) [default: resources]

Expansion parameter to invert Findings collection on.

`--get_url` (boolean) 

Returns a presigned URL rather than a raw Findings collection.

`--mapped` (text) 

Applies mapping format of an expanded Findings collection, by a given key, rather than a listed one.

`--region` (text) 

Region to include in a Findings state.

`--resource_type` (text) 

Resource type to include in a Findings state.

`--rule` (text) 

Rule to include in a Findings state.

`--severity` (High, Medium, Low, Info) 

Severity values to include in a Findings state.

`--subset_targets` (boolean) 

Applies dependent subset inclusion.

`--tenant_name` (text) 

Name of related tenant


[← findings](./index.md)