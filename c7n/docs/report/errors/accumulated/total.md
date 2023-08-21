# total

## Description

Describes job error reports

## Synopsis

```bash
c7n report errors accumulated total
    [--format <json|xlsx>]
    [--from_date <isoparse>]
    [--href <boolean>]
    [--job_type <manual|reactive>]
    [--tenant_name <text>]
    [--to_date <isoparse>]
```

## Options

`--format` (json, xlsx) 

Format of the file within the hypertext reference

`--from_date` (isoparse) 

Generate report FROM date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000

`--href` (boolean) 

Return hypertext reference

`--job_type` (manual, reactive) 

Specify type of jobs to retrieve.

`--tenant_name` (text) 

Name of related tenant

`--to_date` (isoparse) 

Generate report TILL date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000


[← accumulated](./index.md)