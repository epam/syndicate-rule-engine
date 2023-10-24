# accumulated

## Description

Describes tenant-specific detailed reports, based on relevant jobs

## Synopsis

```bash
c7n report details accumulated
    [--from_date <isoparse>]
    [--href <boolean>]
    [--job_type <manual|reactive>]
    [--tenant_name <text>]
    [--to_date <isoparse>]
```

## Options

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


[← details](./index.md)