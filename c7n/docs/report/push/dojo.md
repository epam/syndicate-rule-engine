# dojo

## Description

Pushes job detailed report(s) to the Dojo SIEM

## Synopsis

```bash
c7n report push dojo
    [--from_date <isoparse>]
    [--job_id <text>]
    [--job_type <manual|reactive>]
    [--tenant_name <text>]
    [--to_date <isoparse>]
```

## Options

`--from_date` (isoparse) 

Generate report FROM date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000

`--job_id` (text) 

Unique job identifier. Required if neither `--to_date` or `--from_date` are set.

`--job_type` (manual, reactive) 

Specify type of jobs to retrieve.

`--tenant_name` (text) 

Name of related tenant

`--to_date` (isoparse) 

Generate report TILL date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000


[← push](./index.md)