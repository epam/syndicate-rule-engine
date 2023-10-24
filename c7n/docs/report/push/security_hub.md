# security_hub

## Description

Pushes job detailed report(s) to the AWS Security Hub SIEM

## Synopsis

```bash
c7n report push security_hub
    [--aws_access_key <text>]
    [--aws_default_region <text>]
    [--aws_secret_access_key <text>]
    [--aws_session_token <text>]
    [--from_date <isoparse>]
    [--job_id <text>]
    [--job_type <manual|reactive>]
    [--tenant_name <text>]
    [--to_date <isoparse>]
```

## Options

`--aws_access_key` (text) 

AWS Account access key

`--aws_default_region` (text) [default: eu-central-1]

AWS Account default region to init a client 

`--aws_secret_access_key` (text) 

AWS Account secret access key

`--aws_session_token` (text) 

AWS Account session token

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