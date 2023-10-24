# describe

## Description

Describes results of Custodian Service reactive, batched scans

## Synopsis

```bash
c7n results describe
    [--batch_result_id <text>]
    [--from_date <isoparse>]
    [--limit <integer range>]
    [--next_token <text>]
    [--tenant_name <text>]
    [--to_date <isoparse>]
```

## Options

`--batch_result_id` (text) 

Batch Result identifier to describe by

`--from_date` (isoparse) 

Obtain batched-results FROM date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000

`--limit` (integer range) [default: 10]

Number of records to show

`--next_token` (text) 

Token to start record-pagination from

`--tenant_name` (text) 

Name of related tenant

`--to_date` (isoparse) 

Obtain batched-results TILL date. ISO 8601 format. Example: 2021-09-22T00:00:00.000000


[← results](./index.md)