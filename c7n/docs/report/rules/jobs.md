# jobs

## Description

Describes job-specific rule statistic reports

## Synopsis

```bash
c7n report rules jobs
    --job_id <text>
    [--format <json|xlsx>]
    [--href <boolean>]
    [--job_type <manual|reactive>]
    [--rule <text>]
```

## Options

`--job_id` (text) 

Unique job identifier

`--format` (json, xlsx) 

Format of the file within the hypertext reference

`--href` (boolean) 

Return hypertext reference

`--job_type` (manual, reactive) [default: manual]

Specify type of jobs to retrieve.

`--rule` (text) 

Denotes rule to target


[← rules](./index.md)