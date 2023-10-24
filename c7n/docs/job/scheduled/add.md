# add

## Description

Registers a scheduled job

## Synopsis

```bash
c7n job scheduled add
    --schedule <text>
    [--name <text>]
    [--region <text>]
    [--ruleset <text>]
    [--tenant_name <text>]
```

## Options

`--schedule` (text) 

Cron or Rate expression: cron(0 20 * * *), rate(2 minutes)

`--name` (text) 

Name for the scheduled job. Must be unique. If not given, will be generated automatically

`--region` (text) 

Regions to scan. If not specified, all active regions will be used

`--ruleset` (text) 

Rulesets to scan. If not specified, all available rulesets will be used

`--tenant_name` (text) 

Name of related tenant


[← scheduled](./index.md)