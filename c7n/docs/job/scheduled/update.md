# update

## Description

Updates an existing scheduled job

## Synopsis

```bash
c7n job scheduled update
    --name <text>
    [--enabled <boolean>]
    [--schedule <text>]
```

## Options

`--name` (text) 

Scheduled job name to update

`--enabled` (boolean) 

Param to enable or disable the job temporarily

`--schedule` (text) 

Cron or Rate expression: cron(0 20 * * *), rate(2 minutes)


[← scheduled](./index.md)