# add

## Description

Creates Event-driven ruleset with all the rules

## Synopsis

```bash
c7n ruleset eventdriven add
    --cloud <AWS|AZURE|GCP>
    --name <text>
    [--rule_id <text>]
    [--rule_version <text>]
    [--version <float>]
```

## Options

`--cloud` (AWS, AZURE, GCP) 

Ruleset cloud

`--name` (text) 

Ruleset name

`--rule_id` (text) 

Rule ids to attach to the ruleset

`--rule_version` (text) 

Rule version to choose in case of duplication (the highest version by default). Used with --full_cloud or --standard flags

`--version` (float) [default: 1.0]

Ruleset version


[← eventdriven](./index.md)