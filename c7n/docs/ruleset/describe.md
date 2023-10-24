# describe

## Description

Describes Customer rulesets

## Synopsis

```bash
c7n ruleset describe
    [--active <text>]
    [--cloud <AWS|AZURE|GCP>]
    [--get_rules <boolean>]
    [--licensed <boolean>]
    [--name <text>]
    [--version <float>]
```

## Options

`--active` (text) 

Filter only active rulesets

`--cloud` (AWS, AZURE, GCP) 

Cloud name to filter rulesets

`--get_rules` (boolean) 

If specified, ruleset's rules ids will be returned. MAKE SURE to use '--json' flag to get a clear output 

`--licensed` (boolean) 

If True, only licensed rule-sets are returned. If False, only standard rule-sets

`--name` (text) 

Ruleset name

`--version` (float) 

Ruleset version


[← ruleset](./index.md)