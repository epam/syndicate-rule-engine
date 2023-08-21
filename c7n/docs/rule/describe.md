# describe

## Description

Describes rules within your customer

## Synopsis

```bash
c7n rule describe
    [--cloud <AWS|AZURE|GCP>]
    [--full <boolean>]
    [--git_project_id <text>]
    [--limit <integer range>]
    [--next_token <text>]
    [--rule_id <text>]
```

## Options

`--cloud` (AWS, AZURE, GCP) 

Display only rules of specific cloud.

`--full` (boolean) 

Show full command output

`--git_project_id` (text) 

Filter rules by git project id

`--limit` (integer range) [default: 10]

Number of records to show

`--next_token` (text) 

Token to start record-pagination from

`--rule_id` (text) 

Rule id to describe


[← rule](./index.md)