# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [5.7.0] - 2025-01-30
- added `--new_version` to ruleset update

## [5.6.0] - 2025-01-21
- fix `sre platform k8s delete`
- add help messages for `sre platform k8s ...`

## [5.5.0] - 2024-09-02
- change job submit command to resolve credentials for tenant from all available places
- add `--platform`, `--category`, `--service_section` and `--source` fields to `sre ruleset add`

## [5.4.1] - 2024-08-06
- fix `inappropriate ioctl for device`
- add environment variables: `SRE_CLI_RESPONSE_FORMAT`, `SRE_CLI_VERBOSE`, `SRE_CLI_NO_PROMPT`

## [5.4.0] - 2024-07-09
- renamed `c7n` entrypoint to `sre`
- add 1 exit codes for all commands that failed
- added `license_key` optional parameter to `c7n job submit`
- removed `--description` from `c7n license add`
- added `--rule_source_id` to `c7n rule describe`
- removed `--active` from `c7n ruleset add`
- added `sre ruleset release` command
- added `sre rulesource sync` command

## [5.3.0] - 2024-06-09
- added commands to manage Chronicle instance:
  - `c7n integrations chronicle add`
  - `c7n integrations chronicle describe`
  - `c7n integrations chronicle delete`
  - `c7n integrations chronicle activate`
  - `c7n integrations chronicle deactivate`
  - `c7n integrations chronicle get_activation`
  - `c7n report push chronicle`

## [5.1.2] - 2024-06-03
- made `c7n meta update_mappings` deprecated

## [5.1.1] - 2024-05-13
- fix errors formatting for modular
- add Cli command to change user password

## [5.1.0] - 2024-04-17
- added `--effect` and `--tenant` and `--description` to `c7n policy add|update`
- added `--description` to `c7n role add|update`
- added `--href` param to `c7n report resource job`

## [5.0.2] - 2024-04-02
- add `--customer_id` hidden param to `c7n users ...`

## [5.0.0] - 2024-02-19
- get `trace_id` from headers instead of body
- refactor, minor optimization
- adapt response formatters to new Server responses (`data`, `items`, `errors`)
- Removed commands:
  - security hub
  - backupper
  - `c7n tenant update`
  - `c7n application *`
  - `c7n parent *`
  - `c7n siem *`
- Added commands:
  - `c7n customer set_excluded_rules`
  - `c7n customer get_excluded_rules`
  - `c7n tenant delete`
  - `c7n tenant active_licenses`
  - `c7n tenant set_excluded_rules`
  - `c7n tenant get_excluded_rules`
  - `c7n integrations dojo add`
  - `c7n integrations dojo describe`
  - `c7n integrations dojo delete`
  - `c7n integrations dojo activate`
  - `c7n integrations dojo deactivate`
  - `c7n integrations dojo get_activation`
  - `c7n integrations sre add`
  - `c7n integrations sre describe`
  - `c7n integrations sre delete`
  - `c7n integrations sre update`
  - `c7n license activate`
  - `c7n license deactivate`
  - `c7n license get_activation`
  - `c7n license update_activation`
  - `c7n tenant credentials *`
  - `c7n whoami`


## [4.19.0] - 2023-12-22
- added new command `c7n report status`

## [4.18.0] - 2023-12-20
* rename some parameters in the commands `c7n metrics status` and `c7n job describe`:
  * `--from` to `--from_date`
  * `--to` to `--to_date`
* added new command `c7n report status`

## [4.17.1] - 2023-12-13
* added parameters `--from` and `--to` for the command `c7n metrics status`

## [4.17.0] - 2023-12-08
* added new commands:
  * `c7n setting report enable_sending`
  * `c7n setting report disable_sending`

## [4.16.1] - 2023-12-05
* made `--tenant_name` parameter in the `c7n report operational` command multiple

## [4.16.0] - 2023-11-27
* added new report command:
  * `c7n report diagnostic`
* removed some obsolete cli commands

## [4.15.1] - 2023-11-16
* added `KUBERNETES` report type to the `c7n report operational` command

## [4.15.0] - 2023-10-16
- added new commands to manage k8s platforms:
  - `c7n platform k8s create_eks`
  - `c7n platform k8s create_native`
  - `c7n platform k8s delete_eks`
  - `c7n platform k8s delete_native`
  - `c7n platform k8s describe`


## [4.14.0] - 2023-10-04
* Update libraries to support Python 3.10
  * tabulate from 0.8.9 to 0.9.0
  * requests from 2.27.1 to 2.31.0
* Make python3.10 a min required version

## [4.13.2] - 2023-10-04
* added `FINOPS` report type to the commands:
  * `c7n report operational`
  * `c7n report project`

## [4.13.1] - 2023-09-28
* changed format of the `--report_types` parameter at the time of sending the request in commands:
  * `c7n report operational`
  * `c7n report project`
  * `c7n report department`
  * `c7n report clevel`

## [4.13.0] - 2023-09-28
* made the `--report_types` parameter multiple in commands:
  * `c7n report operational`
  * `c7n report project`
  * `c7n report department`
  * `c7n report clevel`

## [4.12.0] - 2023-09-05
- added `c7n report resource latest` command to allow to retrieve resource 
  specific info
- added `c7n report resource jobs` command
- added `c7n report resource job`

## [4.11.0] - 2023-09-01
- added `c7n meta udpate_mappings`, `c7n meta update_standards`, 
  `c7n meta update_meta` commands

## [4.10.1] - 2023-08-23
- rename `--tenants` to '--tenant' in `c7n user signup` command
- update some descriptions

## [4.10.0] - 2023-08-23
- add `--api_version` param to `c7n setting lm config add`

## [4.9.3] - 2023-08-15
- fix typo in constant name (from `RULES` to `RULE`) in the `c7n report operational` command

## [4.9.2] - 2023-08-10
- added `--raw` flag to `c7n tenant findings describe` command

## [4.9.0] - 2023-08-10
- added parameter `--description` to the `c7n rulesource add`, `c7n rulesource update` commands
- updated `c7n rulesource add` command:
  - made parameter `--git_access_secret` optional;
  - removed default value for the parameter `--git_url`;
  - changed default value for the parameter `--git_ref` from `master` to `main`;
- updated `c7n rule describe` command:
  - renamed parameter `--rule_id` to `--rule_name`;
  - changed alias from `-gpid` to `-pid` for the parameter `--git_project_id`;
  - added optional `--git_ref` parameter;
- updated `c7n rule update` command:
  - removed `--git_project_id` and `--all` parameters;
- updated `c7n rule delete` command:
  - added `--cloud`, `--git_project_id`, `--git_ref` parameters;
  - renamed parameter `--rule_id` to `--rule_name`;
- updated `c7n ruleset add` command:
  - added `--git_ref`, `--severity`, `--mitre` parameters;
  - renamed parameter `--rule_id` to `--rule`;
  - removed `--all_rules`, `--rule_version` parameters;
  - made `--standard` parameters multiple;

## [4.7.2] - 2023-08-01
* added commands to manage Defect Dojo applications:
  - `c7n application dojo add`
  - `c7n application dojo delete`
  - `c7n application dojo update`
  - `c7n application dojo describe`

## [4.7.1] - 2023-07-28
* added `--results_storage` attribute to `c7n application access update`, 
`c7n application access add` commands
* remove `--type` parameter from `c7n parent link_tenant` command

## [4.7.0] - 2023-07-27
* Add `CUSTODIAN_ACCESS` parent type to `c7n parent ...` commands
* added `rules_to_scan` parameter to `c7n job submit`. The parameter can 
  accept either string or JSON string or path to a file containing JSON

## [4.6.1] - 2023-07-11
* Integrate `modular-sdk` instead of the usage of `mcdm-sdk`

## [4.6.0] - 2023-07-04
* added command: 
  * `c7n metrics status`
* renamed old command:
  * `c7n trigger metrics_update` to `c7n metrics update`

## [4.5.2] - 2023-07-03
* made `--tenant_name` and `--tenant_display_name` case-insensitive;

## [4.5.1] - 2023-06-26
* fix a bug with m3-modular and custom name for click option;

## [4.5.0] - 2023-05-30
* added new parameter `report_type` to the following commands:
  * `c7n report operational`;
  * `c7n report project`;
  * `c7n report department`;
  * `c7n report clevel`.
* removed `c7n siem` group

## [4.4.1] - 2023-05-25
* hid some secured params from logs
* added commands:
  - c7n application access add 
  - c7n application access delete
  - c7n application access describe
  - c7n application access update
* added `--protocol`, `--stage` attributes to `c7n setting lm config add`

## [4.4.0] - 2023-05-24
* added command to trigger metrics update:t
  - c7n trigger metrics_update
* renamed license-related commands:
  - from `c7n lm describe` to `c7n license describe`;
  - from `c7n lm delete` to `c7n license delete`;
  - from `c7n lm sync` to `c7n license sync`;
* make flag from `--confirm` option in `c7n setting lm config delete` 
  and `c7n setting mail delete` commands;
* remove `--permissions_admin` from `c7n policy add`

## [4.3.0] - 2023-05-22
* added commands to manage rabbitMQ:
  - c7n customer rabbitmq add;
  - c7n customer rabbitmq describe
  - c7n customer rabbitmq delete;

## [4.2.0] - 2023-05-02
* integrate mcdm cli sdk
* added [`-rsid`/`--rule_source_id`] parameter to the following commands:
  * `c7n rulesource` [`describe`|`update`|`delete`]
  * `c7n rule` [`describe`|`update`]
  * `c7n ruleset add`
* removed source specific git parameters within `c7n rulesource` `update` command:
  * [`--git_url`/`-gurl`]
  * [`--git_ref`/`-gref`]
  * [`git_rules_prefix`/`-gprefix`]

## [4.1.4] - 2023-04-28
* removed default name for `--cloud_application_id` option in `c7n application update` and 
`c7n application add` commands

## [4.1.3] - 2023-04-27
* rename `c7n ruleset event_driven` command to `c7n ruleset eventdriven`
* fix _issues_ found by QA team:
  * #52. Updated description of `c7n parent --help` command 
  * #53. Renamed command from the ‘c7n health’ to the ‘c7n health-check’
  * #54. Unified help description for the `c7n lm --help` command
  * #55. Added description of the [`-lk`/`--license_key`] parameter for the `c7n lm sync` command
  * #56. Updated description for the `-u`, `--username` parameter for the `c7n login` command
  * #57. Updated description of the `c7n policy` command
  * #58. Updated parameters of `c7n policy add` command ([`-padm`/`--permissions_admin`] -> [`-admin`/`--admin_permissions`]).
  * #59. Updated parameters of `c7n policy update` command ([`-a`/`--attach_permission`] -> [`-ap`/ `--attach_permission`]; [`-d`/`--detach_permission`] -> [`-dp`/`--detach_permission`])
  * #60. Updated parameters of `c7n results describe` command ([`-s`/`--start_date`] -> [`-from`/`--from_date`]; [`-e`/`--end_date`] -> [`-to`/`--to_date`])
  * #61. Updated description of `c7n results` group.
  * #62. Updated description of `c7n setting lm client` group.
  * #63. Updated parameters of `c7n setting lm client add` command (`--bs64encoded` -> `--b64encoded`)
  * #64. Added parameter [`-kid`/`--key_id`] to the `c7n setting lm client delete` command
  * #65. Set parameter [`-f`/`--format`] as non-required, defaulting to `PEM` for the `c7n setting lm client` [`describe`|`add`] commands
  * #66. Added a required parameter [`-c`/`--confirm`] of `BOOLEAN`[default: `False`] value for the `c7n setting` [`lm config`| `mail`] `delete` commands
  * #67. Updated help of `c7n setting mail add` command.
  * #68. Updated description of `c7n show_config` command
  * #69. Updated description of `c7n siem` group
  * #70. Updated help of `c7n siem describe` command
  * #71. Changed command `configuration_backupper` to `configuration_backup` of `c7n trigger` group
  * #72. Updated description of `c7n user tenants` group
  * #73. Updated parameter description of `c7n user tenants describe` command

## [4.1.2] - 2023-04-26
* removed default argument name for `--customer_id` option
* changed `tenant_name` to `tenant_names` parameter in `c7n report operational` command;
* changed `tenant_display_name` to `tenant_display_names` parameter in `c7n report project` command;
* made `--tenant_names` parameter required for `c7n report operational` command

## [4.1.1] - 2023-04-25
* added missing parameter `name` to the `show_config` command. Required for compatibility with `m3modular`


## [4.1.0] - 2023-04-20
* fix issues found by QA team:
  1. Removed `c7n job submit aws`, `c7n job submit azure`, `c7n job submit google`. Now there is only one command `c7n job submit`  for which you can optionally pass `--cloud` attr in case you need to resolve credentials from envs locally;
  2. -
  3. added `c7n show-config` param to show the current cli configuration. Added option `--items_per_column` . It restricts the number of items in one table column. If it's not set, all the items is show. By default it's not set;
  4. `--complete` is replaced with `--full` in `c7n customer describe`;
  5. unified help in `c7n customer group`;
  6. removed `c7n customer update`;
  7. `--complete` is replaced with `--full` for `c7n tenant describe` command; `--cloud_identifier` is replaced with `--account_number`;
  8. unified help for `c7n tenant` group, fixed description for create command;
  9. `--cloud_identifier` -> `--account_number` , `-cid` -> `-acc`  for all the commands; Unified its description;
  10. unified `c7n tenant credentials` command;
  11. make `--enabled` boolean type for `c7n tenant credentials update` , changed its description;
  12. added an offer to output the result of a command in JSON format it case the resulting table is too huge for the terminal;
  13. `--target_region` renamed to `--region` . Removed `target` from all the commands;
  14. Removed `derived` from description of `c7n tenant findings` group;
  15. updated help for `c7n tenant priority`;
  16. removed `c7n tenant priority` group;
  17. `--customer` is replaced with `--customer_id` , `-cust` with `-cid` for all the avaiable commands; Unified its description;
  18. (point 9);
  19. fix bugs with `c7n tenant create` command;
  20. remove `--send_scan_results` from `c7n tenant update`;
  21. -
  22. `-pid` renamed to `-gpid`, `-url` to `-gurl`, `-ref` to `-gref` , `-prefix` to `-gprefix`, `-secret` to `-gsecret` for `c7n rulesource` group. Removed `--git_access_token` attribute;
  23. unified description for commands in `c7n rule` group;
  24. `-id` renamed to `-rid` for all the commands in `c7n rule` group;
  25. `c7n ruleset`: for standalone installation `ed` is renamed to `event_driven`; For m3modular cli installation `ed` renamed to `eventdriven` due to m3modular architecture restriction;
  26. `--rule` renamed to `--rule_id`, `-r` to `-rid` for `c7n ruleset ed` group. Added missing descriptions;
  27. `--full_cloud` renamed to `--all_rules` for `c7n ruleset add` command. Updated descriptions;
  28. `-l` renamed to `-ls` for `c7n ruleset describe`;
  29. `-a` renamed to `-ar`, `--rules_to_attach` renamed to `--attach_rules`, `-d`  to `-dr`, `--rules_to_detach` renamed to `--detach_rules` for `c7n ruleset update` command;
  30. `c7n job register|deregister|registered|update` moved to a separate group `c7n job scheduled add|delete|describe|update`;
  31. `--target_region` renamed to `--region`, `--target_ruleset` renamed to `--ruleset` ;
  32. updated description for `c7n job scheduled describe` command;
  33. updated description for `-cenv` flag in `c7n job submit` command;
  34. too huge responses handled (see 12);
  35. changed description for `c7n report compliance jobs`;
  36. updated description for all the reports within accumulated group;
  37. `--start_date` renamed to `--from_date`, `--end_date` renamed to `--to_date` for all the report commands;
  38. updated description for the `tenant_display_name` to distinguish from `tenant_name`
  39. updated help description for `c7n report push` command;
  40. updated help description for `c7n report push dojo|security_hub` command;
  41. -
  42. `csv` replaced with `xlsx` for `c7n report rules` group;
  43. -
  44. -
  45. -
  46. -
  47. -
  48. -
  49. updated help description for `c7n application` group;
  50. `--access_application_id` renamed to `--cloud_application_id`  for `c7n application add|update` commands;
  51. `--application_id` is added to commands `c7n application delete`, `c7n application update`;

## [4.0.1] - 2023-04-14
* fix `c7n results describe` for modular


## [4.0.0] - 2023-02-03 - 2023-03-07
* `c7n account findings` moved to `c7n tenant findings`;
* `c7n account credentials` moved to `c7n tenant credentials`;
* removed `--account_name` from `c7n report ... accumulated`;
* removed `--account` from `c7n result describe`;
* added `--complete` flag to `c7n tenant describe`;
* `--allow_tenant_account` replaces with `--allow_tenant` in `c7n ruleset|rulesource`;
* removed `--account` from `c7n job submit`;
* remove `c7n account` group;
* added `c7n application add` and `c7n application describe`, 
  `c7n application update` and `c7n application delete` commands
* added `c7n parent add`, `c7n parent delete`, `c7n parent describe`, 
  `c7n parent link_tenant`, `c7n parent unlink_tenant`, commands
* refactored @cli_response decorator, adjust to m3modular
* `click` downgraded to 7.1.2 in favour of m3modular cli
* refactored api client initialization, refactored Config 
  representation class. Use JSON instead of YAML. Init config and client
  for each individual request to allow multiple users when the cli is used 
  as M3 Module


## [3.4.0] - 2023-01-30 - 2023-02-03
- add an ability to describe tenants by project id - add `cloud_identifier` 
  param to `c7n tenant describe`

## [3.3.0] - 2023-01-16
- add `c7n job event` to simulate event-driven requests, only in developer mode.
- add `c7n result describe` to describe BatchResults;
- rewrite `c7n report`: add 
  - `compliance jobs/accumulated`;
  - `details jobs/accumulated`;
  - `digests jobs/accumulated`;
  - `errors jobs/accumulated`;
  - `rules jobs/accumulated`;


## [3.2.0] - from end of November till now
- add `rule_source_id` to `c7n rule describe`
- replace `c7n siem delete -type [dojo|security_hub]` 
  with `c7n siem delete dojo|security_hub`
- replace `c7n siem describe -type [dojo|security_hub]` 
  with `c7n siem describe dojo|security_hub`
- add `c7n setting mail [describe|add|delete]`
- add `schedule` parameter to `c7n job update`
- remove `--customer` attr from siem-related commands and add `--tenant_name`

## [3.1.0] (hotfix)


## [3.0.0] 2022-09-14 - 2022-11-02
- integrate Maestro common domain model (MCDM) and its SDK;
* Added Click's autocomplete;
* Removed `--start_date` & `--end_date` from `c7n report compliance` and added
  `--account` attr to allow to generate compliance report based on the known 
  account findings;
* Added parameter `--rule_source_id` to `c7n ruleset add` to allow compiling 
  rulesets based on rule-source;
* Added `--limit` and `--next_token` parameters to `c7n job describe` and
  `c7n rule describe` commands to limit displayed records. Default limit is 10.
* Removed `apply_to_tenant` and `apply_to_account` parameters from the following commands:
  * `c7n rule_source add`;
  * `c7n rule_source update`;
  * `c7n ruleset add`;
  * `c7n ruleset update`;
* Added `restrict_tenant_account` parameters to the following commands:
  * `c7n rule_source add`;
  * `c7n rule_source update`;
  * `c7n ruleset add`;
  * `c7n ruleset update`;
* Replaced `remove_tenant` and `remove_acount` parameters with a relational 
  one: 
  *  `exclude_tenant_account`
* Added `send_scan_result` parameter to the following commands:
  * `c7n account activate`;
  * `c7n account update`
* Added commands to manage scheduled jobs:
  * `c7n job register`;
  * `c7n job deregister`;
  * `c7n job registered`;
  * `c7n job update`;
* Added commands to manage user access to tenant:
  * `c7n user tenant assign`;
  * `c7n user tenant unassign`;
  * `c7n user tenant describe`;
* Adjusted date-time output for each CLI user according to their time-zone;
* Added commands to manage license priorities:
  * `c7n tenant priority add`;
  * `c7n tenant priority describe`;
  * `c7n tenant priority update`;
  * `c7n tenant priority delete`;
* Adjusted CLI to the format required for m3-modular;
* Renamed: 
  * `c7n lm license_sync` -> `c7n lm sync`;
  * `c7n rule_source` -> `c7n rulesource`;
  * `c7n account credentials_manager` -> `c7n account credentials`;
  * `c7n health_check` -> `c7n health`;
  * `c7n rule update_rules` -> `c7n rule update`;
* Updated `requests` version to `2.28.1`;
* Added `c7n user signup`;
* Added `c7n user delete`
  
## [2.2.0] - 2022-08-30
* Added the `--verbose` flag to write detailed information to the log file;
* Parse `trace_id` from each lambdas' response;
* Added `--event_driven` attr to `c7n job describe` to be able to retrieve 
  only event-driven jobs. If not specified, standard jobs will be returned
* Changed a little responses format: shorten long lists and dicts in table
  output in order to make the table look neat
* Added new commands:
  * `c7n account findings describe` for respective GET action;
  * `c7n account findings remove` for respective DELETE action.

## [2.1.0] - 2022-08-30
* Add new optional parameters to `c7n job submit aws` to generate temporary AWS credentials from profile in 
  `~/.aws/credentials`:
  * `--profile`: profile name;
  * `--predefined_role_name`: the role from which to perform scans. If not specified, the `CustodianServiceRole` 
  * is used. Using with --profile parameter;
  * `--duration`: temporary credentials lifetime duration in hours. Default value: 2 hours.
* minor fixes in `c7n lm` commands group;
* fix the way we check invalid login and/or password;


## [2.0.0] - 2022-06-24
* Add new parameters to `c7n siem add dojo` and `c7n siem update dojo`:
  * `--product_type_name`: DefectDojo's product type name. Customer's name 
  will be used by default: `{customer}`;
  * `--product_name`: DefectDojo's product name. Tenant and account names will 
  be used by default: `{tenant} - {account}`;
  * `--engagement_name`: DefectDojo's engagement name. Account name and day's 
  date scope will be used by default: `{account}: {day_scope}`;
  * `--test_title`: Tests' title name in DefectDojo. Job's date scope and 
  job id will be used by default: `{job_scope}: {job_id}`
  * `--resource_per_finding`: if the flag is set, each finding will represent a 
  separate violated resource. By default, each finding represents a 
  violated rule with a bunch of resources;
* Rename commands/command groups that consists of several words to snake_case
* Added new commands:
  * `c7n rule_source describe`
  * `c7n rule_source add`
  * `c7n rule_source update`
  * `c7n rule_source delete`
  * `c7n ruleset describe`
  * `c7n ruleset update`
  * `c7n ruleset delete`
  * `c7n ruleset add`
* Removed the following commands:
  * `c7n account ruleset add`
  * `c7n account ruleset delete`
  * `c7n account ruleset describe`
  * `c7n account ruleset update`
  * `c7n tenant ruleset add`
  * `c7n tenant ruleset delete`
  * `c7n tenant ruleset describe`
  * `c7n tenant ruleset update`
  * `c7n customer ruleset add`
  * `c7n customer ruleset delete`
  * `c7n customer ruleset describe`
  * `c7n customer ruleset update`
  * `c7n account rule_source add`
  * `c7n account rule_source delete`
  * `c7n account rule_source describe`
  * `c7n account rule_source update`
  * `c7n tenant rule_source add`
  * `c7n tenant rule_source delete`
  * `c7n tenant rule_source describe`
  * `c7n tenant rule_source update`
  * `c7n customer rule_source add`
  * `c7n customer rule_source delete`
  * `c7n customer rule_source describe`
  * `c7n customer rule_source update`

## [1.3.0] - 2022-06-20
* Add an ability to specify `--job_id` parameter in commands 
  `c7n report compliance`, `c7n report error`, `c7n report rule` in order 
  to get reports for a specific job;

## [1.2.0] - 2022-05-26
* Split the `c7n report push` command into two, each for the siem type:
  * `c7n report push dojo`
  * `c7n report push security-hub`
* Provided optional aws attributes to the `c7n report push security-hub` command:
  * `-ak`, `--aws_access_key`
  * `-sk`, `--aws_secret_access_key`
  * `-st`, `--aws_session_token`
* Provided compatibility for both aforementioned commands in the `AdapterClient`
* Made `--customer` attribute not required in siem-manager commands

## [1.1.0] - 2022-05-13
* Made `--customer` attribute not required in most commands and changed some 
its help-messages;
* Made `c7n --version` commands available;

## [1.0.0] - 2022-01-14
Initial release 
