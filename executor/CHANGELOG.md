# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


# [4.1.0] - 2023-08-11
* removed `report_fields`, `severity` and `standard_points` from findings
* get impact, article, remediation for dojo reports from meta

# [4.0.0] - 2023-02-03 - 2023-03-07
- removed CaaSAccounts and all the bound code. 
  Now Tenants are used instead of accounts.

# [3.3.1] 2023-01-29 - 2023-02-03
- add a missing attribute to BatchResults;

# [3.3.0] 2022-12-23 - ...
- split event-driven and standard job flows;
- integrate new event-driven and BatchResults;

# [3.2.3] 2022-12-16 - 2022-12-22
- add to statistics the policies that were skipped due to the time threshold
- fix job lifetime threshold (use job's `startedAt` instead of `createdAt`)

# [3.2.2] 2022-12-14 - 2022-12-16
- update notification content;
- replace settings passing through environment variables with settings service

# [3.2.1] 2022-12-09 - 2022-12-13
- make some optimizations:
  - upload all the report files to S3 bucket concurrently;
  - use `libyaml` from C in order to speed up Yaml files loading and dumping;
  - send requests to LM in job_updater_service only if the job is licensed;
  - increase the number of retry attempts in boto3 S3 config;

# [3.2.0] 2022-11-30 - 2022-12-09
- remove PynamoDBToMongoDBAdapter and BaseModel and use the ones from MCDM SDK
- add one more report `user_detailed_report.json`. It's the same 
  as `detailed_report.json` but does not contain standards
- `TARGET_RULESET` env now must contains rulesets' ids instead of names.

# [3.0.0] 2022-09-14 - 2022-11-02
- integrate Maestro common domain model (MCDM) and its SDK;
- Refactored to allow to execute the docker without command but only with envs.
- Refactored the main flow and split to separate classes. Added an ability to 
  execute policies in ThreadPoolExecutor. Set `EXECUTOR_MODE` env to 
  either `consistent` or `concurrent`
- If job item does not exist in DB after the executor has started, 
  it will be created;


## [2.0.0] - 2022-06-24
- Added integration with Custodian License Manager.


## [1.0.0] - 2021-04-08
Initial version of Maestro Custodian Service Docker Image.