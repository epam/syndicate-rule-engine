# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [5.7.0] - 2025-01-14
- moved to modular-sdk 7.0.0
- fixed a bug when ruleset name containing a number was considered to be rulesset version
- added Celery as jobs queue
- refactor such inner reports
  - PROJECT_*
  - DEPARTMENT_*
  - C_LEVEL_*
- added OPERATIONAL_DEPRECATIONS report
- added tests for all reports

## [5.6.0] - 2024-11-05
- refactor metrics collector
- refactor such inner reports
  - OPERATIONAL OVERVIERW
  - OPERATIONAL RESOURCES
  - OPERATIONAL RULES
  - OPERATIONAL FINOPS
  - C LEVEL OVERVIEW
- refactor suc inner reports and metrics
- allow to use metadata provided by the license manager
- write tests for new metrics and existing reports

## [5.6.0b1] - 2024-11-05
- refactor metrics pipeline a bit
- allow maestro reports without license

## [5.5.1] - 2024-10-22
- fixed Internal error if cannot resolve public ip from metadata
- added logic to that validates whether the installation is successful to sre-init
- improved sre-run so that it fails if any command in ami-initialize fails. Also it outputs all logs to log file

## [5.5.0] - 2024-08-07
- return 429 status code if dynamodb provisioned capacity exceeded error
- remove `accN` index from usage
- scan each region in a separate process in order to reduce RAM usage
- change `create_indexes` command. Now it ensures that indexes are up-to-date instead of recreating them
- allow to build rulesets using rule comment field

## [5.4.0] - 2024-07-09
- added `rule_source_id` and `excluded_rules` parameters to `POST /rulestets`.
- added auto version resolving to all the `/rulesets` endpoints. Version parameters is optional
- added `POST /rulestes/release` endpoint to release new rulesets to LM
- added a new type of rulesource - `GITHUB_RELEASE`
- removed `/jobs/standard` endpoint
- refactored a bit

## [5.3.0] - 2024-06-09
- added endpoints for Google Chronicle integration:
  - `POST /integrations/chronicle`
  - `GET /integrations/chronicle`
  - `GET /integrations/chronicle/{id}`
  - `DELETE /integrations/chronicle/{id}`
  - `PUT /integrations/chronicle/{id}/activation`
  - `GET /integrations/chronicle/{id}/activation`
  - `DELETE /integrations/chronicle/{id}/activation`
- added logic that converts shards collection to UDM entities and UDM events
- added an endpoint to push job to Google Chronicle `POST /reports/push/chronicle/{job_id}`
- resolve License Manager API version from LM API

## [5.2.0] - 2024-06-03
- removed `POST /rule-meta/mappings` endpoint
- improve scheduled job cron validator
- Improve ruleset name resolving for `POST /jobs`

## [5.1.3] - 2024-05-23
- fix issue with adding scheduled jobs
- remove rabbitmq check from on-prem health checks. 

## [5.1.2] - 2024-05-21
- treat endpoints with trailing slashes the same as without (/jobs, /jobs/ are equal)

## [5.1.1] - 2024-05-10
- fix users pagination for saas
- fix scheduled expression validation

## [5.1.0] - 2024-04-17
- add `tenant` and `effect` to policy model to allow more flexible policies configurations
- apply tenant restrictions to most endpoints
- refactor `report-generation-handler` slightly
- moved to the latest vault version for k8s and docker
- add `href` param to job resources report. Added `impact`, `remediation` and `article` to that report

## [5.0.3] - 2024-04-09
- added instance profile support in case no credentials for scan can be resolved
- added human-readable errors for jobs

## [5.0.2] - 2024-04-02
- allow users endpoints for standard customers

## [5.0.1] - 2024-02-14
- optimize shards a bit
- add refresh token for onprem

## [5.0.0] - 2024-02-14

- Removed endpoints
  - `POST /tenant/regions` **(replaced with /tenants/{tenant_name}/regions)**
  - `POST /backup` **(obsolete)**
  - `DELETE /license` **(moved to /licenses/{license_key})**
  - `GET /license` **(moved to /licenses/{license_key})**
  - `POST /license` **(moved to /licenses)**
  - `POST /license/sync` **(moved to /licenses/{license_key}/sync)**
  - `GET /reports/digests/tenants/jobs` **(obsolete)**
  - `GET /reports/digests/tenants` **(obsolete)**
  - `GET /reports/digests/tenants/{tenant_name}` **(obsolete)**
  - `GET /reports/details/tenants/jobs` **(obsolete)**
  - `GET /reports/details/tenants` **(obsolete)**
  - `GET /reports/details/tenants/{tenant_name}` **(obsolete)**
  - `POST /reports/push/security-hub/{job_id}` **(obsolete)**
  - `POST /reports/push/security-hub` **(obsolete)**
  - `ANY /applications/dojo` **(moved to /integrations/defect-dojo)**
  - `ANY /applications/dojo/{application_id}` **(moved to /integrations/defect-dojo)**
  - `ANY /applications/access` **(moved to /integrations/temp/sre)**
  - `ANY /applications/access/{application_id}` **(moved to /integrations/temp/sre)**
  - `ANY /applications` **(moved to /licenses)**
  - `ANY /applications/{application_id}` **(moved to /licenses)**
  - `ANY /parents` **(moved to /integrations//activation and under abstraction)***
  - `ANY /parents/{parent_id}` **(moved to /integrations/*/activation and under abstraction)**

- Added endpoints:

  - `POST /refresh` **(allows to refresh access token)**
  - `GET /tenants/{tenant_name}`
  - `DELETE /tenants/{tenant_name}`
  - `POST /tenants/tenant_name}/regions`
  - `GET /tenants/{tenant_name}/active-licenses` **(Get licenses that are active for tenant)**
  - `POST /licenses` **(add license from LM, instead of creating application CUSTODIAN_LICENSES)**
  - `GET /licenses`
  - `GET /licenses/{license_key}`
  - `DELETE /licenses/{license_key}`
  - `POST /licenses/{license_key}/sync`
  - `GET /licenses/{license_key}/activation`
  - `PATCH /licenses/{license_key}/activation`
  - `DELETE /licenses/{license_key}/activation`
  - `PUT /licenses/{license_key}/activation` **(link license to tenants)**
  - `POST /settings/send_reports`
  - `GET /integrations/defect-dojo`
  - `POST /integrations/defect-dojo` **(add defect dojo installation, instead of creating application DEFECT_DOJO)**
  - `GET /integrations/defect-dojo/{id}`
  - `DELETE /integrations/defect-dojo/{id}`
  - `PUT /integrations/defect-dojo/{id}/activation` **(link dojo to tenants)**
  - `GET /integrations/defect-dojo/{id}/activation`
  - `DELETE /integrations/defect-dojo/{id}/activation`
  - `PUT /integrations/temp/sre` **(add application CUSTODIAN and link)**
  - `GET /integrations/temp/sre`
  - `PUT /integrations/temp/sre`
  - `PATCH /integrations/temp/sre`
  - `PUT /tenants/{tenant_name}/excluded-rules` **(set excluded rules for tenant)**
  - `GET /tenants/{tenant_name}/excluded-rules`
  - `PUT /customers/excluded-rules` **(set excluded rules for customer)**
  - `GET /customers/excluded-rules`
  - `GET /doc` **(swagger UI)**
  - `GET /credentials`
  - `GET /credentials/{id}`
  - `GET /credentials/{id}/binding`

- added severity to digest reports
- all endpoints that returned one item in a list or an empty list in case the requested item does not exist 
  (GET /jobs/{job_id}) not return 404 of JSON with one key `data` and the requested object:
  ```json
  {
    "data": {"id": "qwerty", "status": "SUBMITTED"}
  }
  ```
  Generally, all the endpoints that returned one logic item now return it inside `data` key instead of `items`.
- trace_id attribute is returned in headers and removed from body
- Validation errors now returned in such a format:
  ```json
  {"errors": [
    {"location": ["status"], "message": "not available status, choose from: RUNNING, SUBMITTED, ..."},
    {"location": ["limit"], "message": "limit cannot be negative"}
  ]}
  ```
- Added swagger UI to onprem and SAAS, added OpenApi v3 spec generator.
- removed Pycryptodome and other requirements
- moved to Pydantic V2
- added more tests
- used classes with slots instead of dataclasses for performance reasons
- removed `CaaSLicenses`, `CaaSRuleMeta` dynamodb tables
- removed `caas-configuration-updater` & `caas-configuration-backupper` lambdas

## [4.20.0] - 2024-01-22
- fix issue with report type was ignored for all high-level reports.
- correct response models for most actions

## [4.19.4] - 2024-01-26
- upgraded `modular-sdk` version from 4.0.0 to 5.0.0

## [4.19.3] - 2024-01-19
- fixed an issue related to request validation error in case of adding a 
  `parent` without the `--scope` parameter specified
- upgraded `modular-sdk` version from 3.3.10 to 4.0.0
- implemented adding the parameter `sub` of Cognito user to attributes 
  `created_by` and `updated_by` of entities `Parent` and `Application` during related operations
- set the "logs_expiration" parameter for lambdas

## [4.19.2] - 2024-01-16
- fix dates bug in diagnostic reports
- slightly change dates resolving in metrics

## [4.19.1] - 2024-01-11
- add `namespace` for k8s to the list of compulsory report fields
- fixed an issue related to the previous week date definition when the server starts

## [4.19.0] - 2023-12-22
- added `/reports/status` endpoint to retrieve report job status by its ID
- changed response mapping for high-level reports
- added `MAX_RABBITMQ_REQUEST_SIZE` setting

## [4.18.0] - 2023-12-20
- added ability to send big requests with report data to the RabbitMQ

## [4.17.2] - 2023-12-19
- fixed bug in detection duplicates in pending reports
- added `status-index` to the `CaaSReportStatistics` table

## [4.17.1] - 2023-12-14
- upgraded `modular-sdk` version from 3.3.7 to 3.3.10

## [4.17.0] - 2023-12-08
- added fail-safe logic for reports sending;
- added new endpoint `settings/send_reports`;
- added two step-functions: `send_reports` and `retry_send_reports`
- some report endpoints now point to the step functions instead of lambda:
  - `/reports/clevel`
  - `/reports/project`
  - `/reports/operational`
  - `/reports/department`
- added new settings `SEND_REPORT`, `MAX_CRON_NUMBER`(only for onprem)
- remove `docker` folder, move and merge everything with `src`. Change `Dockerfile`
- added a class to resolve Rule comment (https://github.com/epam/ecc-kubernetes-rulepack/wiki/Rule-Index-(Comment)-Structure).

## [4.16.4] - 2023-12-04
- added cache for RabbitMQ transport configuration
- now metrics updater rewrites diagnostic report data with each lambda execution with the appropriate parameters

## [4.16.3] - 2023-12-01
- fixed bug in metrics when both CaaSJobs and CaaSBatchResults items were processed equally despite having 
different parameters  

## [4.16.2] - 2023-11-29
- fixed bug in kubernetes recommendations when all resources were overwritten by the last resource in the list

## [4.16.1] - 2023-11-28
- updated the project attack report to use all sub-techniques and severity
- added all kubernetes findings to the recommendations without binding to severity

## [4.16.0] - 2023-11-24
- added new metrics type - kubernetes
- added new operational level report type - kubernetes
- added ability to archive the metrics of those tenants that have not been scanned for more than a month
- added kubernetes clusters recommendations
- added `/reports/diagnostic` endpoint
- changed s3 files format and paths. Now data is divided into shards
- removed not used and obsolete code
- added resources report. Added xlsx format for different reports.
- global refactoring

## [4.15.4] - 2023-11-08
- make job lock consider regions

## [4.15.3] - 2023-11-08
- fixed `patch_customer_data_2.0` patch to make it compatible with on-prem version

## [4.15.2] - 2023-11-01
- fixed bug when the coverage list was empty for tenant that violates every rules
- fixed bug when metrics lambda deletes archived file of the specific tenant instead of non-archived file of the same tenant
- Implement License Manager auth token storage/rotation per customer

## [4.15.1] - 2023-10-23
- marked the metrics of deactivated tenants or tenants with expired license with `archive-` prefix and 
ignore such tenants when aggregate metrics
- event-driven jobs are now also included in the rule report
- optimized calculation of a large number of jobs for rules report

## [4.15.0] - 2023-10-16
- removed a lot of not used code
- use python built-in enums for HTTP methods and statuses instead of constants
- added k8s scans support:
  - `GET /platforms/k8s`: describe platforms
  - `POST /platforms/k8s/eks`: add EKS platform
  - `DELETE /platforms/k8s/eks/{id}`: delete EKS platform
  - `POST /platforms/k8s/native`: add native platform
  - `DELETE /platforms/k8s/native/{id}`: delete native platform
- refactored a lot
- moved to new parent scopes (modular-sdk 3.3.0+)

## [4.14.2] - 2023-10-13
- changed the logic for duplicating metrics for those tenants that have not been scanned for a week+ - 
now duplicated only once per week

## [4.14.1] - 2023-10-12
- added a new field to the model of each report, which will display a list of tenants with outdated data

## [4.14.0] - 2023-10-09
- moved to python3.10
- updated dependencies versions:
  - pymongo: `3.12.0` -> `4.5.0`
  - requests `2.25.1` -> `2.31.0`
  - pycryptodome `3.15.0` -> `3.19.0`
  - modular-sdk `2.2.6` -> `3.2.0`
  - PyJWT `2.3.0` -> `2.8.0`
  - bcrypt `3.2.0` -> `4.0.1`
  - hvac `0.11.0` -> `0.11.2`
  - typing-extensions `4.5.0` -> `4.8.0`
  - APScheduler `3.9.1` -> `3.10.4`
- removed dependecies
  - importlib-metadata
  - importlib-resources
  - requests-toolbelt
  - tabulate
  - pytablereader
  - zzip
- removed not used lambdas: `caas-jobs-backupper`, `caas-notification-handler
- add env `INNER_CACHE_TTL_SECONDS` which controls inner cache


## [4.13.7] - 2023-10-05
- fixed invalid data format for finops metrics after difference calculation

## [4.13.6] - 2023-10-05
- fixed invalid time range when calculating weekly scan statistics

## [4.13.5] - 2023-10-04
- fixed bug when google accounts were not being added to MITRE high-level reports
- fixed bug when empty service section in FinOps metrics was skipped

## [4.13.4] - 2023-10-04
- updated `modular-sdk` version to `2.2.6a0`
- fixed bug when `end_date` is string not datetime at the `tenant_groups` metrics stage

## [4.13.3] - 2023-10-03
- fixed metrics difference calculation for finops project report

## [4.13.2] - 2023-09-28
- added optional `recievers` parameter for the following endpoints:
  - `/reports/operational GET`
  - `/reports/project GET`
- changed type of `types` parameter from list to str for the following endpoints:
  - `/reports/operational GET`
  - `/reports/project GET`
  - `/reports/department GET`
  - `/reports/clevel GET`

## [4.13.1] - 2023-09-28
- Fixed a bug where failed event-driven scans were skipped when aggregating metrics and collecting weekly scan statistics 

## [4.13.0] - 2023-09-28
- added finops metrics for operational and project reports
- removed optional `type` parameter for the following endpoints:
  - `/reports/operational GET`
  - `/reports/project GET`
  - `/reports/department GET`
  - `/reports/clevel GET`
- added optional `types` parameter of list type for the following endpoints:
  - `/reports/operational GET`
  - `/reports/project GET`
  - `/reports/department GET`
  - `/reports/clevel GET`

## [4.12.7] - 2023-09-25
- fixed a bug when resources in the event-driven report were displayed on one line instead of several
- make cloud not required in rule-meta model, add platform

## [4.12.6] - 2023-09-22
- added new field with rule description to operational MITRE report

## [4.12.5] - 2023-09-21
- refactored metrics aggregation sequence and moved department metrics aggregation to a separate step. The new 
sequence provides the same data source for the department and chief levels:
  - Old sequence: operational(tenant) -> project(tenant_group) + department level + mitre, compliance chief level) -> overview chief level -> difference
  - New sequence: operational(tenant) -> project(tenant_group) -> top(department + chief level) -> difference
- The summation of weekly statistics on the number of scans has been moved from the project level to the operational level.

## [4.12.4] - 2023-09-21
- added tenant job lock to allow only one job per tenant in a certain moment

## [4.12.3] - 2023-09-15
- fixed copying of findings files to a folder with a new date if the files are already there
- severity deduplication moved to the `helpers/utils.py`
- look up resources by only top-level attributes

## [4.12.2] - 2023-09-13
- fixed metrics for tenants that were not scanned for current week and do not remove resources severity from overview type
- reset attack customer variable for each customer

## [4.12.1] - 2023-09-12
- fixed collecting metrics for a specific date
- fix retrieving the latest findings for metrics

## [4.12.0] - 2023-09-05
- added `GET /reports/resources/tenants/{tenant_name}/state/latest` endpoint to
  allow to retrieve specific resource report
- added `GET /reports/resources/tenants/{tenant_name}/jobs` endpoint to 
  retrieve a list of the latest jobs where the resource was found
- added `GET /reports/resources/jobs/{id}` endpoint
- added rules_to_scan validation

## [4.11.2] - 2023-09-05
- fix querying tenants by its account number only for google tenants

## [4.11.1] - 2023-09-01
- refactor `customer_metrics_processor`, move common code to the metrics service
- add ability to query tenants by its account number (NOT project id)

## [4.11.0] - 2023-09-01
- added `POST /rule-meta/standards`, `POST /rule-meta/meta`, `POST /rule-meta/mappings`;
- fix accumulated reports;

## [4.10.2] - 2023-08-31
- reduce items limit for batch_save() method and catch PutError when saving department and c-level metrics
- change the `report_type` field in metrics to more human-readable names
- add `service` field to policy metadata
- fix Internal in case user does not exist when you try to change its tenants

## [4.10.1] - 2023-08-28
- fix bug that occurs when selecting tenants for an event-driven report 
- resources in overview reports are now saved with the highest severity level
- fix resources number for tenant_metrics_updater. Refactor and optimize tenant_metrics_updater

## [4.10.0] - 2023-08-23 (hotfix)
- change the interface for interacting with the Tenants table when generating event-driven report
(from customer-index to query)
- remove duplicates from operational and project OVERVIEW report when splitting resources by 
severity and by resource_type
- fix monthly metrics storage path from current month to next month (so the metrics for recreating 
reports from 01-08 to 01-09 will be stored in 01-09 folder and will be overwritten only in August)
- return the nearest found findings in case today's do not exist
- fix a bug with caas-job-updater when it did not send status to LM
- add an ability to specify LM api version
- add a missing changelog
- fix permissions POST validator

## [4.9.4] - 2023-08-15
- fix `last_scan_date` comparison in c-level OVERVIEW report

## [4.9.3] - 2023-08-15
- add a missing condition to the standard iteration method
- get impact, article, remediation for dojo reports from meta

## [4.9.2] - 2023-08-14
- fix convert region coverages to percents (ex. 0.24 -> 24)
- add `raw` param to get findings action to allow to return raw findings content
- add the ability to handle multiple customer applications in c-level reports
- fix issue in request forming while sending email body to RabbitMQ
- fix weekly job statistics calculation

## [4.9.1] - 2023-08-11
- fix typo in `customer_metrics_processor` (last_scan -> last_scan_date)

## [4.9.0] - 2023-08-10
- changed rules format;
- updated `CaaSRules` model - moved `version`, `standard`, `min_core_version`, `impact`, `severity`, `service_section` 
fields to the `CaaSRulesMeta` table;
- added `CaaSRulesMeta` table to store policies' metadata;
- removed `caas-rulset-compiler` lambda. There is no longer a need for it, since the ruleset is collected immediately 
from the data in the `CaaSRules` table;
- changed ruleset format from `.yml` to `.json`
- updated `CaaSRuleset` table:
  - deleted `customer-ruleset-name` and `customer-cloud` GSI;
  - added `customer-id-index'` and `license_manager_id-index` GSI;
  - made `id` field a composite key - `customer#L|S#name#version`;
  - removed `name`, `licensed` and `version` fields;
- added new health-check - rules_meta_access_data;
- renamed lambda environment variables that contained `mcdm` with `modular`;
- removed user detailed report;
- added an ability to build rulesets by git project id and ref;
- added an ability to describe and delete rules by git project id and ref;
- refactored compliance report - removed excessive code;
- parse eventdriven maps dynamically;
- getting impact and article for recommendation from `CaaSRulesMeta`;


## [4.8.1] - 2023-08-09
- fixed start date in ED report generation

## [4.8.0] - 2023-08-04
- replaced MCDM with Modular

## [4.7.3] - 2023-08-02
- added `CaaSJobStatistics` table;
- collect c-level `OVERVIEW` report using the weekly data that stored in `JobStatistics` table;
- fix duplicate recommendation bug;

## [4.7.2] - 2023-08-01
- fix `NEXT_STEP` value for `findings` step in metrics updater
- fix recommendations bucket name in environment_service
- add a parameter `is_zipped` to s3 service to save the file in an uncompressed format
- added endpoints to manage Defect Dojo applications:
  - `POST /applications/dojo`
  - `GET /applications/dojo`
  - `GET /applications/dojo/{application_id}`
  - `DELETE /applications/dojo/{application_id}`
  - `PATCH /applications/dojo/{application_id}`

## [4.7.1] - 2023-07-31
- add bucket for recommendation
- implement new metrics step for EC2 insights calculation
- add new service to interact with s3 from another account
- remove `type` parameter from `POST /parents/tenant-link` command
- changed inner CaaSEvents model. Removed index and added random partition

## [4.7.0] - 2023-07-27
- added an ability to create parent with type `CUSTODIAN_ACCESS` which is 
  linked to credentials application. After that you can link this parent 
  to tenant. In case tenant has `CUSTODIAN_ACCESS` in its parent_map, these 
  credentials will be preferred.
- added an ability to specify `rules_to_scan` for `POST /jobs`. Only 
  specified rules will be scanned

## [4.6.7] - 2023-07-25
- fixed bug with updating the last execution date of event-driven report
- fixed error related to the high request rate to the `BatchResults` table

## [4.6.6] - 2023-07-21
- fixed bug in last scan date calculation for customer reports
- added support for GOOGLE Maestro events

## [4.6.5] - 2023-07-20
- fixed some issues found by pylint

## [4.6.4] - 2023-07-19
- add `service_type` parameter to License Manager POST job request

## [4.6.3] - 2023-07-19
- fixed invalid data range and findings data in c-level report;
- separated findings by data, then by project identifier;
- added ability to run metrics from specific date;

## [4.6.2] - 2023-07-14
- change filter for maestro AZURE events so that the filter can retrieve 
  cloud from eventMetadata.request.cloud;

## [4.6.1] - 2023-07-07
- fix issue with duplicates in c-level reports
- fix issue with department-level reports: cannot parse empty cloud value

## [4.6.0] - 2023-07-06
- added `GET /metrics/status` API endpoint;
- added custom exception `MetricsUpdateException` to track metrics updating jobs;
- added environment variable `component_name` to `caas-api-handler`;

## [4.5.12] - 2023-07-05
- fixed a bug when metric difference values were saved recursively

## [4.5.11] - 2023-07-03
- fixed a bug when event-driven jobs were not included to metrics

## [4.5.10] - 2023-06-30
- changed `project_id` to `account_number` for gcp tenants;
- fixed a bug that was adding resources to a tenant with zero scan per period

## [4.5.9] - 2023-06-29
- added ability to reset overview data for tenant that has not been scanned in a 
specific period
- added ability to check available report by type and automatically create the missing one

## [4.5.8] - 2023-06-26
- fixed import to dojo
- fixed resource duplication in MITRE reports;
- fixed calculation of the total amount of resources in OVERVIEW report;
- fixed pagination in `CaaSTenantMetrics` and `CaaSCustomerMetrics` tables;

## [4.5.7] - 2023-06-22
- removed redundant fields in compliance, resources and event-driven reports;

## [4.5.6] - 2023-06-20
- fixed bug in pagination of `CaaSJobs` table;
- fixed bug in top 10 tenants by resources and compliance calculation;

## [4.5.5] - 2023-06-19
- fixed last sending date of event-driven reports;
- add more logs to s3 client

## [4.5.4] - 2023-06-14 (hotfix)
- fixed bugs related to invalid parsing of the scans submit date;
- fixed lambda ARNs in `deployment_resources.json`;
- added items limit on scanning `BatchResults` table;
- fixed: get DefectDojo secret from mcdm_assume_arn account;
- fixed internal server error in case dojo config is not set;
- add a list of endpoints which require customer to work to restriction service;
- removed deletion of standard points from account findings at the end of an event-driven scan

## [4.5.3] - 2023-06-09
- changed rule_report collection logic to aggregate manual job statistics only

## [4.5.2] - 2023-06-08
- fixed typo in lambdas naming in `lambda_client` service

## [4.5.1] - 2023-06-07
- fixed calculation of missed accounts in the `caas-metrics-updater` lambda

## [4.5.0] - 2023-06-06
- moved siem configurations to applications
- added endpoints:
  - `/reports/push/dojo/{job_id}`;
  - `/reports/push/security-hub/{job_id}`;
  - `/reports/push/dojo`;
  - `/reports/push/security-hub`;

## [4.4.3] - 2023-06-01
- fix invalid new week calculation in metric-updater
- fix collecting event-driven metrics
- add new requirement `python-dateutil` to the `caas-metrics-updater` lambda

## [4.4.2] - 2023-05-31
- fix invalid import in metrics updater 

## [4.4.1] - 2023-05-26
- implemented event-driven reports that are sent once for the period described in the license
- split application with type `CUSTODIAN` into two different applications: 
  `CUSTODIAN` and `CUSTODIAN_LICENSES`. The first one contains information 
  about access to custodian, the second one - licenses.
- added endpoints:
  - POST /applications/access
  - GET /applications/access
  - PATCH /applications/access/{application_id}
  - GET /applications/access/{application_id}
  - DELETE /applications/access/{application_id}
- update `ACCESS_DATA_LM` format

## [4.4.0] - 2023-05-24
- made metrics collection daily. Each day at 1:00am metrics are collected and the difference between 
  the current data and the last week's data is calculated;
- remove `permissions_admin` parameter from `POST /policies`;
- updated jwt auth for on-prem, token is not encrypted now. 
  Only PyJWT is used, jwtcrypto is removed.
- added endpoint to trigger metrics update:
  - POST /metrics

## [4.3.0] - 2023-05-22
- add endpoints to manage rabbitMQ configuration for customers:
  - POST /customers/rabbitmq
  - GET /customers/rabbitmq
  - DELETE /customers/rabbitmq
- move rabbitmq configuration to Applications with type `RABBITMQ` and use them;
- fix default value for POST /role `expiration` parameter. Not it's today + 2 months;

## [4.2.1] - 2023-05-19
**Api:**
- redone the existing difference calculation function using dataclasses

**Executor:**
- Implement License Manager auth token storage/rotation per customer


## [4.2.0] - 2023-05-15
**Api:**
- changed `RuleSource` primary key from [`id`:`hash-key`/`customer`:`range-key`] to a unique [`id`:`hash-key`]
  - updated `/rule-sources` endpoint `*` handlers
  - updated `/ruleset` endpoint POST handler
- added `rule_source_id` attribute & GSI to `Rule` entity: 
  - updated `/rules` endpoint `GET`, `DELETE` handlers 
  - updated `caas-rule-meta-updater` accordingly
- add an ability to create multiple applications with type `CUSTODIAN` 
  within a customer.

**Executor:**
- added aws xray profiling. Some jobs are sampled and the statistics will be 
  pushed to statistics bucket;

## [4.1.9] - 2023-05-15
- fixed bug of department report generation
- fixed lambda ARN creation in `caas-metrics-updater` 

## [4.1.8] - 2023-05-12
- added error handling to reports if rules have incorrect MITRE format;
- implement ED notification json-model

## [4.1.7] - 2023-05-10
- adjusted to MCDM SDK 2.0.0

## [4.1.6] - 2023-05-05
- put gitlab sdk to layer requirements to prevent urllib3==2.0.x from being 
  installed, because it fails;
- rewrite link * unlink parent according to new MCDM api;

## [4.1.5] - 2023-05-03 (hotfix)
- fix a bug with `GET /scheduled-job`;
- updated `buckets-exist` health-check;
- fixed datetime of current metrics collection;
- add new available instance types to batch comp-env

## [4.1.4] - 2023-05-02
- fix minor bug with standard google jobs;

## [4.1.3] - 2023-05-01
- update event-driven template and add filters to repo;

## [4.1.2] - 2023-04-26
- changed type of `tenant_names` and `tenant_display_names` parameters in `/report/operational` and 
`/report/project` endpoints from array to string;
- fixed bug when sending a report without specifying a tenant name

## [4.1.1] - 2023-04-25
- added `from` date calculation to reports if there is no previous date in settings;
- added the ability to send multiple reports at once via RabbitMQ;
- added `key_id` parameter for the DELETE `/settings/license-manager/client` action

## [4.1.0] - 2023-04-20
**Api:**
- `csv` replaced with `xlsx` for `/reports/rules/...` endpoints;
- updated validation for the `/tenants/license-priorities` actions;
- concealed `/tenants/license-priorities` resource; 

**Executor:**
- removed `report_fields`, `severity` and `standard_points` from findings
- get impact, article, remediation for dojo reports from meta

## [4.0.0] - 2023-02-03 - 2023-03-27
**Api:**
- removed CaaSAccounts and all the bound code. Tenants are fully used 
  instead of accounts. (this includes a lot of changes inside, but outside 
  everything is more-of-less the same);
- all the endpoints that required `account_name` and|or `tenant_name` now just 
  require `tenant_name`. Tenants contain almost all the information accounts 
  used to have. Custodian-specific information that does not have its 
  place in Maestro Tenant model we put to TenantSettings;
- removed `syndicate_configs` for on-prem. Dotenv is used instead. Added 
  entry script `main.py` with such actions `init_vault`, `create_buckets`, 
  `create_indexes`, `run`;
- moved all the scripts to `main.py`;
- implemented mechanism that collects scans data and group them for reports:
  - added `caas-metrics-updater` and `caas-report-generation-handler` lambdas;
  - added `CaaSCustomerMetrics` and `CaaSTenantMetrics` tables;
  - added `rabbitmq`, `tenant_metrics` and `customer_metrics` services;
  - added new settings `MAESTRO_RABBITMQ_CONFIG` and `REPORT_DATE_MARKER`;
  - removed `/reports/event-driven` endpoint;
- added `attack_vector` field to the scan detailed report;
- refactored access_control_service, added new cache, and an ability to set 
  a wildcard as part of permission to allow all the groups and subgroups 
  (only SYSTEM can set currently);
- added endpoints to manage Custodian application for Maestro Customer:
  - `POST /application`
  - `GET /application`
  - `DELETE /application`
  - `PATCH /application`
- added endpoints to manage Custodian parents for Maestro Customer:
  - `POST /parents`
  - `GET /parents`
  - `GET /parents/{parent_id}`
  - `DELETE /parents/{parent_id}`
  - `PATCH /parents/{parent_id}`
  - `POST /parents/tenant-link`
  - `DELETE /parents/tenant-link`
- Removed `POST /license`, refactored `GET /license`, `DELETE /license`
- Refactored `POST /jobs` so that it could submit only licensed jobs and 
  could resolve rulesets automatically;
- added new API endpoints:
  - `GET /reports/department`;
  - `GET /reports/operational`;
  - `GET /reports/project`;
  - `GET /reports/clevel`;
- added new CLI commands:
  - `c7n report department`;
  - `c7n report operational`;
  - `c7n report project`;
  - `c7n report clevel`;
- updated deployment guide;

**Executor:**
- removed CaaSAccounts and all the bound code. 
  Now Tenants are used instead of accounts.

## [3.4.0] - 2023-01-30 - 2023-02-03
- fix a bug with push to siem
- add an ability to describe tenants by project id
- fix a bug with not found rule-source branch;
- unify incoming events format, add an ability to process Maestro Azure events
- process events only for tenants which has event-driven enabled by license; 
  Process only rules allowed by license

## [3.3.2] - 2023-01-28 - 2023-01-30
- add env `event_consider_tenant_region_activity` to be able to disable 
  tenant region activity validation;
- Add some filters to caas-api-handler concerning EventBridge events;
- Amend registration timestamping;
- Adapt to Maestro's `active-region` states of Tenants.

## [3.3.1] - 2022-12-23 - 2023-01-20
**Api:**
- add new lambda to process Event-Driven events from SQS: 
  caas-sqs-events-processor;

**Executor:**
- add a missing attribute to BatchResults;

## [3.3.0] 2022-12-23 - 2023-01-20
**Api:**
- Add siem and license configuration to configuration backupper and configuration updater;
- Refactor CredentialsManager: swap its hash_key and range_key;
- new event-driven 2.0: collect and save events to CaaSEvents and then 
  process them by batches regularly. Save them to CaaSBatchResults.
- Add CaaSEventStatistics;
- Rewrite reports API drastically;

**Executor:**
- split event-driven and standard job flows;
- integrate new event-driven and BatchResults;

## [3.2.3] 2022-12-20 - 2022-12-22 (hotfix)
**Api:**
- implement customer-tenant restriction for actions related to:
  - scheduled-job management
  - credentials manager configuration

**Executor:**
- add to statistics the policies that were skipped due to the time threshold
- fix job lifetime threshold (use job's `startedAt` instead of `createdAt`)

## [3.2.2] 2022-12-14 - 2022-12-16
**Api:**
- remove LM_ACCESS_DATA_HOST, MAIL_CONFIGURATION, SYSTEM_CUSTOMER env variables from api-handler

**Executor:**
- update notification content;
- replace settings passing through environment variables with settings service

## [3.2.1] 2022-12-09 - 2022-12-13
**Api:**
- optimize `caas-job-updater` a bit: send request to LM only if the 
  job is licensed.

**Executor:**
- make some optimizations:
  - upload all the report files to S3 bucket concurrently;
  - use `libyaml` from C in order to speed up Yaml files loading and dumping;
  - send requests to LM in job_updater_service only if the job is licensed;
  - increase the number of retry attempts in boto3 S3 config;

## [3.2.0] 2022-11-28 - 2022-12-09
**Api:**
- remove PynamoDBToMongoDBAdapter and BaseModel and use the ones from MCDM SDK;
- changed response code from `200` to `202` on the following endpoints:
  - `/job POST`;
  - `/job DELETE`;
  - `/rules/update-meta POST`;
  - `/license POST`;
  - `/license/sync POST`;
- changed `POST` response code for success from `200` to `201` on the following endpoints:
  - `/signup`;
  - `/scheduled-job`;
  - `/accounts`;
  - `/accounts/regions`;
  - `/accounts/credential_manager`;
  - `/rulesets`;
  - `/rule-sources`;
  - `/policies`;
  - `/roles`;
  - `/users/role`;
  - `/users/customer`;
  - `/users/tenants`.
- split `/siem` endpoint into `/siem/defect_dojo` and `/siem/security_hub` endpoints;
- refactor siem manager so that it could contain siem configurations for 
  tenants within customers
- add ability to update schedule expression for scheduled jobs;
- provided rescheduling-notice notifications, for any eventually-inaccessible
  licensed rulesets.
- provided `/setting/mail` configuration endpoint;
- unify responses:
  - return empty lists instead of messages in case no items are found;
  - return correct status codes instead of just 200 everywhere;
- restrict reports for users: do not show them standards' points;
- leave target rulesets filtration for scan only in the `custodian-api-handler` lambda, 
  remove filtration code from `executor.py`. In other words: for not licensed 
  rulesets not we set rulesets' ids in docker environment instead of names;
- add Swagger for SAAS.
- add `pydantic` model validators for all the incoming requests and DTO 
  models for all the responses (used just to document an API-GW);
- add `ttl` attribute to `CaaSJobs`; Implemented event-driven jobs backup: 
  added `caas-jobs-backupper` lambda which is triggered by CaaSJobs DynamoDB 
  Stream and pushes records to Kinesis firehose which saves them to S3.
- add an ability to backup jobs directly to S3 without AWS Firehose
- fix a bug with scheduled job name length limit exceeded;
- remove git source access data validation in case a user wants to update 
  only source's permissions;

**Executor:**
- remove PynamoDBToMongoDBAdapter and BaseModel and use the ones from MCDM SDK
- add one more report `user_detailed_report.json`. It's the same 
  as `detailed_report.json` but does not contain standards
- `TARGET_RULESET` env now must contain rulesets' ids instead of names.


## [3.1.0] 2022-11-21 - 2022-11-25 (hotfix)
- fix bug with `attribute_values` with pynamodb DynamicMapAttribute;
- correct license sync action to sync only for allowed by LM client types;
- add tenant-scope permission for rulesets/rule_sources and rules;
- add tenant-scope permission to `get_account_by_cloud_id`;

## [3.0.0] 2022-09-14 - 2022-11-02
**Api:**
- integrate Maestro common domain model (MCDM) and its SDK;
- adjusted compliance_report action in `caas-report-generator` to the new 
  parsed coverage's data; calculate compliance the right way based on points;
- added an ability to generate compliance report based on the whole account:
  param `account` can be sent in an event to `/report/compliance` endpoint;
- fixed bug when getting ruleset by name from system user occurs to an error;
- add gzip compression to all the files in S3/Minio. Presined urls now returns 
  files `.gz`;
- add new mapping to CloudTrail event-driven mode: eventName to rule ids;
- refactor on-prem connections to Vault, Minio and MongoDB;
- adopted a batch-internal licensed job instantiation for the submission-related
  actions in `caas-api-handler`;
- adapted licensed ruleset entities to a non-persistent content approach.
- add an ability to build rulesets by rule_source_id;
- when a user removes a rule_source, all its rules will be removed as well;
- added `customer-display-name` and `tenant-display-name` indexes to `CaaSJob` table;
- added `last_evaluated_key` pagination for `GET /job` and `GET /rules` action 
  (default 10);
- added an ability to send an email with scan results of a specific account 
  in case new violating resources are found;
- added an ability to register scheduled jobs both on Premises and on AWS:
  - `GET` `/{stage}/scheduled-job/`;
  - `POST` `/{stage}/scheduled-job/`;
  - `DELETE` `/{stage}/scheduled-job/`;
  - `UPDATE` `/{stage}/scheduled-job/`;
- added permissions restriction by user tenants;
- added new optional custom attribute `tenants` for cognito user and an ability to assign/unassign tenants via API:
  - `GET` `/{stage}/users/tenants/`;
  - `POST` `/{stage}/users/tenants/`;
  - `DELETE` `/{stage}/users/tenants/`;
- migrated all the inner date to UTC;
- add license priorities for rulesets
- added token management service and token-based authentication for SAAS to 
  allow to sync all the tenants at once
- added an ability to remove your user: `DELETE /users` and an ability to 
  reset you password `POST /users/password-reset`

**Executor:**
- integrate Maestro common domain model (MCDM) and its SDK;
- Refactored to allow to execute the docker without command but only with envs.
- Refactored the main flow and split to separate classes. Added an ability to 
  execute policies in ThreadPoolExecutor. Set `EXECUTOR_MODE` env to 
  either `consistent` or `concurrent`
- If job item does not exist in DB after the executor has started, 
  it will be created;


## [2.2.1] - 2022-09-08
- remove `root_policy.json`, `root_role.json`, `iam_permissions.json`;
- resolve IAM permissions dynamically using inner endpoint to permission mapping;
- remove `s3_to_minio_adapter` and use just boto3 client to interact with S3 and Minio;
- remove the ability to create rule sources from customer with the parameter `inherit` as `True`
- add resolving rule ids from partly given strings in request attributes 
  (`501` -> `epam-aws-501-sagemaker_instance_root_disabled_1.0`)

## [2.2.0] - 2022-09-01
- add notifications for vulnerable resources detections in event-driven mode;
- add `trace_id` to each lambdas' response;

## [2.1.0] - 2022-08-29
- refactor and fix terminating jobs in `custodian-api-handler` lambda;
- add an ability to assemble rulesets by `service_section` (Storage, 
  Network, etc.);
- optimize ruleset-compiler: clone the full rule-source and load the necessary 
  rules instead of making a request for each rule;
- optimize and speed up rule-meta-updater: query for git-blame in parallel;
- integrate the new calculated by Custom-Core coverage and 
  refactor reports-generator to adjust to it;
- fix a bug with stale `resources_mappings.json` in docker executor;
- adjust Kubernetes setup: add missing scripts and configs, optimize k8s 
  docker image;
- optimize Batch docker image;
- refactor `custodian-license-updater` lambda;
- replace custom API-Gateway integrations with API-Gateway lambda-proxy 
  integration;
- minor fixes in event-driven, c7n lm, executor;

## [2.0.1] - 2022-07-07
- Fix setting job `stopped_at` status;
- Add filtering of licensed rulesets on job submit: ruleset will be skipped 
  if customer is not in list of allowed by license;
- Increase timeout of license-updater lambda to 300 sec;
- Add missing permissions to system_policy;


## [2.0.0] - 2022-06-24
**Api**
- Added integration with Custodian License Manager.
- Behaviour of objects that holds configuration (Customers, Tenants, Accounts). 
  Rulesets and rule sources now stored in a separate tables:
  CaaSRulesets, CaaSRuleSources.
- Updated DefectDojo integration:
  - Added `entities_mapping` attribute to SiemManager model and an ability 
    to alter the object by API. The object represents the way the Custodian's 
    entities are related to DefectDojo's entities;
  - Consign each finding to a specific DefectDojo's group named by 
    `service_section` attribute from policy's yaml during the push action;
  - Set finding's ARN to `component_name` DefectDojo's attribute;
  - Set policy's id to `vuln_id_from_tool` DefectDojo's attribute;
  - Each finding's region will be set to DefectDojo's tag instead of the 
    finding's title;
  - Added ab ability to represent each finding in DefectDojo as a separate 
    violated resources by setting `resource_per_finding` to `true` in 
    SIEMManager config; By default each finding will still represent a 
    separate rule with a bunch of resources;

**Executor**
- Added integration with Custodian License Manager.
  
## [1.3.0] - 2022-06-20
- `/report/compliance/`, `/report/error/`, `/report/rule` endpoints can 
  receive `job_id` parameter in requests and generate reports for a 
  specific job;
- Optimized RAM memory usage in docker executor during multi-region scans;
- Fixed an error with missing resources after scans and different number of 
  resources in `detailed_report.json` and `report.json`;
- Fixed collecting AccessDenied errors - added missing possible AWS exceptions;
- Fixed compliance report generation;

## [1.2.0] - 2022-05-26
Compatibility of optional AWS credentials with the `custodian-report-generator` lambda
### Altered
- Behaviour of the `custodian-report-generator` lambda `handler`,
to provide support for optional AWS credentials, intended for actions with SecurityHub SIEM
- Initialization flow of the `SecurityHubAdapter`, 
allowing one to be instantiated with respective credentials
### Fixed
- Fixed an error occurred during context creation for DefectDojo;
- Fixes an error occurred when two tenants with similar names 
  (from different customer) were pushed to DefectDojo;
- Add access restrictions for siem-manager handler and an ability to describe 
  all the siem configs from all the customer if you are the SYSTEM customer.

## [1.1.0] - 2022-05-16
First production release of Custodian-as-a-Service
### Added
- RBAC
- Integrations with SIEM: `Defect Dojo` and `AWS SecurityHub`
- Event driven scans
- Kubernetes installation of CaaS (On-premises mode)
- Reports generation: compliance, rule, error


## [1.0.0] - 2021-04-08
Initial release of Custodian Service.
