
# Release 5.15.0

## API

- Added hiding expired resource exceptions
- Fixed an issue with filtering resource exceptions by `--tags_filters`
- Added the parameter `overwrite` to the `POST /rulesets/release` endpoint that allows to overwrite existing ruleset version
- Added the possibility to configure gunicorn workers timeout via the `SRE_GUNICORN_WORKERS_TIMEOUT` env variable
- Push to Defect Dojo changed from synchronous to asynchronous
- Added user guide and commands reference guide to the documentation
- Added `/metadata/update` endpoint to updating locally stored metadata
- Added `/service-operations/status?type={service_operation_type}` endpoint to get the status of service operations where `service_operation_type` is one of:
  - `metrics-update`
  - `metadata-update`
  - `push-dojo`
- Added `Permission.SERVICE_OPERATIONS_STATUS` permission for getting the status of service operations
- Added exceptions information (`exceptions_data`) to project-level reports: 
  - Project Overview
  - Project Compliance
  - Project Resources
  - Project Attacks
  - Project FinOps
- Added violated rules information (`cluster_metadata.rules.violated`) to operational Kubernetes report
- Restored recommendations processing flow in `metrics_updater` lambda
- Disabled the plugin `gcp_cloudrun`
- Fixed plugin `aws.workspaces-directory.filter.check-vpc-endpoints-availability`
- Fixed issue with kube-confid file during k8s scan
- Fixed an issue with describing rules by the cloud name
- Fixed an issue with handling the `expiration` parameter in `sre role add/update` commands
- Fixed an issue with creating resource exceptions with the same parameters
- Added the possibility to submit jobs and push reports to dojo with custom `product`, `engagement`, and `test` names
- Resolved a problem caused by the license manager being temporarily unavailable.
- `Permission.METRICS_STATUS` permission replaced with `Permission.SERVICE_OPERATIONS_STATUS`
- Fixed an issue with inaccurate K8S scan results
- Fixed rulesets double compression issue

## CLI

- Changed an alias name from `-l` to `-loc` for the `--location` parameter in the command `resource exception describe` because of duplication with the `--limit` parameter alias in the same command
- Added the flag `--include_expired` to the command `resource exception describe` to allow retrieving expired resource exceptions
- Added the `--overwrite` flag to the `ruleset release` command, enabling overwriting of an existing ruleset version
- Updated `--expiration` parameter help text to clarify UTC interpretation for naive datetime values
- Added `sre metadata update` command to updating locally stored metadata
- Added unified command to get the status of service operations:
  - `sre service_operation status --operation <operation_type>`
  - Available operations: `metrics_update`, `metadata_update`, `push_dojo`
- Updated help messages for date parameters to use dynamic date examples in `sre role add|update`, `sre job describe` and `sre metrics status` commands
- Added `dojo_product`, `dojo_engagement`, and `dojo_test` parameters to commands `job submit`, `job submit_k8s`, and `repot push dojo`
- Fixed an issue with `--google_application_credentials_path` parameter not being recognized as a file parameter in `re job submit` command when SRE CLI is installed as a module within Modular API, causing "${file} not found" error
- Fixed an issue with formatting datetime for `sre setting lm config describe` command
- `sre metrics status` command replaced with `sre service_operation status --operation metrics_update`

## Included in this release
<!-- Roman Myhun -->
- [11114] SRE. Add metadata (cluster_metadata.rules.violated) to Kubernetes report
- [10867] SRE. Metrics aggregation issue, no metrics/recommendations
- [10821] SRE. Unify status tracking for async jobs of diff types (batch, celery)
- [10462] SRE. Add exception information in separate section and in the attachment for project reports
- [10357] SRE. Updating locally stored metadata
- [10701] SRE CLI. Issue with sre setting lm config describe
- [10718] SRE. 'File not found' error message when running 're job submit' with --google_application_credentials_path parameter
- [10744] SRE. Implement dynamic date generation in parameter descriptions
- [10745] SRE. Issue with handling the `expiration` parameter in `sre role add/update` commands
- [10820] SRE. Issue on resolving SRE-LM version compatibility
- [10354] SRE. Add a check to prevent the creation of exceptions with the same parameters
<!-- Mykhailo Kutsybala -->
- [10784] SRE. Incorrect mapping of resources to policies
- [10751] SRE. Custom Dojo structure
- [10750] SRE. Issue with scanning k8s clusters
- [10648] SRE. Push findings to DefectDojo fails to finish within timeout 
- [10783] SRE. Jobs are stuck in the RUNNING status in Google Cloud
- [10518] SRE. Quick start guide
- [10465] SRE. Do not show expired exceptions in the describe exception command
- [10463] SRE. Implement ability to describe exceptions by tags, types and resources
- [10359] SRE. Update QUICKSTART and README.md




