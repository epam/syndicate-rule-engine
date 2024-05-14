### Folder structure:

* expected_metrics - folder with jsons to compare results with
* mock_files - folder with jsons that mock some s3 files like metadata, statistics, old metrics to compare with

### How to run via terminal (on windows it works, hope it works for unix too):

1. Change folder to the `custodian-as-a-service`
2. Run `pytest tests/tests_metrics`
