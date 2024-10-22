
These files are raw outputs from Cloud Custodian to test our post-processing methods, but with such differences:

- `custodian-run.log` is removed. We don't use it
- all fields except `name`, 'resource`, `description` are removed from `metadata.json` `.policy`. Description is changed with mocked one
- all the `output_dir` in `metadata.json` are replaced with just region dir
- all account ids values for AWS are replaced with `123456789012`, AZURE - `3d615fa8-05c6-47ea-990d-9d162testing`, 
  GOOGLE - `null` (because it's None after the scan in metadata)
- Mostly some random rules are kept. Of course, i tried to keep some tricky rules (s3, CloudTrail). Some rules have 
  resources, some don't, some are failed. Feel free to adjust the data if you have some obviously peculiar cases
- all the names of resources are changed, sensitive data is also changed