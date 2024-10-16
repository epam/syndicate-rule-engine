

- a patch is a docker executable that has will be executed as K8S job
- patch should not break the data if executed more than once
- patch executable has access to minio vault and mongo credentials
- patch should wait till the patched service is started
