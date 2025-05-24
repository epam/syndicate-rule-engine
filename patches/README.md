

- a patch is a docker executable that will be executed as K8S job
- patch should not break the data if executed more than once
- patch executable has access to minio vault and mongo credentials
- patch should wait till the patched service is started


## Building

Assuming that your working directory is `patches/`


Export the necessary patch version to env `PATCH_VERION`:

```bash
expor PATCH_VERSION=<specify version here>
```


Build for arm64:
```bash
podman build --platform linux/arm64 -t public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-arm64 -f ./"$PATCH_VERSION/Dockerfile" ./"$PATCH_VERSION"
```

Build for amd64:
```bash
podman build --platform linux/amd64 -t public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-amd64 -f ./"$PATCH_VERSION/Dockerfile" ./"$PATCH_VERSION"
```

Push both versions:

```bash
podman push public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERION"-arm64
podman push public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERION"-amd64
```

Create image manifest

```bash
podman manifest create public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-arm64 public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-amd64
podman manifest annotate public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-arm64 --arch arm64
podman manifest annotate public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION" public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"-amd64 --arch amd64
```

Push manifest
```bash
podman manifest push public.ecr.aws/x4s4z8e1/syndicate/patches:"$PATCH_VERSION"
```