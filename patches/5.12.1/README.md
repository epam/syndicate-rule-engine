

This patch requires access to the main code of SRE. So, its image should be built differently from most other patches.

Follow the steps from [patches/README.md](../README.md) but make sure you are building from ROOT sre context instead of 
just patch context. The command should look like this:

```bash
podman build --platform linux/arm64 -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-"$PATCH_VERSION"-arm64 -f ./patches/"$PATCH_VERSION"/Dockerfile .
```
