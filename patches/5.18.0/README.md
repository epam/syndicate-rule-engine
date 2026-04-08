# Patch 5.13.0 - Reports Path Migration

## Purpose

This patch migrates existing reports in the S3/MinIO bucket from the old path structure using `platform_id` to the new path structure using `id`.

### Path Structure Change

**Old path:** `reports/{customer}/{cloud}/{platform.platform_id}/...`

**New path:** `reports/{customer}/{cloud}/{platform.id}/...`

## Prerequisites

- Access to the reports S3/MinIO bucket
- Platform data in the database (to map `platform_id` to `id`)
- Same environment variables as the main SRE application

## Building

This patch requires access to the main SRE codebase. Build from the ROOT sre context:

```bash
export PATCH_VERSION=5.13.0

# For AMD64
podman build --platform linux/amd64 \
  -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-${PATCH_VERSION}-amd64 \
  -f ./patches/${PATCH_VERSION}/Dockerfile .

# For ARM64
podman build --platform linux/arm64 \
  -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-${PATCH_VERSION}-arm64 \
  -f ./patches/${PATCH_VERSION}/Dockerfile .