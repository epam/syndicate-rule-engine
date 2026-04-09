# Patch 5.18.0 - Kubernetes Platform Reports Path Migration

## Purpose

This patch migrates existing Kubernetes platform reports in the S3/MinIO bucket from the old path structure (using `platform_id` derived from `name-region`) to the new path structure (using platform `id`).

### Path Structure Change

| Old Path                                                | New Path                                      |
|---------------------------------------------------------|-----------------------------------------------|
| `reports/raw/{customer}/KUBERNETES/{name}-{region}/...` | `reports/raw/{customer}/KUBERNETES/{pid}/...` |

**Example:**
- **Before:** `reports/raw/CUSTOMER1/KUBERNETES/my-cluster-eu-west-1/latest/...`
- **After:** `reports/raw/CUSTOMER1/KUBERNETES/abc1-23de-f456-asd1/latest/...`

## Prerequisites

- Access to the reports S3/MinIO bucket
- Access to MongoDB with `Parents` collection containing `PLATFORM_K8S` documents
- Environment variables properly configured (see below)

## Building

This patch requires access to the main SRE codebase. Build from the **ROOT** SRE context:

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


