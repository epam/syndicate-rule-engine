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

| Argument  | Description                                                |
|:----------|:-----------------------------------------------------------|
| --dry-run | Preview changes without making any modifications           |
| --force   | Process platforms with duplicate names (see warning below) |


| Variable                    | Required | Description                                    |
|:----------------------------|:---------|:-----------------------------------------------|
| REPORTS_BUCKET_NAME         | Yes      | Name of the S3/MinIO bucket containing reports |
| SRE_MINIO_ENDPOINT          | Yes      | MinIO/S3 endpoint URL                          |
| SRE_MINIO_ACCESS_KEY_ID     | Yes      | MinIO/S3 access key                            |
| SRE_MINIO_SECRET_ACCESS_KEY | Yes      | MinIO/S3 secret key                            |
| SRE_MONGO_URI               | Yes      | MongoDB connection URI                         |
| SRE_MONGO_DB_NAME           | Yes      | MongoDB database name                          |


## Stats
- migrated - platforms whose reports have been successfully migrated
- no_source - platforms with empty reports
- skipped - platforms that were skipped during the `--dry-run`
- duplicate_skipped - platforms with the same name that were skipped without using `--force`
- errors - platforms that could not be migrated


## Building

This patch requires access to the main SRE codebase. Build from the **ROOT** SRE context:

```bash
export PATCH_VERSION=5.18.0

# For AMD64
podman build --platform linux/amd64 \
  -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-${PATCH_VERSION}-amd64 \
  -f ./patches/${PATCH_VERSION}/Dockerfile .

# For ARM64
podman build --platform linux/arm64 \
  -t public.ecr.aws/x4s4z8e1/syndicate/patches:rule-engine-${PATCH_VERSION}-arm64 \
  -f ./patches/${PATCH_VERSION}/Dockerfile .


$env:PYTHONPATH = "C:\Users\DemianDiakulych\PycharmProjects\syndicate-rule-engine\src".
# Set environment variables first
$env:REPORTS_BUCKET_NAME = "reports"
$env:SRE_MINIO_ENDPOINT = "http://localhost:9000"
$env:SRE_MINIO_ACCESS_KEY_ID = "minioadmin"
$env:SRE_MINIO_SECRET_ACCESS_KEY = "minioadmin"
$env:SRE_MONGO_URI = "mongodb://localhost:27017"
$env:SRE_MONGO_DB_NAME = "sre_db"

# Then run
python patches\5.18.0\main.py --dry-run

python .\patches\5.18.0\main.py --dry-run
