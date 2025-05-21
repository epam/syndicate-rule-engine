### Execute patch:

```
docker build -t patch:5.10.0 .
docker run --network rule-engine -e SRE_REPORTS_BUCKET_NAME=$SRE_REPORTS_BUCKET_NAME -e SRE_MINIO_ACCESS_KEY_ID=$SRE_MINIO_ACCESS_KEY_ID -e SRE_MINIO_SECRET_ACCESS_KEY=$SRE_MINIO_SECRET_ACCESS_KEY patch:5.10.0
```