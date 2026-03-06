"""Event-driven domain constants."""

# CloudTrail event
CT_USER_IDENTITY = "userIdentity"
CT_EVENT_SOURCE = "eventSource"
CT_EVENT_NAME = "eventName"
CT_ACCOUNT_ID = "accountId"
CT_RECORDS = "Records"
CT_RESOURCES = "resources"
CT_REGION = "awsRegion"
CT_EVENT_TIME = "eventTime"
CT_EVENT_VERSION = "eventVersion"

# EventBridge event
EB_ACCOUNT_ID = "account"
EB_EVENT_SOURCE = "source"
EB_REGION = "region"
EB_DETAIL_TYPE = "detail-type"
EB_DETAIL = "detail"
EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE = "AWS API Call via CloudTrail"

# Maestro event
MA_EVENT_ACTION = "eventAction"
MA_GROUP = "group"
MA_SUB_GROUP = "subGroup"
MA_EVENT_METADATA = "eventMetadata"
MA_CLOUD = "cloud"
MA_EVENT_SOURCE = "eventSource"
MA_EVENT_NAME = "eventName"
MA_TENANT_NAME = "tenantName"
MA_REGION_NAME = "regionName"
MA_REQUEST = "request"
