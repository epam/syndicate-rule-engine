from pynamodb.attributes import UnicodeAttribute, ListAttribute, TTLAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import CAASEnv, JobState
from helpers.time_helper import utc_iso
from models import BaseModel

JOB_ID = 'i'
JOB_BATCH_JOB_ID = 'b'
JOB_CELERY_TASK_ID = 'cti'
JOB_TENANT_NAME = 't'
JOB_CUSTOMER_NAME = 'c'
JOB_STATUS = 's'
JOB_SUBMITTED_AT = 'sa'
JOB_CREATED_AT = 'cr'
JOB_STARTED_AT = 'sta'
JOB_STOPPED_AT = 'sto'
JOB_QUEUE = 'q'
JOB_DEFINITION = 'd'
JOB_OWNER = 'o'
JOB_REGIONS = 'rg'
JOB_RULESETS = 'rs'
JOB_REASON = 'r'
JOB_SCHEDULED_RULE_NAME = 'sr'
JOB_RULES_TO_SCAN = 'ru'
JOB_PLATFORM_ID = 'p'
JOB_TTL = 'ttl'
JOB_AFFECTED_LICENSE = 'al'


class TenantNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{JOB_TENANT_NAME}-{JOB_SUBMITTED_AT}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant_name = UnicodeAttribute(hash_key=True, attr_name=JOB_TENANT_NAME)
    submitted_at = UnicodeAttribute(range_key=True, attr_name=JOB_SUBMITTED_AT)


class CustomerNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{JOB_CUSTOMER_NAME}-{JOB_SUBMITTED_AT}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True,
                                     attr_name=JOB_CUSTOMER_NAME)
    submitted_at = UnicodeAttribute(range_key=True, attr_name=JOB_SUBMITTED_AT)


class Job(BaseModel):
    class Meta:
        table_name = 'CaaSJobs'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name=JOB_ID)
    batch_job_id = UnicodeAttribute(null=True, attr_name=JOB_BATCH_JOB_ID)
    celery_task_id = UnicodeAttribute(null=True, attr_name=JOB_CELERY_TASK_ID)
    tenant_name = UnicodeAttribute(attr_name=JOB_TENANT_NAME)
    customer_name = UnicodeAttribute(attr_name=JOB_CUSTOMER_NAME)

    submitted_at = UnicodeAttribute(attr_name=JOB_SUBMITTED_AT,
                                    default=utc_iso)
    status = UnicodeAttribute(attr_name=JOB_STATUS,
                              default=JobState.SUBMITTED.value)

    created_at = UnicodeAttribute(null=True, attr_name=JOB_CREATED_AT)
    started_at = UnicodeAttribute(null=True, attr_name=JOB_STARTED_AT)
    stopped_at = UnicodeAttribute(null=True, attr_name=JOB_STOPPED_AT)

    queue = UnicodeAttribute(null=True, attr_name=JOB_QUEUE)
    definition = UnicodeAttribute(null=True, attr_name=JOB_DEFINITION)
    owner = UnicodeAttribute(null=True, attr_name=JOB_OWNER)

    regions = ListAttribute(default=list, attr_name=JOB_REGIONS)
    rulesets = ListAttribute(default=list, attr_name=JOB_RULESETS)
    reason = UnicodeAttribute(null=True, attr_name=JOB_REASON)
    scheduled_rule_name = UnicodeAttribute(null=True,
                                           attr_name=JOB_SCHEDULED_RULE_NAME)
    rules_to_scan = ListAttribute(default=list, attr_name=JOB_RULES_TO_SCAN,
                                  of=UnicodeAttribute)
    platform_id = UnicodeAttribute(null=True, attr_name=JOB_PLATFORM_ID)
    affected_license = UnicodeAttribute(null=True,
                                        attr_name=JOB_AFFECTED_LICENSE)

    ttl = TTLAttribute(null=True, attr_name=JOB_TTL)

    customer_name_submitted_at_index = CustomerNameSubmittedAtIndex()
    tenant_name_submitted_at_index = TenantNameSubmittedAtIndex()
