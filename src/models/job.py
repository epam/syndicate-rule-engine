import warnings

from pynamodb.attributes import (
    Attribute,
    ListAttribute,
    MapAttribute,
    TTLAttribute,
    UnicodeAttribute,
)
from pynamodb.constants import STRING
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import Env, JobState, JobType
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
JOB_CREDENTIALS_KEY = 'ck'
JOB_APPLICATION_ID = 'aid'
JOB_WARNINGS = 'w'
JOB_DOJO_STRUCTURE = 'ds'
JOB_TYPE = 'ty'


class TenantNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f"{JOB_TENANT_NAME}-{JOB_SUBMITTED_AT}-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant_name = UnicodeAttribute(hash_key=True, attr_name=JOB_TENANT_NAME)
    submitted_at = UnicodeAttribute(range_key=True, attr_name=JOB_SUBMITTED_AT)


class CustomerNameSubmittedAtIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f"{JOB_CUSTOMER_NAME}-{JOB_SUBMITTED_AT}-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True, attr_name=JOB_CUSTOMER_NAME)
    submitted_at = UnicodeAttribute(range_key=True, attr_name=JOB_SUBMITTED_AT)


class DojoStructureAttribute(MapAttribute):
    product = UnicodeAttribute(null=True)
    engagement = UnicodeAttribute(null=True)
    test = UnicodeAttribute(null=True)


class JobTypeAttribute(Attribute[JobType]):
    """
    Attribute for JobType enum
    Serializes to and from string
    """

    attr_type = STRING

    def serialize(self, value):
        value = value.value if isinstance(value, JobType) else value
        value = value.lower()
        if value == JobType.MANUAL.value:
            standard_value = JobType.STANDARD.value
            warnings.warn(
                message=(
                    f"JobType {value!r} is deprecated and will be removed in "
                    f"future releases, using {standard_value!r} instead."
                ),
                category=DeprecationWarning,
                stacklevel=2,
            )
            value = standard_value
        return value

    def deserialize(self, value):
        return JobType(value)


class Job(BaseModel):
    class Meta:
        table_name = "SREJobs"
        region = Env.AWS_REGION.get()
        mongo_attributes = True  # ttl attribute is patched

    id = UnicodeAttribute(hash_key=True, attr_name=JOB_ID)  # our unique id
    batch_job_id = UnicodeAttribute(null=True, attr_name=JOB_BATCH_JOB_ID)
    celery_task_id = UnicodeAttribute(null=True, attr_name=JOB_CELERY_TASK_ID)
    job_type = JobTypeAttribute(
        null=False,
        attr_name=JOB_TYPE,
    )  # enum of 'standard', 'reactive', 'scheduled'

    tenant_name = UnicodeAttribute(attr_name=JOB_TENANT_NAME)
    customer_name = UnicodeAttribute(attr_name=JOB_CUSTOMER_NAME)

    submitted_at = UnicodeAttribute(attr_name=JOB_SUBMITTED_AT, default=utc_iso)
    status = UnicodeAttribute(attr_name=JOB_STATUS, default=JobState.SUBMITTED.value)

    created_at = UnicodeAttribute(null=True, attr_name=JOB_CREATED_AT)
    started_at = UnicodeAttribute(null=True, attr_name=JOB_STARTED_AT)
    stopped_at = UnicodeAttribute(null=True, attr_name=JOB_STOPPED_AT)

    queue = UnicodeAttribute(null=True, attr_name=JOB_QUEUE)
    definition = UnicodeAttribute(null=True, attr_name=JOB_DEFINITION)
    owner = UnicodeAttribute(null=True, attr_name=JOB_OWNER)

    regions = ListAttribute(default=list, attr_name=JOB_REGIONS)
    rulesets = ListAttribute(default=list, attr_name=JOB_RULESETS)
    rules_to_scan = ListAttribute(default=list, attr_name=JOB_RULES_TO_SCAN)

    reason = UnicodeAttribute(null=True, attr_name=JOB_REASON)
    warnings = ListAttribute(default=list, attr_name=JOB_WARNINGS)

    scheduled_rule_name = UnicodeAttribute(null=True, attr_name=JOB_SCHEDULED_RULE_NAME)
    platform_id = UnicodeAttribute(null=True, attr_name=JOB_PLATFORM_ID)
    affected_license = UnicodeAttribute(
        null=True, attr_name=JOB_AFFECTED_LICENSE
    )
    credentials_key = UnicodeAttribute(
        null=True, attr_name=JOB_CREDENTIALS_KEY
    )
    application_id = UnicodeAttribute(
        null=True, attr_name=JOB_APPLICATION_ID
    )

    ttl = TTLAttribute(null=True, attr_name=JOB_TTL)

    dojo_structure = DojoStructureAttribute(default=dict, attr_name=JOB_DOJO_STRUCTURE)

    customer_name_submitted_at_index = CustomerNameSubmittedAtIndex()
    tenant_name_submitted_at_index = TenantNameSubmittedAtIndex()

    @property
    def is_platform_job(self) -> bool:
        return not self.is_ed_job and bool(self.platform_id)  # Copy from AmbiguousJob

    @property
    def is_ed_job(self) -> bool:
        """
        Check if the job is an event-driven job
        """
        return self.job_type == JobType.REACTIVE

    @property
    def is_succeeded(self) -> bool:
        return self.status == JobState.SUCCEEDED.value

    @property
    def is_failed(self) -> bool:
        return self.status == JobState.FAILED.value

    @property
    def is_finished(self) -> bool:
        return bool(self.stopped_at) and self.status in (
            JobState.SUCCEEDED,
            JobState.FAILED,
        )
