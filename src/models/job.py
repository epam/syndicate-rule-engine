import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute, TTLAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel, BaseGSI

TENANT_DISPLAY_NAME_ATTR = 'tenant_display_name'
SUBMITTED_AT_ATTR = 'submitted_at'


class TenantDisplayNameSubmittedAtIndex(BaseGSI):
    class Meta:
        index_name = f'{TENANT_DISPLAY_NAME_ATTR}-{SUBMITTED_AT_ATTR}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant_display_name = UnicodeAttribute(hash_key=True)
    submitted_at = UnicodeAttribute(range_key=True)


class CustomerDisplayNameIndex(BaseGSI):
    class Meta:
        index_name = "customer-display-name"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_display_name = UnicodeAttribute(hash_key=True)
    submitted_at = UnicodeAttribute(range_key=True)


class Job(BaseModel):
    """
    Model that represents job entity.
    """

    class Meta:
        table_name = "CaaSJobs"
        region = os.environ.get(ENV_VAR_REGION)

    job_id = UnicodeAttribute(hash_key=True)
    tenant_display_name = UnicodeAttribute(null=True)
    customer_display_name = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute(null=True)
    started_at = UnicodeAttribute(null=True)
    stopped_at = UnicodeAttribute(null=True)
    submitted_at = UnicodeAttribute(null=True)
    status = UnicodeAttribute(null=True)
    job_queue = UnicodeAttribute(null=True)
    job_definition = UnicodeAttribute(null=True)
    job_owner = UnicodeAttribute(null=True)
    scan_regions = ListAttribute(null=True, default=list)
    scan_rulesets = ListAttribute(null=True, default=list)
    reason = UnicodeAttribute(null=True)
    scheduled_rule_name = UnicodeAttribute(null=True)
    ttl = TTLAttribute(null=True)
    rules_to_scan = ListAttribute(default=list)

    customer_display_name_index = CustomerDisplayNameIndex()
    tenant_display_name_index = TenantDisplayNameSubmittedAtIndex()
