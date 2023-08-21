import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute, \
    TTLAttribute

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel


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
    scan_regions = ListAttribute(null=True)
    scan_rulesets = ListAttribute(null=True)
    reason = UnicodeAttribute(null=True)
    scheduled_rule_name = UnicodeAttribute(null=True)
    ttl = TTLAttribute(null=True)
    rules_to_scan = ListAttribute(default=list)
