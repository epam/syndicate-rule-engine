import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute, \
    NumberAttribute, MapAttribute, BooleanAttribute

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel

PERMITTED_ATTACHMENT = 'permitted'
PROHIBITED_ATTACHMENT = 'prohibited'
ALLOWED_ATTACHMENT_MODELS = (PERMITTED_ATTACHMENT, PERMITTED_ATTACHMENT)


class AllowanceAttribute(MapAttribute):
    time_range = UnicodeAttribute(null=True)
    job_balance = NumberAttribute(null=True)
    balance_exhaustion_model = UnicodeAttribute(null=True)


class EventDriven(MapAttribute):
    active = BooleanAttribute(null=True)
    quota = NumberAttribute(null=True)  # in minutes
    last_execution = UnicodeAttribute(null=True)


class License(BaseModel):
    class Meta:
        table_name = 'CaaSLicenses'
        region = os.environ.get(ENV_VAR_REGION)

    license_key = UnicodeAttribute(hash_key=True)
    # you do not use DynamicMapAttribute - it contains a bug in pynamodb==5.2.1
    # customers = DynamicMapAttribute(default=dict)
    customers = MapAttribute(default=dict)
    expiration = UnicodeAttribute(null=True)  # ISO8601
    ruleset_ids = ListAttribute(null=True, default=list)
    latest_sync = UnicodeAttribute(null=True)  # ISO8601
    # applied_only_for_descendants = ListAttribute(null=True)
    allowance = AllowanceAttribute(null=True, default=dict)
    event_driven = EventDriven(default=dict, null=True)
