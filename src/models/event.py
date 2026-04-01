from pynamodb.attributes import (
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
)

from helpers.constants import Env
from models import BaseModel

E_PARTITION_ATTR = "p"
E_TIMESTAMP_ATTR = "t"
E_EVENTS = "e"
E_VENDOR = "v"
E_TTL = "ttl"


class EventRecordAttribute(MapAttribute):
    """PynamoDB MapAttribute for storing event records inside Event items."""

    cloud = UnicodeAttribute()
    region_name = UnicodeAttribute()
    source_name = UnicodeAttribute()
    event_name = UnicodeAttribute()
    platform_id = UnicodeAttribute(null=True)
    account_id = UnicodeAttribute(null=True)
    tenant_name = UnicodeAttribute(null=True)


class Event(BaseModel):
    """
    Persistence Model that represents registry of events.
    """

    class Meta:  # type: ignore
        table_name = "SREEvents"
        region = Env.AWS_REGION.get()

    partition = NumberAttribute(
        hash_key=True, attr_name=E_PARTITION_ATTR
    )  # random 0..N
    timestamp = NumberAttribute(range_key=True, attr_name=E_TIMESTAMP_ATTR)
    events = ListAttribute(default=list, of=EventRecordAttribute, attr_name=E_EVENTS)
    vendor = UnicodeAttribute(attr_name=E_VENDOR)
    ttl = NumberAttribute(null=True, attr_name=E_TTL)
