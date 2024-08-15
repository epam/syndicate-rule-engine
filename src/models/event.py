from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    ListAttribute, NumberAttribute

from helpers.constants import CAASEnv
from models import BaseModel

E_PARTITION_ATTR = 'p'
E_TIMESTAMP_ATTR = 't'
E_EVENTS = 'e'
E_VENDOR = 'v'
E_TTL = 'ttl'


class Event(BaseModel):
    """
    Persistence Model that represents registry of events.
    """

    class Meta:
        table_name = "CaaSEvents"
        region = CAASEnv.AWS_REGION.get()

    partition = NumberAttribute(hash_key=True,
                                attr_name=E_PARTITION_ATTR)  # random 0..N
    timestamp = NumberAttribute(range_key=True, attr_name=E_TIMESTAMP_ATTR)
    events = ListAttribute(default=list, of=MapAttribute, attr_name=E_EVENTS)
    vendor = UnicodeAttribute(attr_name=E_VENDOR)
    ttl = NumberAttribute(null=True, attr_name=E_TTL)
