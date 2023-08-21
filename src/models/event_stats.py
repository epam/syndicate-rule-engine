import os

from pynamodb.attributes import UnicodeAttribute, MapAttribute

from helpers.constants import ENV_VAR_REGION
from helpers.time_helper import utc_iso
from models.modular import BaseModel

ES_TENANT = 't'
ES_TIMESTAMP_START = 'ets'
ES_TIMESTAMP_END = 'ete'
ES_STATISTICS = 'st'
ES_COLLECTED_AT = 'c'

ES_STATUS_COLLECTED = 'COL'


class EventStatistics(BaseModel):
    """
    Persistence Model that represents registry of events.
    """

    class Meta:
        table_name = "CaaSEventStatistics"
        region = os.environ.get(ENV_VAR_REGION)

    tenant = UnicodeAttribute(hash_key=True, attr_name=ES_TENANT)
    start = UnicodeAttribute(
        range_key=True, attr_name=ES_TIMESTAMP_START
    )  # epoch timestamp
    end = UnicodeAttribute(attr_name=ES_TIMESTAMP_END)

    statistics = MapAttribute(default=dict, attr_name=ES_STATISTICS)
    collected_at = UnicodeAttribute(default=utc_iso, attr_name=ES_COLLECTED_AT)
