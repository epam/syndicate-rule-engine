from pynamodb.attributes import UnicodeAttribute, NumberAttribute, \
    MapAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import CAASEnv
from models import BaseModel


class CustomerNameFromDateIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "customer_name-from_date-index"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True)
    from_date = UnicodeAttribute(range_key=True)


class JobStatistics(BaseModel):
    """
    Model that represents job stats entity.
    """

    class Meta:
        table_name = "CaaSJobStatistics"
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True)
    cloud = UnicodeAttribute()
    from_date = UnicodeAttribute()
    to_date = UnicodeAttribute()
    customer_name = UnicodeAttribute()
    succeeded = NumberAttribute(null=True, default=0)
    failed = NumberAttribute(null=True, default=0)
    last_scan_date = UnicodeAttribute(null=True)
    tenants = MapAttribute(default=dict)
    reason = MapAttribute(default=dict)
    scanned_regions = MapAttribute(default=dict)

    customer_name_from_date_index = CustomerNameFromDateIndex()
