import os

from pynamodb.attributes import UnicodeAttribute, NumberAttribute, \
    MapAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel, BaseGSI


class CustomerNameFromDateIndex(BaseGSI):
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
        region = os.environ.get(ENV_VAR_REGION)

    id = UnicodeAttribute(hash_key=True)
    cloud = UnicodeAttribute()
    from_date = UnicodeAttribute()
    to_date = UnicodeAttribute()
    customer_name = UnicodeAttribute()
    succeeded = NumberAttribute(null=True, default=0)
    failed = NumberAttribute(null=True, default=0)
    last_scan_date = UnicodeAttribute(null=True)
    tenants = MapAttribute(null=True)

    customer_name_from_date_index = CustomerNameFromDateIndex()
