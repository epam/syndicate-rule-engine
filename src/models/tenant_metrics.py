from pynamodb.attributes import (
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
)
from pynamodb.indexes import AllProjection

from helpers.constants import CAASEnv
from models import BaseGSI, BaseModel

TM_ID_ATTR = 'id'
TM_TENANT_ATTR = 'tn'
TM_DATE_ATTR = 'd'
TM_TYPE_ATTR = 't'
TM_CUSTOMER_ATTR = 'c'
TM_DEFINING_ATTRIBUTE_ATTR = 'da'
TM_OUTDATED_TENANTS_ATTR = 'ot'


class CustomerDateIndex(BaseGSI):
    class Meta:
        index_name = f'{TM_CUSTOMER_ATTR}-{TM_DATE_ATTR}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=TM_CUSTOMER_ATTR)
    date = UnicodeAttribute(range_key=True, attr_name=TM_DATE_ATTR)


class TenantDateIndex(BaseGSI):
    class Meta:
        index_name = f'{TM_TENANT_ATTR}-{TM_DATE_ATTR}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant_display_name = UnicodeAttribute(
        hash_key=True, attr_name=TM_TENANT_ATTR
    )
    date = UnicodeAttribute(range_key=True, attr_name=TM_DATE_ATTR)


# TODO: remove
class TenantMetrics(BaseModel):
    class Meta:
        table_name = 'CaaSTenantMetrics'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name=TM_ID_ATTR)
    tenant_display_name = UnicodeAttribute(
        range_key=True, attr_name=TM_TENANT_ATTR
    )
    customer = UnicodeAttribute(attr_name=TM_CUSTOMER_ATTR)
    date = UnicodeAttribute(attr_name=TM_DATE_ATTR)  # ISO8601
    type = UnicodeAttribute(attr_name=TM_TYPE_ATTR)
    azure = MapAttribute(default=dict)
    aws = MapAttribute(default=dict)
    google = MapAttribute(default=dict)
    defining_attribute = NumberAttribute(
        null=True, attr_name=TM_DEFINING_ATTRIBUTE_ATTR
    )
    outdated_tenants = ListAttribute(
        default=list, attr_name=TM_OUTDATED_TENANTS_ATTR
    )

    customer_date_index = CustomerDateIndex()
    tenant_date_index = TenantDateIndex()
