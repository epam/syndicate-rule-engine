from pynamodb.attributes import UnicodeAttribute, MapAttribute, ListAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import CAASEnv
from models import BaseModel, BaseGSI

TM_ID_ATTR = 'id'
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


class CustomerMetrics(BaseModel):
    class Meta:
        table_name = 'CaaSCustomerMetrics'
        region = CAASEnv.AWS_REGION.get()
    id = UnicodeAttribute(hash_key=True, attr_name=TM_ID_ATTR)
    customer = UnicodeAttribute(attr_name=TM_CUSTOMER_ATTR)
    date = UnicodeAttribute(attr_name=TM_DATE_ATTR)  # ISO8601
    type = UnicodeAttribute(attr_name=TM_TYPE_ATTR)  # OVERVIEW, COMPLIANCE
    azure = MapAttribute(null=True, default=dict)
    aws = MapAttribute(null=True, default=dict)
    google = MapAttribute(null=True, default=dict)
    average = MapAttribute(null=True, default=dict)
    outdated_tenants = ListAttribute(null=True, default=[],
                                     attr_name=TM_OUTDATED_TENANTS_ATTR)

    customer_date_index = CustomerDateIndex()
