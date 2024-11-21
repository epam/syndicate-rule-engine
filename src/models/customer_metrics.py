from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import CAASEnv
from models import BaseGSI, BaseModel

TM_ID_ATTR = 'id'
TM_DATE_ATTR = 'd'
TM_TYPE_ATTR = 't'
TM_CUSTOMER_ATTR = 'c'
TM_DEFINING_ATTRIBUTE_ATTR = 'da'
TM_OUTDATED_TENANTS_ATTR = 'ot'


# TODO: remove
class CustomerMetrics(BaseModel):
    class Meta:
        table_name = 'CaaSCustomerMetrics'
        region = CAASEnv.AWS_REGION.get()

    customer = UnicodeAttribute(hash_key=True, attr_name=TM_CUSTOMER_ATTR)
    date = UnicodeAttribute(range_key=True, attr_name=TM_DATE_ATTR)  # ISO8601
    type = UnicodeAttribute(attr_name=TM_TYPE_ATTR)  # OVERVIEW, COMPLIANCE
    azure = MapAttribute(default=dict)
    aws = MapAttribute(default=dict)
    google = MapAttribute(default=dict)
    average = MapAttribute(default=dict)
    outdated_tenants = ListAttribute(
        default=list, attr_name=TM_OUTDATED_TENANTS_ATTR
    )
