from modular_sdk.models.pynamongo.attributes import BinaryAttribute
from pynamodb.attributes import UnicodeAttribute, MapAttribute, BooleanAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import Env
from helpers.time_helper import utc_iso
from models import BaseSafeUpdateModel


class CustomerNameTypeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'cn-t-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True, attr_name='cn')
    typ = UnicodeAttribute(range_key=True, attr_name='t')


class TypeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 't-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    typ = UnicodeAttribute(hash_key=True, attr_name='t')


class ScheduledJob(BaseSafeUpdateModel):
    """
    Allows to register scheduled executions of some jobs per customer
    """

    class Meta:
        table_name = 'SREScheduledJobs'
        region = Env.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name='id')
    typ = UnicodeAttribute(attr_name='t')

    name = UnicodeAttribute(null=True, attr_name='n')
    enabled = BooleanAttribute(default=True, attr_name='e')
    creation_date = UnicodeAttribute(attr_name='cd', default_for_new=utc_iso)
    customer_name = UnicodeAttribute(null=True, attr_name='cn')
    tenant_name = UnicodeAttribute(null=True, attr_name='tn')
    description = UnicodeAttribute(null=True, attr_name='d')

    meta = MapAttribute(default=dict, attr_name='m')  # depends on job type

    # celery's ScheduleEntry
    celery = BinaryAttribute(null=True)

    customer_name_typ_index = CustomerNameTypeIndex()
    typ_index = TypeIndex()
