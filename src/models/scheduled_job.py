from pynamodb.attributes import UnicodeAttribute, ListAttribute, \
    MapAttribute, BooleanAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.settings import OperationSettings

from helpers.constants import CAASEnv, CUSTODIAN_TYPE, \
    SCHEDULED_JOB_TYPE
from helpers.time_helper import utc_iso
from models import BaseSafeUpdateModel

SCHEDULED_JOBS_TABLE_NAME = 'CaaSScheduledJobs'

SJ_ID_ATTR = 'id'
SJ_TYPE_ATTR = 'type'
SJ_TENANT_NAME = 'tenant_name'
SJ_CREATION_DATE_ATTR = 'creation_date'
SJ_LAST_EXECUTION_TIME_ATTR = 'last_execution_time'
SJ_CONTEXT_ATTR = 'context'
SJ_CUSTOMER_NAME_ATTR = 'customer_name'


class ScheduledJobContext(MapAttribute):
    schedule = UnicodeAttribute(null=True)
    # job_state = BinaryAttribute(null=True)  # no need to load this attr
    scan_regions = ListAttribute(null=True, of=UnicodeAttribute, default=list)
    scan_rulesets = ListAttribute(null=True, of=UnicodeAttribute, default=list)
    is_enabled = BooleanAttribute(null=True, default_for_new=True)


class CustomerNamePrincipalIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{SJ_CUSTOMER_NAME_ATTR}-{SJ_TENANT_NAME}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer_name = UnicodeAttribute(hash_key=True,
                                     attr_name=SJ_CUSTOMER_NAME_ATTR)
    tenant_name = UnicodeAttribute(range_key=True, attr_name=SJ_TENANT_NAME)


class ScheduledJob(BaseSafeUpdateModel):
    """
    Model that represents job entity.
    """
    default_type = f'{CUSTODIAN_TYPE}:{SCHEDULED_JOB_TYPE}'

    class Meta:
        table_name = SCHEDULED_JOBS_TABLE_NAME
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name=SJ_ID_ATTR)
    type = UnicodeAttribute(range_key=True, default=default_type,
                            attr_name=SJ_TYPE_ATTR)
    tenant_name = UnicodeAttribute(null=True, attr_name=SJ_TENANT_NAME)
    customer_name = UnicodeAttribute(null=True,
                                     attr_name=SJ_CUSTOMER_NAME_ATTR)
    creation_date = UnicodeAttribute(
        null=True, attr_name=SJ_CREATION_DATE_ATTR,
        default_for_new=utc_iso)
    last_execution_time = UnicodeAttribute(
        null=True, attr_name=SJ_LAST_EXECUTION_TIME_ATTR)
    context = ScheduledJobContext(default=dict, attr_name=SJ_CONTEXT_ATTR)
    customer_name_principal_index = CustomerNamePrincipalIndex()

    @classmethod
    def get_nullable(
        cls,
        hash_key,
        range_key=None,
        consistent_read: bool = False,
        attributes_to_get=None,
        settings: OperationSettings = OperationSettings.default,
    ) -> 'ScheduledJob | None':
        return super().get_nullable(
            hash_key=hash_key,
            range_key=range_key or cls.default_type,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get,
            settings=settings
        )