import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute, \
    MapAttribute, BinaryAttribute, BooleanAttribute
from pynamodb.indexes import AllProjection
from typing import Optional

from helpers.constants import ENV_VAR_REGION, CUSTODIAN_TYPE, \
    SCHEDULED_JOB_TYPE
from helpers.time_helper import utc_iso
from models.modular import BaseGSI, BaseSafeUpdateModel

SCHEDULED_JOBS_TABLE_NAME = 'CaaSScheduledJobs'

SJ_ID_ATTR = 'id'
SJ_TYPE_ATTR = 'type'
# SJ_PRINCIPAL_ATTR = 'principal'
SJ_TENANT_NAME = 'tenant_name'
SJ_CREATION_DATE_ATTR = 'creation_date'
SJ_LAST_EXECUTION_TIME_ATTR = 'last_execution_time'
SJ_CONTEXT_ATTR = 'context'
SJ_CUSTOMER_NAME_ATTR = 'customer_name'


class ScheduledJobContext(MapAttribute):
    schedule = UnicodeAttribute(null=True)
    job_state = UnicodeAttribute(null=True)
    scan_regions = ListAttribute(null=True, of=UnicodeAttribute)
    scan_rulesets = ListAttribute(null=True, of=UnicodeAttribute)
    is_enabled = BooleanAttribute(null=True, default_for_new=True)


class CustomerNamePrincipalIndex(BaseGSI):
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
        region = os.environ.get(ENV_VAR_REGION)

    id = UnicodeAttribute(hash_key=True, attr_name=SJ_ID_ATTR)
    type = UnicodeAttribute(range_key=True, default=default_type,
                            attr_name=SJ_TYPE_ATTR)
    tenant_name = UnicodeAttribute(null=True, attr_name=SJ_TENANT_NAME)
    customer_name = UnicodeAttribute(null=True,
                                     attr_name=SJ_CUSTOMER_NAME_ATTR)
    creation_date = UnicodeAttribute(
        null=True, attr_name=SJ_CREATION_DATE_ATTR,
        default_for_new=lambda: utc_iso())
    last_execution_time = UnicodeAttribute(
        null=True, attr_name=SJ_LAST_EXECUTION_TIME_ATTR)
    context = ScheduledJobContext(default=dict, attr_name=SJ_CONTEXT_ATTR)
    customer_name_principal_index = CustomerNamePrincipalIndex()

    @classmethod
    def get_nullable(cls, hash_key, range_key=None, attributes_to_get=None
                     ) -> Optional['ScheduledJob']:
        return super().get_nullable(
            hash_key, range_key or cls.default_type, attributes_to_get)

    def update_with(self, customer: str = None, tenant: str = None,
                    schedule: str = None, scan_regions: list = None,
                    scan_rulesets: list = None,
                    is_enabled: bool = True):
        """
        Sets some common parameters to the object
        """
        if customer:
            self.customer_name = customer
        if tenant:
            self.tenant_name = tenant
        if schedule:
            self.context.schedule = schedule
        if scan_regions:
            self.context.scan_regions = scan_regions
        if scan_rulesets:
            self.context.scan_rulesets = scan_rulesets
        if is_enabled is not None:
            self.context.is_enabled = is_enabled
