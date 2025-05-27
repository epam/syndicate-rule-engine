from typing import TYPE_CHECKING

from pynamodb.attributes import UnicodeAttribute, ListAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    CAASEnv,
    Cloud,
    ReportType,
)
from models import BaseModel
from modular_sdk.models.pynamongo.attributes import BinaryAttribute

if TYPE_CHECKING:
    from modular_sdk.models.tenant import Tenant

    from services.platform_service import Platform


class CustomerEndIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'c-e-index'
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name='c')
    end = UnicodeAttribute(range_key=True, attr_name='e')


class ReportMetrics(BaseModel):
    class Meta:
        table_name = 'CaaSReportMetrics'
        region = CAASEnv.AWS_REGION.get()

    # type#customer#project#cloud#tenant#region
    key = UnicodeAttribute(hash_key=True, attr_name='k')
    # report data is relevant as of this date
    end = UnicodeAttribute(range_key=True, attr_name='e')
    # start date for a reporting period (if "period" can be applied to the sort of data)
    start = UnicodeAttribute(null=True, default=None, attr_name='s')

    # date when the report was created, it can be different from the date
    # as of which data is collected. In case we generate a report retroactively
    _created_at = UnicodeAttribute(null=True, attr_name='cd')

    customer = UnicodeAttribute(attr_name='c')

    # holds tenants that were involved in collecting this report
    tenants = ListAttribute(of=UnicodeAttribute, default=list, attr_name='t')

    customer_end_index = CustomerEndIndex()

    # either of these can be stored
    data = BinaryAttribute(null=True, attr_name='b')

    # url to payload
    s3_url = UnicodeAttribute(null=True, default=None, attr_name='l')

    content_type = UnicodeAttribute(null=True, attr_name='ct')
    content_encoding = UnicodeAttribute(null=True, attr_name='ce')

    @property
    def created_at(self) -> str:
        if self._created_at:
            return self._created_at
        return self.end

    @property
    def type(self) -> ReportType:
        return ReportType(self.key.split(COMPOUND_KEYS_SEPARATOR, 1)[0])

    @property
    def project(self) -> str | None:
        return self.key.split(COMPOUND_KEYS_SEPARATOR, 3)[2] or None

    @property
    def cloud(self) -> Cloud | None:
        item = self.key.split(COMPOUND_KEYS_SEPARATOR, 4)[3] or None
        if not item:
            return
        return Cloud(item)

    @property
    def tenant(self) -> str | None:
        if self.cloud is Cloud.KUBERNETES:
            return
        return self.key.split(COMPOUND_KEYS_SEPARATOR, 5)[4] or None

    @property
    def platform_id(self) -> str | None:
        if self.cloud is Cloud.KUBERNETES:
            return self.key.split(COMPOUND_KEYS_SEPARATOR, 5)[4] or None
        return

    @property
    def entity(self) -> str:
        """
        Returns a domain (entity) that represents scope of this report
        """
        if t := self.tenant:
            return t
        if pl := self.platform_id:
            return pl
        if p := self.project:
            return p
        return self.customer

    @staticmethod
    def build_key(
        type_: ReportType,
        customer: str,
        project: str = '',
        cloud: str = '',
        tenant_or_platform: str = '',
        region: str = '',
    ) -> str:
        return COMPOUND_KEYS_SEPARATOR.join(
            (type_.value, customer, project, cloud, tenant_or_platform, region)
        )

    @classmethod
    def build_key_for_customer(cls, type_: ReportType, customer: str) -> str:
        return cls.build_key(type_=type_, customer=customer)

    @classmethod
    def build_key_for_project(
        cls, type_: ReportType, customer: str, project: str
    ) -> str:
        return cls.build_key(type_=type_, customer=customer, project=project)

    @classmethod
    def build_key_for_platform(
        cls, type_: ReportType, platform: 'Platform'
    ) -> str:
        return cls.build_key(
            type_=type_,
            customer=platform.customer,
            cloud=Cloud.KUBERNETES.value,
            tenant_or_platform=platform.id,
        )

    @classmethod
    def build_key_for_tenant(cls, type_: ReportType, tenant: 'Tenant') -> str:
        cl = Cloud.parse(tenant.cloud)
        assert cl, 'Not supported tenant came'
        return cls.build_key(
            type_=type_,
            customer=tenant.customer_name,
            cloud=cl.value,
            tenant_or_platform=tenant.name,
        )
