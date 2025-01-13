from pynamodb.attributes import MapAttribute, UnicodeAttribute

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    CAASEnv,
    Cloud,
    ReportType,
)
from models import BaseModel


# TODO: optimize for mongo queries, make start and end Date objects instead of strings
class ReportMetrics(BaseModel):
    class Meta:
        table_name = 'CaaSReportMetrics'
        region = CAASEnv.AWS_REGION.get()

    # type#customer#project#cloud#tenant#region
    key = UnicodeAttribute(hash_key=True, attr_name='k')
    end = UnicodeAttribute(range_key=True, attr_name='e')
    start = UnicodeAttribute(null=True, default=None, attr_name='s')
    data = MapAttribute(default=dict, attr_name='d')
    s3_url = UnicodeAttribute(null=True, default=None, attr_name='l')
    customer = UnicodeAttribute(attr_name='c')
    # todo add ttl
    # todo add link to s3

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
    def region(self) -> str | None:
        return self.key.split(COMPOUND_KEYS_SEPARATOR)[5] or None

    @property
    def is_fetched(self) -> bool:
        if not self.s3_url:
            return True
        # s3 path exists
        return bool(self.data.as_dict())

    @property
    def entity(self) -> str:
        """
        Returns a domain (entity) that represents scope of this report
        """
        if t := self.tenant:
            return t
        if pl := self.platform_id:
            return pl
        if c := self.cloud:
            return c.value
        if p := self.project:
            return p
        return self.customer

