from pynamodb.attributes import MapAttribute, UnicodeAttribute

from helpers.constants import COMPOUND_KEYS_SEPARATOR, CAASEnv, ReportType
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
    s3_path = UnicodeAttribute(null=True, default=None, attr_name='l')
    customer = UnicodeAttribute(attr_name='c')
    # todo add ttl
    # todo add link to s3

    @property
    def type(self) -> ReportType:
        return ReportType(self.key.split(COMPOUND_KEYS_SEPARATOR, 1)[0])

    @property
    def is_fetched(self) -> bool:
        if not self.s3_path:
            return True
        # s3 path exists
        return bool(self.data)
