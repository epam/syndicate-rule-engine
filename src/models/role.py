from pynamodb.attributes import UnicodeAttribute, ListAttribute

from helpers.constants import CAASEnv
from helpers.time_helper import utc_datetime
from models import BaseModel


class Role(BaseModel):
    class Meta:
        table_name = 'CaaSRoles'
        region = CAASEnv.AWS_REGION.get()

    customer = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    expiration = UnicodeAttribute(null=True)  # ISO8601, valid to date
    policies = ListAttribute(default=list)
    description = UnicodeAttribute(null=True)

    def is_expired(self) -> bool:
        if not self.expiration:
            return False
        return utc_datetime() >= utc_datetime(self.expiration)
