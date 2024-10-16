from pynamodb.attributes import UnicodeAttribute, ListAttribute

from helpers.constants import CAASEnv
from models import BaseModel


class Retries(BaseModel):
    class Meta:
        table_name = 'CaaSRetries'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    permissions = ListAttribute(null=True, default=list)
