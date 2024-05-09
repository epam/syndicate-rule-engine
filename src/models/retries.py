import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute

from models import BaseModel
from helpers.constants import CAASEnv


class Retries(BaseModel):
    class Meta:
        table_name = 'CaaSRetries'
        region = os.environ.get(CAASEnv.AWS_REGION)

    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    permissions = ListAttribute(null=True, default=list)
