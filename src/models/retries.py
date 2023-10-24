import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute

from models.modular import BaseModel
from helpers.constants import ENV_VAR_REGION


class Retries(BaseModel):
    class Meta:
        table_name = 'CaaSRetries'
        region = os.environ.get(ENV_VAR_REGION)

    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    permissions = ListAttribute(null=True, default=list)
