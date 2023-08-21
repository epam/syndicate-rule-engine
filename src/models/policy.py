import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute

from models.modular import BaseModel
from helpers.constants import ENV_VAR_REGION


class Policy(BaseModel):
    class Meta:
        table_name = 'CaaSPolicies'
        region = os.environ.get(ENV_VAR_REGION)

    customer = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    permissions = ListAttribute(null=True, default=list)
