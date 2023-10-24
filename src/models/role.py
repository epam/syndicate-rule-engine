import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute

from models.modular import BaseModel, BaseGSI
from helpers.constants import ENV_VAR_REGION


class Role(BaseModel):
    class Meta:
        table_name = 'CaaSRoles'
        region = os.environ.get(ENV_VAR_REGION)

    customer = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)
    expiration = UnicodeAttribute(null=True)  # ISO8601, valid to date
    policies = ListAttribute(null=True, default=list)
    resource = ListAttribute(null=True, default=list)
