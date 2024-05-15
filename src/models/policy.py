import os

from pynamodb.attributes import UnicodeAttribute, ListAttribute

from models import BaseModel
from enum import Enum
from helpers.constants import CAASEnv


class PolicyEffect(str, Enum):
    ALLOW = 'allow'
    DENY = 'deny'


class Policy(BaseModel):
    class Meta:
        table_name = 'CaaSPolicies'
        region = os.environ.get(CAASEnv.AWS_REGION)

    customer = UnicodeAttribute(hash_key=True)  # todo hot partition?
    name = UnicodeAttribute(range_key=True)
    description = UnicodeAttribute(null=True)
    permissions = ListAttribute(default=list, of=UnicodeAttribute)
    tenants = ListAttribute(default=list, of=UnicodeAttribute)
    effect = UnicodeAttribute(default=PolicyEffect.DENY.value)
