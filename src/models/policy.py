from pynamodb.attributes import UnicodeAttribute, ListAttribute

from helpers.constants import Env, PolicyEffect
from models import BaseModel


class Policy(BaseModel):
    class Meta:
        table_name = 'SREPolicies'
        region = Env.AWS_REGION.get()

    customer = UnicodeAttribute(hash_key=True)  # todo hot partition?
    name = UnicodeAttribute(range_key=True)
    description = UnicodeAttribute(null=True)
    permissions = ListAttribute(default=list, of=UnicodeAttribute)
    tenants = ListAttribute(default=list, of=UnicodeAttribute)
    effect = UnicodeAttribute(default=PolicyEffect.DENY.value)
