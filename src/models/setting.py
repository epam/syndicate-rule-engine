from pynamodb.attributes import UnicodeAttribute
from modular_sdk.models.pynamongo.attributes import DynamicAttribute

from helpers.constants import CAASEnv
from models import BaseModel


class Setting(BaseModel):
    class Meta:
        table_name = 'CaaSSettings'
        region = CAASEnv.AWS_REGION.get()
        max_retry_attempts = 5

    name = UnicodeAttribute(hash_key=True)
    value = DynamicAttribute()
