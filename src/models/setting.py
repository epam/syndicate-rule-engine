from modular_sdk.models.pynamodb_extension.base_model import DynamicAttribute
from pynamodb.attributes import UnicodeAttribute

from helpers.constants import CAASEnv
from models import BaseModel


class Setting(BaseModel):
    class Meta:
        table_name = 'CaaSSettings'
        region = CAASEnv.AWS_REGION.get()
        max_retry_attempts = 5

    name = UnicodeAttribute(hash_key=True)
    value = DynamicAttribute()
    # this attribute has a problem. It does not perform deserialization
    # for binary data it causes a problem when the returned data is not
    # base64 decoded
