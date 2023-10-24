import os

from modular_sdk.models.pynamodb_extension.base_model import DynamicAttribute
from pynamodb.attributes import UnicodeAttribute

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel


class Setting(BaseModel):
    class Meta:
        table_name = 'CaaSSettings'
        region = os.environ.get(ENV_VAR_REGION)
        max_retry_attempts = 5

    name = UnicodeAttribute(hash_key=True)
    value = DynamicAttribute()
    # this attribute has a problem. It does not perform deserialization
    # for binary data it causes a problem when the returned data is not
    # base64 decoded
