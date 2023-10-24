import os

from pynamodb.attributes import UnicodeAttribute

from models.modular import BaseModel
from modular_sdk.models.pynamodb_extension.base_model import DynamicAttribute
from helpers.constants import ENV_VAR_REGION


class Setting(BaseModel):
    class Meta:
        table_name = 'CaaSSettings'
        region = os.environ.get(ENV_VAR_REGION)

    name = UnicodeAttribute(hash_key=True, attr_name='name')
    value = DynamicAttribute(attr_name='value')
