import os

from pynamodb.attributes import UnicodeAttribute

from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel


class User(BaseModel):
    class Meta:
        table_name = 'CaaSUsers'
        region = os.environ.get(ENV_VAR_REGION)

    user_id = UnicodeAttribute(hash_key=True)
    tenants = UnicodeAttribute(null=True)
    customer = UnicodeAttribute(null=True)
    role = UnicodeAttribute(null=True)
    password = UnicodeAttribute(null=True)
    latest_login = UnicodeAttribute(null=True)

