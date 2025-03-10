from pynamodb.attributes import UnicodeAttribute

from modular_sdk.models.pynamongo.attributes import BinaryAttribute
from helpers.constants import CAASEnv
from models import BaseModel


# used only for on-prem
class User(BaseModel):
    class Meta:
        table_name = 'CaaSUsers'
        region = CAASEnv.AWS_REGION.get()

    user_id = UnicodeAttribute(hash_key=True)
    tenants = UnicodeAttribute(null=True)
    customer = UnicodeAttribute(null=True)
    role = UnicodeAttribute(null=True)
    password = BinaryAttribute(null=True)
    latest_login = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute(null=True)

