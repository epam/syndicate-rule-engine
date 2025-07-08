from pynamodb.attributes import UnicodeAttribute, MapAttribute
from pymongo.database import Database


from helpers.constants import CAASEnv
from models import BaseModel

def create_compound_index(db: Database):
    """
    Create a compound index on the 'CaaSResources' collection.
    The index is on 'id', 'name', 'location', 'resource_type', 'tenant_name'.
    """
    db.CaaSResources.create_index(
        [('id', 1), ('name', 1), ('location', 1), ('resource_type', 1), ('tenant_name', 1)],
        name='resource_id_name_location_type_tenant_index',
        unique=True
    )

class Resource(BaseModel):
    class Meta:
        table_name = 'CaaSResources'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute()
    location = UnicodeAttribute()
    # custodian resource type
    resource_type = UnicodeAttribute()
    tenant_name = UnicodeAttribute()

    # all attributes of the resource
    data = MapAttribute(default=dict)

    sync_date = UnicodeAttribute()  # ISO8601
    hash = UnicodeAttribute()  # hash of the resource data