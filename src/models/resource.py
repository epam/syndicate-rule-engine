import hashlib

from pynamodb.attributes import UnicodeAttribute, MapAttribute, NumberAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pymongo.database import Database
import msgspec

from helpers.constants import CAASEnv, Cloud, COMPOUND_KEYS_SEPARATOR
from helpers.log_helper import get_logger
from models import BaseModel

_LOG = get_logger(__name__)


def create_caasresources_indexes(db: Database):
    """
    Create a compound index on the 'CaaSResources' collection.
    The index is on 'id', 'name', 'location', 'resource_type', 'tenant_name'.

    Create a sparse index on the 'arn' field.
    """
    db.CaaSResources.create_index(
        [
            ('customer_name', 1),
            ('tenant_name', 1),
            ('resource_type', 1),
            ('location', 1),
            ('name', 1),
            ('id', 1),
        ],
        name='resource_id_name_location_type_tenant_index',
        unique=True,
    )
    db.CaaSResources.create_index(
        [('arn', 1)],
        name='resource_arn_index',
        sparse=True,
    )

class Resource(BaseModel):
    class Meta:
        table_name = 'CaaSResources'
        region = CAASEnv.AWS_REGION.get()

    # account_id#location#resource_type#id
    did = UnicodeAttribute(hash_key=True)


    id = UnicodeAttribute()
    name = UnicodeAttribute()
    location = UnicodeAttribute()

    # custodian resource type with cloud prefix
    resource_type = UnicodeAttribute()

    tenant_name = UnicodeAttribute()
    customer_name = UnicodeAttribute()

    arn = UnicodeAttribute(null=True)

    # all attributes of the resource
    _data = MapAttribute(default=dict, attr_name='data')

    sync_date = NumberAttribute() # timestamp
    _hash = UnicodeAttribute(attr_name='sha256')

    encoder = msgspec.json.Encoder()

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        did = COMPOUND_KEYS_SEPARATOR.join([
            kwargs.get('account_id', ''),
            kwargs.get('location', ''),
            kwargs.get('resource_type', ''),
            kwargs.get('id', ''),
        ])
        kwargs['did'] = did
        kwargs.pop('account_id', None)
        
        super().__init__(
            *args,
            **kwargs,
        )
        if self._data is not None:
            self._hash = self._compute_hash(self._data.as_dict())

    @classmethod
    def _compute_hash(cls, data: dict) -> str:
        """
        Computes SHA256 hash of the resource data using JSON encoding.
        :param data: Dictionary to hash
        :return: SHA256 hash string
        """
        encoded_data = cls.encoder.encode(data)
        return hashlib.sha256(encoded_data).hexdigest()

    @property
    def data(self) -> dict:
        return self._data.as_dict()

    @data.setter
    def data(self, value: dict):
        self._hash = self._compute_hash(value)
        self._data = value

    @property
    def hash(self) -> str:
        return self._hash

    def save(self, *args, **kwargs):
        return super().save(*args, **kwargs)

    @property
    def cloud(self) -> Cloud:
        return Cloud[self.resource_type.split('.')[0].upper()]

    def __repr__(self):
        return (
            f'Resource(id={self.id}, name={self.name}, '
            f'location={self.location}, resource_type={self.resource_type}, '
            f'tenant_name={self.tenant_name}, customer_name={self.customer_name})'
        )
