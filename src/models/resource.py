import hashlib

from pynamodb.attributes import UnicodeAttribute, MapAttribute
from pynamodb.expressions.update import Action
from pymongo.database import Database
import msgspec.json

from helpers.constants import CAASEnv, Cloud
from helpers.log_helper import get_logger
from models import BaseModel

_LOG = get_logger(__name__)


def create_compound_index(db: Database):
    """
    Create a compound index on the 'CaaSResources' collection.
    The index is on 'id', 'name', 'location', 'resource_type', 'tenant_name'.
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
    customer_name = UnicodeAttribute()

    # all attributes of the resource
    _data = MapAttribute(default=dict, attr_name='data')

    sync_date = UnicodeAttribute()  # ISO8601
    _hash = UnicodeAttribute(attr_name='sha256')

    def __init__(
        self,
        id: str,
        name: str,
        location: str,
        resource_type: str,
        tenant_name: str,
        customer_name: str,
        data: dict,
        sync_date: str,
    ):
        super().__init__(
            id=id,
            name=name,
            location=location,
            resource_type=resource_type,
            tenant_name=tenant_name,
            customer_name=customer_name,
            _data=data,
            sync_date=sync_date,
        )
        self._hash = self._compute_hash(data)

    @staticmethod
    def _compute_hash(data: dict) -> str:
        """
        Computes SHA256 hash of the resource data using deterministic JSON encoding.
        :param data: Dictionary to hash
        :return: SHA256 hash string
        """
        encoded_data = msgspec.json.encode(data, order='deterministic')
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
        if not self._hash:
            self._hash = self._compute_hash(self._data.as_dict())
        return self._hash

    def save(self, *args, **kwargs):
        return super().save(*args, **kwargs)

    @property
    def cloud(self) -> Cloud:
        """
        Returns cloud name based on resource_type
        :return: cloud name or None if resource_type is not specified
        """
        return Cloud(self.resource_type.split('.')[0].upper())

    def __str__(self):
        return (
            f'Resource(id={self.id}, name={self.name}, '
            f'location={self.location}, resource_type={self.resource_type}, '
            f'tenant_name={self.tenant_name}, customer_name={self.customer_name})'
        )

    def __repr__(self):
        return (
            f'Resource(id={self.id}, name={self.name}, '
            f'location={self.location}, resource_type={self.resource_type}, '
            f'tenant_name={self.tenant_name}, customer_name={self.customer_name})'
        )
