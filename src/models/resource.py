import hashlib

import msgspec
from pymongo.database import Database
from pynamodb.attributes import MapAttribute, NumberAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    Env,
    Cloud,
    ResourcesCollectorType,
)
from helpers.log_helper import get_logger
from models import BaseModel

_LOG = get_logger(__name__)


def create_resources_indexes(db: Database) -> tuple[str, ...]:
    collection = db.get_collection(Resource.Meta.table_name)
    name1 = 'aid_1_l_1_rt_1_i_1'
    indexes = collection.index_information()
    if name1 not in indexes:
        _LOG.info(f'Index {name1} does not exist yet')
        collection.create_index(
            [('aid', 1), ('l', 1), ('rt', 1), ('i', 1)], name=name1, unique=True
        )

    name2 = 'cn_1_tn_1_l_1_rt_1_n_1_i_1'
    if name2 not in indexes:
        _LOG.info(f'Index {name2} does not exist yet')
        collection.create_index(
            [('cn', 1), ('tn', 1), ('l', 1), ('rt', 1), ('n', 1), ('i', 1)],
            name=name2,
        )

    return name1, name2


class ResourceARNIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'arn-index'
        projection = AllProjection()

    # TODO: make it a sparse index and maybe unique?
    arn = UnicodeAttribute(hash_key=True, attr_name='arn')


class Resource(BaseModel):
    encoder = msgspec.json.Encoder()

    class Meta:
        table_name = 'SREResources'
        region = Env.AWS_REGION.get()

    # just to please our DynamoDB's inheritance. It looks like:
    # account_id#location#resource_type#id
    # where:
    # - account id is AWS account id or Azure subscription id or GCP project id
    # - location is SRE's entity that represents region against which Cloud Custodian policy was executed.
    #   For AZURE/GOOGLE/KUBERNETES it is literally "global". For AWS it's either a real region name or "global" for S3, IAM and other    global services
    # - resource_type is resource type of Cloud Custodian containing cloud prefix
    # - id is resource's unique identifier, taken from Cloud Custodian
    did = UnicodeAttribute(hash_key=True, attr_name='id')
    # what does "did" stand for? probably DynamoDB ID

    account_id = UnicodeAttribute(attr_name='aid')
    location = UnicodeAttribute(attr_name='l')
    resource_type = UnicodeAttribute(attr_name='rt')
    id = UnicodeAttribute(attr_name='i')

    name = UnicodeAttribute(
        attr_name='n', null=True
    )  # human-readable name if available
    # ARN for AWS, URN for GOOGLE and ID for AZURE or K8S
    arn = UnicodeAttribute(attr_name='arn', null=True)

    _data = MapAttribute(default=dict, attr_name='d')
    sync_date = NumberAttribute(attr_name='s')
    sha256 = UnicodeAttribute(attr_name='sha256')
    _collector_type = UnicodeAttribute(attr_name='c')

    tenant_name = UnicodeAttribute(attr_name='tn')
    customer_name = UnicodeAttribute(attr_name='cn')

    arn_index = ResourceARNIndex()

    def __init__(self, *args, **kwargs):
        kwargs['did'] = COMPOUND_KEYS_SEPARATOR.join(
            (
                kwargs.get('account_id', ''),
                kwargs.get('location', ''),
                kwargs.get('resource_type', ''),
                kwargs.get('id', ''),
            )
        )

        super().__init__(*args, **kwargs)
        self.sha256 = self._compute_hash(self._data.as_dict())

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
        self.sha256 = self._compute_hash(value)
        self._data = value

    @property
    def cloud(self) -> Cloud:
        return Cloud[self.resource_type.split('.')[0].upper()]

    @property
    def collector_type(self) -> ResourcesCollectorType:
        return ResourcesCollectorType(self._collector_type)

    def __repr__(self):
        return (
            f'Resource(id={self.id}, name={self.name}, '
            f'location={self.location}, resource_type={self.resource_type}, '
            f'tenant_name={self.tenant_name}, customer_name={self.customer_name})'
        )
