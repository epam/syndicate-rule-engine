from pynamodb.attributes import (
    NumberAttribute,
    UnicodeAttribute,
    ListAttribute,
)
from pymongo.database import Database
from modular_sdk.models.pynamongo.attributes import MongoTTLAttribute

from helpers.constants import Env
from helpers.log_helper import get_logger
from models import BaseModel

_LOG = get_logger(__name__)


# NOTE: I don't use pynamodb indexes here because
# we need sparse indexes in MongoDB
# because we have a lot of null values in these fields
def create_resource_exceptions_indexes(db: Database):
    collection = db.get_collection(ResourceException.Meta.table_name)
    indexes = collection.index_information()

    name = 'cn_1_tn_1_l_1_rt_1_i_1'
    if name not in indexes:
        _LOG.info(f'Index {name} does not exist yet')
        collection.create_index(
            [('cn', 1), ('tn', 1), ('l', 1), ('rt', 1), ('ri', 1)],
            name=name,
            sparse=True,
        )

    name = 'arn_1'
    if name not in indexes:
        _LOG.info(f'Index {name} does not exist yet')
        collection.create_index([('arn', 1)], name=name, sparse=True)
    
    name = 'tf_1_multikey'
    if name not in indexes:
        _LOG.info(f'Index {name} does not exist yet')
        collection.create_index([('tf', 1)], name=name, sparse=True)


class ResourceException(BaseModel):
    class Meta:
        table_name = 'SREResourceExceptions'
        region = Env.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True, attr_name='i')

    # To actually work needs location, resource_type, and tenant_name
    resource_id = UnicodeAttribute(attr_name='ri', null=True)

    # ARN for AWS, URN for GOOGLE and ID for AZURE or K8S
    arn = UnicodeAttribute(attr_name='arn', null=True)

    # In mongo we can create a multikey index on this field
    # and we can use it to filter resources by tags
    # Example:
    # data: {"f": ["tag1=value1", "tag2=value2"]}
    # filters that uses index:
    # {"f": /^tag1=/}, {"f": "tag2=value2"}, {"f": ["tag1=value1", "tag2=value2"]}
    # https://www.mongodb.com/docs/manual/core/indexes/index-types/index-multikey/
    tags_filters = ListAttribute(attr_name='tf', of=UnicodeAttribute, null=True)

    location = UnicodeAttribute(attr_name='l', null=True)
    resource_type = UnicodeAttribute(attr_name='rt', null=True)
    tenant_name = UnicodeAttribute(attr_name='tn')
    customer_name = UnicodeAttribute(attr_name='cn')

    created_at = NumberAttribute(attr_name='ca')
    updated_at = NumberAttribute(attr_name='ua')
    expire_at = MongoTTLAttribute(attr_name='ea')
