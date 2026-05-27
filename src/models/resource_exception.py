from pynamodb.attributes import (
    NumberAttribute,
    UnicodeAttribute,
    ListAttribute,
)
from pymongo.database import Database
from modular_sdk.models.pynamongo.attributes import MongoTTLAttribute

from helpers.constants import Env, ResourceExceptionType
from helpers.log_helper import get_logger
from models import BaseModel

_LOG = get_logger(__name__)


# NOTE: I don't use pynamodb indexes here because
# we need sparse indexes in MongoDB
# because we have a lot of null values in these fields
def create_resource_exceptions_indexes(db: Database) -> tuple[str, ...]:
    collection = db.get_collection(ResourceException.Meta.table_name)
    indexes = collection.index_information()

    # TODO: need create migration to rename index from 'cn_1_tn_1_l_1_rt_1_i_1' to 'cn_1_tn_1_l_1_rt_1_ri_1'
    name1 = 'cn_1_tn_1_l_1_rt_1_i_1'
    if name1 not in indexes:
        _LOG.info(f'Index {name1} does not exist yet')
        collection.create_index(
            [('cn', 1), ('tn', 1), ('l', 1), ('rt', 1), ('ri', 1)],
            name=name1,
            sparse=True,
        )

    name2 = 'arn_1'
    if name2 not in indexes:
        _LOG.info(f'Index {name2} does not exist yet')
        collection.create_index([('arn', 1)], name=name2, sparse=True)
    
    name3 = 'tf_1_multikey'
    if name3 not in indexes:
        _LOG.info(f'Index {name3} does not exist yet')
        collection.create_index([('tf', 1)], name=name3, sparse=True)
    
    return name1, name2, name3


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
    # data: {"tf": ["tag1=value1", "tag2=value2"]}
    # filters that uses index:
    # {"tf": /^tag1=/}, {"tf": "tag2=value2"}, {"tf": ["tag1=value1", "tag2=value2"]}
    # https://www.mongodb.com/docs/manual/core/indexes/index-types/index-multikey/
    tags_filters = ListAttribute(attr_name='tf', of=UnicodeAttribute, null=True)

    location = UnicodeAttribute(attr_name='l', null=True)
    resource_type = UnicodeAttribute(attr_name='rt', null=True)
    tenant_name = UnicodeAttribute(attr_name='tn')
    customer_name = UnicodeAttribute(attr_name='cn')

    created_at = NumberAttribute(attr_name='ca')
    updated_at = NumberAttribute(attr_name='ua')
    expire_at = MongoTTLAttribute(attr_name='ea')

    @property
    def type(self) -> ResourceExceptionType:
        if self.arn:
            return ResourceExceptionType.ARN
        elif self.location and self.resource_type and self.resource_id:
            return ResourceExceptionType.RESOURCE
        elif self.tags_filters:
            return ResourceExceptionType.TAG_FILTER
        else:
            raise ValueError('ResourceException must have at least one type set')
    
    def to_dict(self) -> dict:
        d = {'id': self.id}
        if self.type == ResourceExceptionType.ARN:
            d['arn'] = self.arn
        elif self.type == ResourceExceptionType.RESOURCE:
            d.update({
                'location': self.location,
                'resource_type': self.resource_type,
                'resource_id': self.resource_id,
            })
        elif self.type == ResourceExceptionType.TAG_FILTER:
            d['tags_filters'] = self.tags_filters
        return d
