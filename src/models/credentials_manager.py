import os

from pynamodb.attributes import (
    UnicodeAttribute, BooleanAttribute, NumberAttribute
)

from pynamodb.indexes import AllProjection
from models.modular import BaseModel, BaseGSI
from helpers.constants import ENV_VAR_REGION

CM_CLOUD_IDENTIFIER = 'cid'
CM_CLOUD = 'c'
CM_ROLE_ARN = 'ra'
CM_ENABLED = 'e'
CM_EXPIRATION = 'ex'
CM_CREDENTIALS_KEY = 'ck'
CM_CUSTOMER = 'cn'
CM_TENANT = 'tn'


class TenantCloudIdentifierIndex(BaseGSI):
    class Meta:
        index_name = f'{CM_TENANT}-{CM_CLOUD}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    tenant = UnicodeAttribute(hash_key=True, attr_name=CM_TENANT)
    cloud = UnicodeAttribute(range_key=True, attr_name=CM_CLOUD)


class CustomerCloudIdentifierIndex(BaseGSI):
    class Meta:
        index_name = f'{CM_CUSTOMER}-{CM_CLOUD}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=CM_CUSTOMER)
    cloud = UnicodeAttribute(range_key=True, attr_name=CM_CLOUD)


class CredentialsManager(BaseModel):
    class Meta:
        table_name = 'CaaSCredentialsManager'
        region = os.environ.get(ENV_VAR_REGION)

    cloud_identifier = UnicodeAttribute(hash_key=True,
                                        attr_name=CM_CLOUD_IDENTIFIER)
    cloud = UnicodeAttribute(range_key=True, attr_name=CM_CLOUD)
    trusted_role_arn = UnicodeAttribute(null=True, attr_name=CM_ROLE_ARN)
    enabled = BooleanAttribute(null=True, default=False, attr_name=CM_ENABLED)
    expiration = NumberAttribute(null=True,
                                 attr_name=CM_EXPIRATION)  # timestamp
    credentials_key = UnicodeAttribute(null=True, attr_name=CM_CREDENTIALS_KEY)

    customer = UnicodeAttribute(null=True, attr_name=CM_CUSTOMER)
    tenant = UnicodeAttribute(null=True, attr_name=CM_TENANT)

    customer_cloud_identifier_index = CustomerCloudIdentifierIndex()
    tenant_cloud_identifier_index = TenantCloudIdentifierIndex()
