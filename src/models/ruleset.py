import os

from pynamodb.attributes import UnicodeAttribute, BooleanAttribute, \
    ListAttribute, MapAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import COMPOUND_KEYS_SEPARATOR
from helpers.constants import ENV_VAR_REGION
from models.modular import BaseModel, BaseGSI

RULESET_LICENSES = 'L'
RULESET_STANDARD = 'S'


class S3PathAttribute(MapAttribute):
    bucket_name = UnicodeAttribute(null=True)
    path = UnicodeAttribute(null=True)


class RulesetStatusAttribute(MapAttribute):
    code = UnicodeAttribute(null=True)  # READY_TO_SCAN, ASSEMBLING
    last_update_time = UnicodeAttribute(null=True)  # ISO8601
    reason = UnicodeAttribute(null=True)


class CustomerIdIndex(BaseGSI):
    class Meta:
        index_name = 'customer-id-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True)
    id = UnicodeAttribute(range_key=True)


class LicenseManagerIdIndex(BaseGSI):
    class Meta:
        index_name = 'license_manager_id-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    license_manager_id = UnicodeAttribute(hash_key=True)


class Ruleset(BaseModel):
    class Meta:
        table_name = 'CaaSRulesets'
        region = os.environ.get(ENV_VAR_REGION)

    id = UnicodeAttribute(hash_key=True)  # "customer#L|S#name#version"
    customer = UnicodeAttribute()
    cloud = UnicodeAttribute()
    active = BooleanAttribute(default=True)
    event_driven = BooleanAttribute(null=True, default=False)
    rules = ListAttribute(of=UnicodeAttribute, default=list)
    s3_path = S3PathAttribute(default=dict)
    status = RulesetStatusAttribute(default=dict)
    allowed_for = ListAttribute(default=list)
    license_keys = ListAttribute(of=UnicodeAttribute, default=list)
    license_manager_id = UnicodeAttribute(null=True)

    customer_id_index = CustomerIdIndex()
    license_manager_id_index = LicenseManagerIdIndex()

    @property
    def name(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[2]

    @property
    def version(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[3]

    @property
    def licensed(self) -> bool:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[1] == RULESET_LICENSES
