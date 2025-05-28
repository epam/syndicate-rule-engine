from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers import Version
from helpers.constants import COMPOUND_KEYS_SEPARATOR, CAASEnv
from helpers.time_helper import utc_iso
from models import BaseModel

RULESET_LICENSES = 'L'
RULESET_STANDARD = 'S'

EMPTY_VERSION = ''


class S3PathAttribute(MapAttribute):
    bucket_name = UnicodeAttribute(null=True)
    path = UnicodeAttribute(null=True)


class RulesetStatusAttribute(MapAttribute):
    code = UnicodeAttribute(null=True)  # READY_TO_SCAN, deprecated
    last_update_time = UnicodeAttribute(null=True)  # ISO8601
    reason = UnicodeAttribute(null=True)


class CustomerIdIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'customer-id-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True)
    id = UnicodeAttribute(range_key=True)


class Ruleset(BaseModel):
    class Meta:
        table_name = 'CaaSRulesets'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True)  # "customer#L|S#name#version"
    customer = UnicodeAttribute()
    cloud = UnicodeAttribute()
    rules = ListAttribute(of=UnicodeAttribute, default=list)
    s3_path = S3PathAttribute(default=dict)
    status = RulesetStatusAttribute(default=dict)  # deprecated
    license_keys = ListAttribute(default=list)
    created_at = UnicodeAttribute(null=True, default_for_new=utc_iso)
    versions = ListAttribute(default=list)
    description = UnicodeAttribute(null=True)

    customer_id_index = CustomerIdIndex()

    @property
    def name(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[2]

    @property
    def version(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[3]

    @version.setter
    def version(self, value: str):
        items = self.id.split(COMPOUND_KEYS_SEPARATOR)
        items[3] = value
        self.id = COMPOUND_KEYS_SEPARATOR.join(items)

    @property
    def licensed(self) -> bool:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[1] == RULESET_LICENSES

    @property
    def is_empty_version(self) -> bool:
        return self.version is EMPTY_VERSION

    @property
    def latest_version(self) -> str:
        """
        Supposed to be used for licensed rulesets
        """
        if self.licensed and self.versions:
            return max(self.versions, key=Version)
        return self.version
