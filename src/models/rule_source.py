from pynamodb.attributes import UnicodeAttribute, MapAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import CAASEnv, CUSTOMER_ATTR, \
    GIT_PROJECT_ID_ATTR, RuleSourceType
from models import BaseModel


class LatestSyncAttribute(MapAttribute):
    sync_date = UnicodeAttribute(null=True)
    commit_hash = UnicodeAttribute(null=True)
    commit_time = UnicodeAttribute(null=True)  # ISO8601
    current_status = UnicodeAttribute(null=True)  # SYNCING, SYNCED
    release_tag = UnicodeAttribute(null=True)
    version = UnicodeAttribute(null=True)
    cc_version = UnicodeAttribute(null=True)


class CustomerGitProjectIdIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{CUSTOMER_ATTR}-{GIT_PROJECT_ID_ATTR}-index'
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True)
    git_project_id = UnicodeAttribute(range_key=True)


class RuleSource(BaseModel):
    class Meta:
        table_name = 'CaaSRuleSources'
        region = CAASEnv.AWS_REGION.get()

    id = UnicodeAttribute(hash_key=True)
    customer = UnicodeAttribute()
    git_project_id = UnicodeAttribute()  # owner/repo for GITHUB, id for GITLAB
    git_url = UnicodeAttribute()
    git_access_secret = UnicodeAttribute(null=True)
    git_rules_prefix = UnicodeAttribute(null=True)
    git_ref = UnicodeAttribute(null=True)
    type_ = UnicodeAttribute(null=True, attr_name='type')

    description = UnicodeAttribute(null=True)
    latest_sync = LatestSyncAttribute(default=dict)

    customer_git_project_id_index = CustomerGitProjectIdIndex()

    @property
    def type(self) -> RuleSourceType:
        if self.type_:
            return RuleSourceType(self.type_)
        # old rule sources can have an empty field
        if self.git_project_id.count('/') == 1:
            # GitHub project full name: "owner/repo"
            return RuleSourceType.GITHUB
        if self.git_project_id.isdigit():
            return RuleSourceType.GITLAB

    @property
    def has_secret(self) -> bool:
        return bool(self.git_access_secret)

    @property
    def release_tag(self) -> str | None:
        return self.latest_sync.release_tag

    @property
    def version(self) -> str | None:
        return self.latest_sync.version

    @property
    def cc_version(self) -> str | None:
        return self.latest_sync.cc_version
