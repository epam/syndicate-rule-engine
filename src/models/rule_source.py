import os
from typing import Optional

from pynamodb.attributes import UnicodeAttribute, MapAttribute, ListAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import CAASEnv, CUSTOMER_ATTR, \
    GIT_PROJECT_ID_ATTR, RuleSourceType
from models import BaseModel, BaseGSI


class LatestSyncAttribute(MapAttribute):
    sync_date = UnicodeAttribute(null=True)
    commit_hash = UnicodeAttribute(null=True)
    commit_time = UnicodeAttribute(null=True)  # ISO8601
    current_status = UnicodeAttribute(null=True)  # SYNCING, SYNCED


class CustomerGitProjectIdIndex(BaseGSI):
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
        region = os.environ.get(CAASEnv.AWS_REGION)

    id = UnicodeAttribute(hash_key=True)
    customer = UnicodeAttribute()
    git_project_id = UnicodeAttribute()  # owner/repo for GITHUB, id for GITLAB
    git_url = UnicodeAttribute()
    git_access_type = UnicodeAttribute(null=True)
    git_access_secret = UnicodeAttribute(null=True)
    git_rules_prefix = UnicodeAttribute(null=True)
    git_ref = UnicodeAttribute(null=True)

    description = UnicodeAttribute(null=True)
    latest_sync = LatestSyncAttribute(null=True, default=dict)
    allowed_for = ListAttribute(default=list, null=True)  # tenants

    customer_git_project_id_index = CustomerGitProjectIdIndex()

    @property
    def type(self) -> Optional[RuleSourceType]:
        """
        In case None is returned, we cannot know.
        This property looks into
        :return:
        """
        if self.git_project_id.count('/') == 1:
            # GitHub project full name: "owner/repo"
            return RuleSourceType.GITHUB
        if self.git_project_id.isdigit():
            return RuleSourceType.GITLAB

    @property
    def has_secret(self) -> bool:
        return bool(self.git_access_secret)
