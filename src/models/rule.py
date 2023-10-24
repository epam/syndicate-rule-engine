import os
from itertools import islice
from typing import Optional

from pynamodb.attributes import UnicodeAttribute, MapAttribute, ListAttribute
from pynamodb.indexes import AllProjection

from helpers.constants import ENV_VAR_REGION, COMPOUND_KEYS_SEPARATOR, \
    RuleSourceType
from models.modular import BaseModel, BaseGSI

R_ID_ATTR = 'id'
R_CUSTOMER_ATTR = 'c'
R_RESOURCE_ATTR = 'rs'
R_DESCRIPTION_ATTR = 'd'
R_FILTERS_ATTR = 'f'
R_COMMENT_ATTR = 'i'  # index
R_LOCATION_ATTR = 'l'
R_COMMIT_HASH_ATTR = 'ch'
R_UPDATED_DATE_ATTR = 'u'


class CustomerIdIndex(BaseGSI):
    class Meta:
        index_name = f'{R_CUSTOMER_ATTR}-{R_ID_ATTR}-index'
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=R_CUSTOMER_ATTR)
    id = UnicodeAttribute(range_key=True, attr_name=R_ID_ATTR)


class CustomerLocationIndex(BaseGSI):
    class Meta:
        index_name = f'{R_CUSTOMER_ATTR}-{R_LOCATION_ATTR}-index'
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=R_CUSTOMER_ATTR)
    location = UnicodeAttribute(range_key=True, attr_name=R_LOCATION_ATTR)


class RuleIndex:
    """
    https://github.com/epam/ecc-kubernetes-rulepack/wiki/Rule-Index-(Comment)-Structure
    """
    def __init__(self, comment: str):
        self._comment = comment or ''
        it = iter(self._comment)
        self._cloud = ''.join(islice(it, 2))
        self._platform = ''.join(islice(it, 2))
        self._category = ''.join(islice(it, 2))
        self._service_section = ''.join(islice(it, 2))
        self._source = ''.join(islice(it, 2))
        self._customization = ''.join(islice(it, 1))
        self._multiregional = ''.join(islice(it, 1))

    @staticmethod
    def _cloud_map() -> dict:
        return {
            '00': None,
            '01': 'AWS',
            '02': 'AZURE',
            '03': 'GCP'
        }

    @staticmethod
    def _platform_map() -> dict:
        return {
            '00': None,
            '01': 'Kubernetes',
            '02': 'OpenShift',
            '03': 'Kubernetes and OpenShift'
        }

    @property
    def cloud(self) -> Optional[str]:
        return self._cloud_map().get(self._cloud)

    @property
    def platform(self) -> Optional[str]:
        return self._platform_map().get(self._platform)

    @property
    def multiregional(self) -> bool:
        if not self._multiregional:
            return True  # most rules are multiregional
        return bool(int(self._multiregional))


class Rule(BaseModel):
    @classmethod
    def latest_version_tag(cls) -> str:
        """
        ascii table:
        Index: 55  56  57  58
        Value: "7" "8" "9" ":"
        This makes sorting work properly
        :return:
        """
        return chr(58)  # ":"

    class Meta:
        table_name = 'CaaSRules'
        region = os.environ.get(ENV_VAR_REGION)

    # "customer#cloud#name#version"
    id = UnicodeAttribute(hash_key=True, attr_name=R_ID_ATTR)
    customer = UnicodeAttribute(attr_name=R_CUSTOMER_ATTR)
    resource = UnicodeAttribute(attr_name=R_RESOURCE_ATTR)
    description = UnicodeAttribute(attr_name=R_DESCRIPTION_ATTR)
    filters = ListAttribute(default=list, of=MapAttribute,
                            attr_name=R_FILTERS_ATTR)
    comment = UnicodeAttribute(null=True, attr_name=R_COMMENT_ATTR)

    # "project#ref#path"
    location = UnicodeAttribute(attr_name=R_LOCATION_ATTR)
    commit_hash = UnicodeAttribute(null=True, attr_name=R_COMMIT_HASH_ATTR)
    updated_date = UnicodeAttribute(null=True, attr_name=R_UPDATED_DATE_ATTR)
    # project id for GitLab, project full_name for GitHub

    customer_id_index = CustomerIdIndex()
    customer_location_index = CustomerLocationIndex()

    @property
    def cloud(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[1]

    @property
    def name(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[2]

    @property
    def version(self) -> Optional[str]:
        """
        Version can be absent. This means that the item represents the latest
        version of the rule. In case the version is not specified, returns
        None even though it means the latest version
        :return:
        """
        v = self.id.split(COMPOUND_KEYS_SEPARATOR)[3]
        return None if v == self.latest_version_tag() else v

    @property
    def git_project(self) -> Optional[str]:
        if not self.location:
            return
        return self.location.split(COMPOUND_KEYS_SEPARATOR)[0]

    @property
    def ref(self) -> Optional[str]:
        if not self.location:
            return
        return self.location.split(COMPOUND_KEYS_SEPARATOR)[1]

    @property
    def path(self) -> Optional[str]:
        if not self.location:
            return
        return self.location.split(COMPOUND_KEYS_SEPARATOR)[2]

    @property
    def is_latest(self) -> bool:
        """
        Returns true only if the version is guaranteed to be the latest
        :return:
        """
        return self.version is None

    @property
    def git_service(self) -> Optional[RuleSourceType]:
        """
        In case None is returned, we cannot know.
        This property looks into
        :return:
        """
        if not self.git_project:
            return
        if self.git_project.count('/') == 1:
            # GitHub project full name: "owner/repo"
            return RuleSourceType.GITHUB
        if self.git_project.isdigit():
            return RuleSourceType.GITLAB

    def build_policy(self) -> dict:
        """
        Return a minimal prepared policy which can be used for scanning:
            name: ecc-aws-002-...
            resource: aws.iam-user
            filters:
              - type: credential
                key: password_enabled
                value: true
              - type: credential
                key: mfa_active
                value: false
        :return:
        """
        return {
            'name': self.name,
            'resource': self.resource,
            'filters': self.filters,
            'description': self.description
        }
