from itertools import count
from typing import cast

from pynamodb.attributes import ListAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from helpers.constants import (
    COMPOUND_KEYS_SEPARATOR,
    Env,
    Cloud,
    RuleSourceType,
    LATEST_VERSION_TAG,
)
from helpers import to_normalized_version, from_normalized_version
from models import BaseModel

R_ID_ATTR = 'id'
R_CUSTOMER_ATTR = 'c'
R_RESOURCE_ATTR = 'rs'
R_DESCRIPTION_ATTR = 'd'
R_FILTERS_ATTR = 'f'
R_COMMENT_ATTR = 'i'  # index
R_LOCATION_ATTR = 'l'
R_COMMIT_HASH_ATTR = 'ch'
R_UPDATED_DATE_ATTR = 'u'
R_RULE_SOURCE_ID_ATTR = 's'
R_FINGERPRINT_ATTR = 'fp'


class CustomerIdIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{R_CUSTOMER_ATTR}-{R_ID_ATTR}-index'
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=R_CUSTOMER_ATTR)
    id = UnicodeAttribute(range_key=True, attr_name=R_ID_ATTR)


class CustomerLocationIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{R_CUSTOMER_ATTR}-{R_LOCATION_ATTR}-index'
        projection = AllProjection()

    customer = UnicodeAttribute(hash_key=True, attr_name=R_CUSTOMER_ATTR)
    location = UnicodeAttribute(range_key=True, attr_name=R_LOCATION_ATTR)


class RuleSourceIdIdIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = f'{R_RULE_SOURCE_ID_ATTR}-{R_ID_ATTR}-index'
        projection = AllProjection()

    rule_source_id = UnicodeAttribute(
        hash_key=True, attr_name=R_RULE_SOURCE_ID_ATTR
    )
    id = UnicodeAttribute(range_key=True, attr_name=R_ID_ATTR)


class RuleIndex:
    """
    https://github.com/epam/ecc-kubernetes-rulepack/wiki/Rule-Index-(Comment)-Structure
    """

    @staticmethod
    def _it(d: int):
        f = f'0{d}d'
        for i in count():
            yield format(i, f)

    cloud_map = dict(zip(_it(2), [None, 'AWS', 'AZURE', 'GCP']))

    platform_map = dict(
        zip(
            _it(2),
            [None, 'Kubernetes', 'OpenShift', 'Kubernetes and OpenShift'],
        )
    )
    category_map = dict(
        zip(
            _it(2),
            [
                'FinOps',
                'Lifecycle management',
                'Unutilized Resources',
                'Idle and underutilized resources',
                'Rightsizing',
                'Autoscaling',
                'Computing resources optimization',
                'Storage optimization',
                'Reserved instances and savings plan usage',
                'Other cost optimization checks',
                'Tagging',
                'Security',
                'Detect',
                'Identify',
                'Protect',
                'Recover',
                'Detection services',
                'Secure access management',
                'Inventory',
                'Logging',
                'Resource configuration',
                'Vulnerability, patch, and version management',
                'Secure access management',
                'Secure configuration',
                'Secure network configuration',
                'Data protection',
                'API protection',
                'Protective services',
                'Secure development',
                'Key, Secrets, and Certificate management',
                'Network security',
                'Resilience',
                'Monitoring',
                'Access control',
                'Passwordless authentication',
                'Root user access restrictions',
                'MFA enabled',
                'Sensitive API actions restricted',
                'Resource policy configuration',
                'API private access',
                'Resources not publicly accessible',
                'Resources within VPC',
                'Security group configuration',
                'Encryption of data at rest',
                'Encryption of data in transit',
                'Encryption of data at rest and in transit',
                'Data integrity',
                'Data deletion protection',
                'Credentials not hardcoded',
                'Backups enabled',
                'High availability',
            ],
        )
    )
    service_section_map = dict(
        zip(
            _it(2),
            [
                'Identity and Access Management',
                'Logging and Monitoring',
                'Networking & Content Delivery',
                'Compute',
                'Storage',
                'Analytics',
                'Databases',
                'Kubernetes Engine',
                'Containers',
                'Security & Compliance',
                'Cryptography & PKI',
                'Machine learning',
                'End User Computing',
                'Developer Tools',
                'Application Integration',
                'Dataproc',
                'App Engine',
                'AppService',
                'Microsoft Defender for Cloud',
                'API Server',
                'Controller Manager',
                'etcd',
                'General Policies',
                'Pod Security Standards',
                'RBAC and Service Accounts',
                'Scheduler',
                'Secrets Management',
            ],
        )
    )
    source_map = dict(
        zip(
            _it(2),
            [
                'Azure Security Benchmark (V3)',
                'CIS Amazon Web Services Foundations Benchmark v1.2.0',
                'CIS Amazon Web Services Foundations Benchmark v1.4.0',
                'CIS Amazon Web Services Foundations Benchmark v1.5.0',
                'CIS AWS Compute Services Benchmark v1.0.0',
                'CIS AWS EKS Benchmark 1.1.0',
                'CIS AWS End User Compute Services Benchmark v1.0.0',
                'CIS Benchmark Google Cloud Platform Foundation v1.0.0',
                'CIS Benchmark Google Cloud Platform Foundation v1.2.0',
                'CIS Benchmark Google Cloud Platform Foundation v1.3.0',
                'CIS Benchmark Google Cloud Platform Foundation v2.0.0',
                'CIS Google Kubernetes Engine (GKE) Benchmark v1.3.0',
                'CIS Google Kubernetes Engine (GKE) Benchmark v1.4.0',
                'CIS Microsoft Azure Foundations Benchmark v1.4.0',
                'CIS Microsoft Azure Foundations Benchmark v1.5.0',
                'CIS Microsoft Azure Foundations Benchmark v2.0.0',
                'CIS MySQL Enterprise Edition 8.0 Benchmark v1.2.0',
                'CIS Oracle Database 19 Benchmark v1.0.0',
                'CIS Oracle MySQL Community Server 5.7 Benchmark v2.0.0',
                'CIS PostgreSQL 11 Benchmark 1.0.0',
                'EPAM',
                'NIST SP 800-53 Rev. 5',
                'PCI DSS',
                'CIS Kubernetes Benchmark v1.7.0',
                'CIS RedHat OpenShift Container Platform Benchmark v1.4.0',
                'CIS Amazon Web Services Foundations Benchmark v2.0.0',
            ],
        )
    )

    __slots__ = (
        'raw_cloud',
        'raw_platform',
        'category',
        'service_section',
        'source',
        'has_customization',
        'is_global',
    )

    def __init__(self, comment: str | None):
        comment = comment or ''
        self.raw_cloud = self.cloud_map.get(comment[0:2])
        self.raw_platform = self.platform_map.get(comment[2:4])
        self.category = self.category_map.get(comment[4:6])
        self.service_section = self.service_section_map.get(comment[6:8])
        self.source = self.source_map.get(comment[8:10])
        self.has_customization = not not int(comment[10:11] or '0')

        # This fields means whether the rule should be executed only once and
        # not whether the resource itself is global. For instance: s3.
        # S3 buckets are not global. They reside in specific regions.
        # But the API endpoint is global and returns all the buckets from
        # all regions. So, we should execute such rules only once.

        # '1' because most rules are global
        self.is_global = not not int(comment[11:12] or '1')

    @property
    def cloud(self) -> Cloud:
        pl = self.raw_platform
        if pl:
            # K8S or OpenShift
            return Cloud.KUBERNETES
        return Cloud[cast(str, self.raw_cloud)]


class Rule(BaseModel):
    class Meta:
        table_name = 'SRERules'
        region = Env.AWS_REGION.get()

    # "customer#cloud#name#normalized_version"
    id = UnicodeAttribute(hash_key=True, attr_name=R_ID_ATTR)
    customer = UnicodeAttribute(attr_name=R_CUSTOMER_ATTR)
    resource = UnicodeAttribute(attr_name=R_RESOURCE_ATTR)
    description = UnicodeAttribute(attr_name=R_DESCRIPTION_ATTR)
    filters = ListAttribute(
        default=list, attr_name=R_FILTERS_ATTR
    )  # list of either strings or maps
    comment = UnicodeAttribute(null=True, attr_name=R_COMMENT_ATTR)
    rule_source_id = UnicodeAttribute(
        null=True, attr_name=R_RULE_SOURCE_ID_ATTR
    )
    fingerprint = UnicodeAttribute(
        null=False, attr_name=R_FINGERPRINT_ATTR
    )

    # "project#ref#path"
    location = UnicodeAttribute(attr_name=R_LOCATION_ATTR)
    commit_hash = UnicodeAttribute(null=True, attr_name=R_COMMIT_HASH_ATTR)
    updated_date = UnicodeAttribute(null=True, attr_name=R_UPDATED_DATE_ATTR)
    # project id for GitLab, project full_name for GitHub

    customer_id_index = CustomerIdIndex()
    customer_location_index = CustomerLocationIndex()
    rule_source_id_id_index = RuleSourceIdIdIndex()

    @property
    def cloud(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[1]

    @property
    def name(self) -> str:
        return self.id.split(COMPOUND_KEYS_SEPARATOR)[2]

    @property
    def version(self) -> str | None:
        """
        Version can be absent. This means that the item represents the latest
        version of the rule. In case the version is not specified, returns
        None even though it means the latest version
        :return:
        """
        v = self.id.split(COMPOUND_KEYS_SEPARATOR)[3]
        if not v or v == LATEST_VERSION_TAG:
            return None
        return from_normalized_version(v)

    @property
    def normalized_version(self) -> str:
        """
        Return normalized version. Latest or absent version will return ':',
        so that it is bigger then any other version
        :return:
        """
        v = self.id.split(COMPOUND_KEYS_SEPARATOR)[3]
        if not v or v == LATEST_VERSION_TAG:
            return LATEST_VERSION_TAG
        return to_normalized_version(v)

    @property
    def git_project(self) -> str | None:
        if not self.location:
            return
        return self.location.split(COMPOUND_KEYS_SEPARATOR)[0]

    @property
    def ref(self) -> str | None:
        if not self.location:
            return
        return self.location.split(COMPOUND_KEYS_SEPARATOR)[1]

    @property
    def path(self) -> str | None:
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
    def git_service(self) -> RuleSourceType | None:
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
        policy = {
            'name': self.name,
            'resource': self.resource,
            'filters': self.filters,
            'description': self.description,
            'comment': self.comment,
            'fingerprint': self.fingerprint,
        }
        return policy
