"""
https://cloud.google.com/chronicle/docs/reference/udm-field-list

These models contain only fields that we needed
"""
import enum
from datetime import datetime, timezone

import msgspec
from modular_sdk.models.tenant import Tenant

from helpers import filter_dict, hashable
from helpers.constants import REPORT_FIELDS, CAASEnv, Severity
from helpers.mappings.udm_resource_type import (
    UDMResourceType,
    from_cc_resource_type,
)
from helpers.time_helper import utc_datetime
from services.metadata import Metadata, RuleMetadata
from services.report_convertors import ShardCollectionConvertor
from services.sharding import ShardsCollection
from services import SP


class UDMEntityType(str, enum.Enum):
    ASSET = 'ASSET'
    DOMAIN_NAME = 'DOMAIN_NAME'
    FILE = 'FILE'
    GROUP = 'GROUP'
    IP_ADDRESS = 'IP_ADDRESS'
    METRIC = 'METRIC'
    MUTEX = 'MUTEX'
    RESOURCE = 'RESOURCE'
    URL = 'URL'
    USER = 'USER'


class UDMEventType(str, enum.Enum):
    SCAN_VULN_HOST = 'SCAN_VULN_HOST'
    # todo write other types


class UDMSourceType(str, enum.Enum):
    DERIVED_CONTEXT = 'DERIVED_CONTEXT'
    ENTITY_CONTEXT = 'ENTITY_CONTEXT'
    GLOBAL_CONTEXT = 'GLOBAL_CONTEXT'
    SOURCE_TYPE_UNSPECIFIED = 'SOURCE_TYPE_UNSPECIFIED'


class UDMCloudEnvironment(str, enum.Enum):
    AMAZON_WEB_SERVICES = 'AMAZON_WEB_SERVICES'
    GOOGLE_CLOUD_PLATFORM = 'GOOGLE_CLOUD_PLATFORM'
    MICROSOFT_AZURE = 'MICROSOFT_AZURE'
    UNSPECIFIED_CLOUD_ENVIRONMENT = 'UNSPECIFIED_CLOUD_ENVIRONMENT'

    @classmethod
    def from_local_cloud(cls, name: str | enum.Enum
                         ) -> 'UDMCloudEnvironment':
        if isinstance(name, enum.Enum):
            name = name.value
        match name:
            case 'AWS':
                return cls.AMAZON_WEB_SERVICES
            case 'GOOGLE' | 'GCP':
                return cls.GOOGLE_CLOUD_PLATFORM
            case 'AZURE':
                return cls.MICROSOFT_AZURE
            case _:
                return cls.UNSPECIFIED_CLOUD_ENVIRONMENT


class UDMSecurityCategory(str, enum.Enum):
    ACL_VIOLATION = 'ACL_VIOLATION'
    AUTH_VIOLATION = 'AUTH_VIOLATION'
    DATA_AT_REST = 'DATA_AT_REST'
    DATA_DESTRUCTION = 'DATA_DESTRUCTION'
    DATA_EXFILTRATION = 'DATA_EXFILTRATION'
    EXPLOIT = 'EXPLOIT'
    MAIL_PHISHING = 'MAIL_PHISHING'
    MAIL_SPAM = 'MAIL_SPAM'
    MAIL_SPOOFING = 'MAIL_SPOOFING'
    NETWORK_CATEGORIZED_CONTENT = 'NETWORK_CATEGORIZED_CONTENT'
    NETWORK_COMMAND_AND_CONTROL = 'NETWORK_COMMAND_AND_CONTROL'
    NETWORK_DENIAL_OF_SERVICE = 'NETWORK_DENIAL_OF_SERVICE'
    NETWORK_MALICIOUS = 'NETWORK_MALICIOUS'
    NETWORK_RECON = 'NETWORK_RECON'
    NETWORK_SUSPICIOUS = 'NETWORK_SUSPICIOUS'
    PHISHING = 'PHISHING'
    POLICY_VIOLATION = 'POLICY_VIOLATION'
    SOCIAL_ENGINEERING = 'SOCIAL_ENGINEERING'
    SOFTWARE_MALICIOUS = 'SOFTWARE_MALICIOUS'
    SOFTWARE_PUA = 'SOFTWARE_PUA'
    SOFTWARE_SUSPICIOUS = 'SOFTWARE_SUSPICIOUS'
    TOR_EXIT_NODE = 'TOR_EXIT_NODE'
    UNKNOWN_CATEGORY = 'UNKNOWN_CATEGORY'


class UDMSeverity(str, enum.Enum):
    CRITICAL = 'CRITICAL'
    ERROR = 'ERROR'
    HIGH = 'HIGH'
    INFORMATIONAL = 'INFORMATIONAL'
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    NONE = 'NONE'
    UNKNOWN_SEVERITY = 'UNKNOWN_SEVERITY'

    @classmethod
    def from_local(cls, name: Severity) -> 'UDMSeverity':
        match name:
            case Severity.HIGH:
                return cls.HIGH
            case Severity.MEDIUM:
                return cls.MEDIUM
            case Severity.LOW:
                return cls.LOW
            case Severity.INFO:
                return cls.INFORMATIONAL
            case Severity.UNKNOWN:
                return cls.UNKNOWN_SEVERITY


class VulnerabilitySeverity(str, enum.Enum):
    CRITICAL = 'CRITICAL'
    HIGH = 'HIGH'
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    UNKNOWN_SEVERITY = 'UNKNOWN_SEVERITY'

    @classmethod
    def from_local(cls, name: Severity) -> 'VulnerabilitySeverity':
        match name:
            case Severity.HIGH:
                return cls.HIGH
            case Severity.MEDIUM:
                return cls.MEDIUM
            case Severity.LOW | Severity.INFO:
                return cls.LOW
            case Severity.UNKNOWN:
                return cls.UNKNOWN_SEVERITY


class UDMLocation(msgspec.Struct):
    name: str


class UDMEntityMetadata(msgspec.Struct, kw_only=True):
    entity_type: UDMEntityType
    collected_timestamp: datetime
    product_entity_id: str
    product_name: str
    vendor_name: str
    creation_timestamp: datetime = msgspec.UNSET
    product_version: str = msgspec.UNSET
    source_type: str = msgspec.UNSET
    description: str = msgspec.UNSET


class UDMCloud(msgspec.Struct, kw_only=True):
    environment: UDMCloudEnvironment
    availability_zone: str = msgspec.UNSET


class UDMLabel(msgspec.Struct, kw_only=True):
    key: str
    value: str
    rbac_enabled: bool = msgspec.UNSET


class UDMAttribute(msgspec.Struct, kw_only=True):
    cloud: UDMCloud
    creation_time: datetime = msgspec.UNSET
    labels: list[UDMLabel] = msgspec.UNSET


class UDMResource(msgspec.Struct, kw_only=True):
    name: str
    product_object_id: str
    resource_subtype: str = msgspec.UNSET
    resource_type: UDMResourceType = msgspec.UNSET
    attribute: UDMAttribute = msgspec.UNSET


class UDMAttackDetailsTactic(msgspec.Struct, kw_only=True):
    name: str
    id: str = msgspec.UNSET


class UDMAttackDetailsTechnique(msgspec.Struct, kw_only=True):
    id: str
    name: str
    subtechnique_id: str = msgspec.UNSET
    subtechnique_name: str = msgspec.UNSET


class UDMAttackDetails(msgspec.Struct, kw_only=True):
    tactics: list[UDMAttackDetailsTactic] = msgspec.UNSET
    techniques: list[UDMAttackDetailsTechnique] = msgspec.UNSET
    version: str = msgspec.UNSET


class UDMSecurityResult(msgspec.Struct, kw_only=True):
    category: UDMSecurityCategory
    attack_details: UDMAttackDetails = msgspec.UNSET
    description: str = msgspec.UNSET
    rule_author: str = msgspec.UNSET
    rule_id: str = msgspec.UNSET
    rule_name: str = msgspec.UNSET
    rule_set: str = msgspec.UNSET
    rule_set_display_name: str = msgspec.UNSET
    rule_version: str = msgspec.UNSET
    ruleset_category_display_name: str = msgspec.UNSET
    severity: UDMSeverity = msgspec.UNSET
    summary: str = msgspec.UNSET


class UDMNoun(msgspec.Struct, kw_only=True):
    location: UDMLocation = msgspec.UNSET
    resource: UDMResource = msgspec.UNSET
    security_result: list[UDMSecurityResult] = msgspec.UNSET
    namespace: str = msgspec.UNSET
    application: str = msgspec.UNSET
    hostname: str = msgspec.UNSET


class UDMEntity(msgspec.Struct, kw_only=True):
    metadata: UDMEntityMetadata
    entity: UDMNoun = msgspec.UNSET


class UDMEventMetadata(msgspec.Struct, kw_only=True):
    collected_timestamp: datetime
    event_timestamp: datetime = msgspec.field(default_factory=utc_datetime)
    event_type: UDMEventType
    product_name: str
    vendor_name: str
    product_version = msgspec.UNSET
    description: str = msgspec.UNSET


class UDMVulnerability(msgspec.Struct, kw_only=True):
    about: UDMNoun = msgspec.UNSET
    description: str = msgspec.UNSET
    name: str = msgspec.UNSET
    scan_start_time: datetime = msgspec.UNSET
    scan_end_time: datetime = msgspec.UNSET
    severity: VulnerabilitySeverity = msgspec.UNSET
    vendor: str = msgspec.UNSET
    vendor_vulnerability_id: str = msgspec.UNSET


class UDMVulnerabilities(msgspec.Struct, kw_only=True):
    vulnerabilities: list[UDMVulnerability]


class UDMExtensions(msgspec.Struct, kw_only=True):
    vulns: UDMVulnerabilities


class UDMEvent(msgspec.Struct, kw_only=True):
    metadata: UDMEventMetadata
    principal: UDMNoun = msgspec.UNSET
    target: UDMNoun = msgspec.UNSET
    extensions: UDMExtensions


class UDMSecurityResultBuilder:
    __slots__ = 'policy', 'description', 'meta', 'rule_set'

    def __init__(self, policy: str, description: str,
                 metadata: RuleMetadata,
                 rule_set: str | None = None):
        self.policy = policy
        self.description = description
        self.meta = metadata
        self.rule_set = rule_set

    def _build_mitre(self) -> UDMAttackDetails:
        item = UDMAttackDetails(
            tactics=[],
            techniques=[]
        )
        mitre = self.meta.mitre
        for tactic, techniques in mitre.items():
            item.tactics.append(UDMAttackDetailsTactic(name=tactic))
            for technique in techniques:
                sub = technique.get('st', [])
                if not sub:
                    item.techniques.append(UDMAttackDetailsTechnique(
                        id=technique.get('tn_id'),
                        name=technique.get('tn_name')
                    ))
                else:
                    for i in sub:
                        item.techniques.append(UDMAttackDetailsTechnique(
                            id=technique.get('tn_id'),
                            name=technique.get('tn_name'),
                            subtechnique_id=i.get('st_id'),
                            subtechnique_name=i.get('st_name')
                        ))
        return item

    def build(self) -> UDMSecurityResult:
        item = UDMSecurityResult(
            category=UDMSecurityCategory.POLICY_VIOLATION,
            summary=self.description,
            rule_author='Syndicate Rule Engine',
            rule_id=self.policy,
            rule_name=self.policy,
            attack_details=self._build_mitre()
        )
        if self.rule_set:
            item.rule_set = self.rule_set
        if article := self.meta.article:
            item.description = article
        if sev := self.meta.severity:
            item.severity = UDMSeverity.from_local(sev)
        return item


class UDMVulnerabilityBuilder:
    __slots__ = 'policy', 'description', 'meta', 'scan_start_time', 'scan_end_time'

    def __init__(self, policy: str, description: str,
                 metadata: RuleMetadata,
                 scan_start_time: str | datetime | None = None,
                 scan_end_time: str | datetime | None = None):
        self.policy = policy
        self.description = description
        self.meta = metadata
        self.scan_start_time = scan_start_time
        self.scan_end_time = scan_end_time

    @staticmethod
    def _parse_dt(item: str | datetime | None) -> datetime | None:
        if not item:
            return
        if isinstance(item, datetime):
            return item
        try:
            return utc_datetime(item)
        except Exception:
            return

    def build(self) -> UDMVulnerability:
        item = UDMVulnerability(
            name=self.description,
            vendor='Syndicate Rule Engine',
            vendor_vulnerability_id=self.policy,
        )
        if st := self._parse_dt(self.scan_start_time):
            item.scan_start_time = st
        if et := self._parse_dt(self.scan_end_time):
            item.scan_end_time = et
        if article := self.meta.article:
            item.description = article
        if sev := self.meta.severity:
            item.severity = VulnerabilitySeverity.from_local(sev)
        return item


class ShardCollectionUDMEntitiesConvertor(ShardCollectionConvertor):
    """
    Converts a collection to a list of UDM Entities where each entity
    represents one resource with inner list of all its violations
    """

    def __init__(self, metadata: Metadata, tenant: Tenant,
                 rule_set: str | None = None, **kwargs):
        super().__init__(metadata)
        self._tenant = tenant
        self._rule_set = rule_set

    @staticmethod
    def _parse_date(date: str | float) -> datetime | None:
        try:
            if isinstance(date, float):
                return datetime.fromtimestamp(date, tz=timezone.utc)
            else:  # str
                return utc_datetime(date)
        except Exception:
            return

    def convert(self, collection: 'ShardsCollection') -> list[dict]:
        """
        :param collection:
        :return:
        """
        meta = collection.meta
        datas = {}
        policies_results = {}
        for part in collection.iter_parts():
            for res in part.resources:
                unique = hashable((
                    filter_dict(res, REPORT_FIELDS),
                    part.location,
                    meta.get(part.policy, {}).get('resource')
                ))
                inner = datas.setdefault(unique, [set(), res, part.timestamp])
                inner[0].add(part.policy)
                inner[1].update(res)
                inner[2] = max(inner[2], part.timestamp)

                if part.policy not in policies_results:
                    policies_results[part.policy] = UDMSecurityResultBuilder(
                        policy=part.policy,
                        description=meta.get(part.policy, {}).get('description'),
                        metadata=self.meta.rule(part.policy),
                        rule_set=self._rule_set
                    ).build()
        entities = []
        for unique, inner in datas.items():
            policies, full_res, ts = inner
            res, region, rt = unique

            resource_id = res.get('arn') or res.get('id') or res.get('name')
            resource_name = res.get('name') or res.get('id') or res.get('arn')
            resource_type = from_cc_resource_type(rt)
            if resource_type is UDMResourceType.UNSPECIFIED:  # todo currently api does not accept this one(
                resource_type = UDMResourceType.CLOUD_PROJECT

            entity = UDMEntity(
                metadata=UDMEntityMetadata(
                    entity_type=UDMEntityType.RESOURCE,
                    collected_timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                    product_entity_id=resource_id,
                    product_name=self._tenant.name,
                    source_type=UDMSourceType.ENTITY_CONTEXT,
                    vendor_name=self._tenant.customer_name
                ),
                entity=UDMNoun(
                    security_result=[policies_results.get(p) for p in policies],
                    location=UDMLocation(region),
                    resource=UDMResource(
                        product_object_id=resource_id,
                        name=resource_name,
                        resource_subtype=rt,
                        resource_type=resource_type,
                        attribute=UDMAttribute(
                            cloud=UDMCloud(
                                environment=UDMCloudEnvironment.from_local_cloud(self._tenant.cloud),
                            ),
                            labels=[]
                        )
                    )
                )
            )
            if service := self.meta.rule(next(iter(policies))).service:
                entity.entity.application = service
            if date := full_res.get('date'):
                dt = self._parse_date(date)
                if dt:
                    entity.entity.resource.attribute.creation_time = dt
            if tags := full_res.get('Tags'):
                entity.entity.resource.attribute.labels.extend(
                    UDMLabel(key=t['Key'], value=t['Value']) for t in tags
                )
            entities.append(msgspec.to_builtins(entity))
        return entities


# TODO these two convertors are kind of POC and can be improved or extended.
#  I'm not sure about the right way to convert our findings to UDM


class ShardCollectionUDMEventsConvertor(ShardCollectionConvertor):
    """
    Converts a collection to a list of UDM Events
    """

    def __init__(self, metadata: Metadata, tenant: Tenant,
                 rule_set: str | None = None, **kwargs):
        super().__init__(metadata)
        self._tenant = tenant
        self._rule_set = rule_set

    @staticmethod
    def _parse_date(date: str | float) -> datetime | None:
        try:
            if isinstance(date, float):
                return datetime.fromtimestamp(date, tz=timezone.utc)
            else:  # str
                return utc_datetime(date)
        except Exception:
            return

    def convert(self, collection: 'ShardsCollection') -> list[dict]:
        """
        :param collection:
        :return:
        """
        meta = collection.meta
        datas = {}
        policies_results = {}
        for part in collection.iter_parts():
            for res in part.resources:
                unique = hashable((
                    filter_dict(res, REPORT_FIELDS),
                    part.location,
                    meta.get(part.policy, {}).get('resource')
                ))
                inner = datas.setdefault(unique, [set(), res, part.timestamp])
                inner[0].add(part.policy)
                inner[1].update(res)
                inner[2] = max(inner[2], part.timestamp)

                if part.policy not in policies_results:
                    policies_results[part.policy] = UDMVulnerabilityBuilder(
                        policy=part.policy,
                        description=meta.get(part.policy, {}).get('description'),
                        metadata=self.meta.rule(part.policy),
                    ).build()
        events = []
        for unique, inner in datas.items():
            policies, full_res, ts = inner
            res, region, rt = unique

            resource_id = res.get('arn') or res.get('id') or res.get('name')
            resource_name = res.get('name') or res.get('id') or res.get('arn')
            resource_type = from_cc_resource_type(rt)
            if resource_type is UDMResourceType.UNSPECIFIED:  # todo currently api does not accept this one(
                resource_type = UDMResourceType.CLOUD_PROJECT

            event = UDMEvent(
                metadata=UDMEventMetadata(
                    collected_timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                    description='Syndicate Rule Engine scanned target product',
                    event_type=UDMEventType.SCAN_VULN_HOST,
                    product_name=self._tenant.name,
                    vendor_name=self._tenant.customer_name
                ),
                principal=UDMNoun(
                    application='Syndicate Rule Engine',  # todo maybe add other data
                    hostname=SP.tls.__dict__.get('host', 'rule-engine'),  # todo maybe get from ec2 metadata
                ),
                target=UDMNoun(
                    location=UDMLocation(region),
                    resource=UDMResource(
                        product_object_id=resource_id,
                        name=resource_name,
                        resource_subtype=rt,
                        resource_type=resource_type,
                        attribute=UDMAttribute(
                            cloud=UDMCloud(
                                environment=UDMCloudEnvironment.from_local_cloud(self._tenant.cloud),
                            ),
                            labels=[]
                        )
                    )
                ),
                extensions=UDMExtensions(
                    vulns=UDMVulnerabilities(
                        vulnerabilities=[policies_results.get(p) for p in policies]
                    )
                )
            )

            if service := self.meta.rule(next(iter(policies))).service:
                event.target.application = service
            if date := full_res.get('date'):
                dt = self._parse_date(date)
                if dt:
                    event.target.resource.attribute.creation_time = dt
            if tags := full_res.get('Tags'):
                event.target.resource.attribute.labels.extend(
                    UDMLabel(key=t['Key'], value=t['Value']) for t in tags
                )
            events.append(msgspec.to_builtins(event))
        return events
