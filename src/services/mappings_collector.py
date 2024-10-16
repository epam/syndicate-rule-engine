from functools import cached_property
from typing import TypedDict, TYPE_CHECKING

from helpers.constants import RuleDomain
from services import SP
from models.rule import RuleIndex

if TYPE_CHECKING:
    from services.rule_meta_service import RuleMetaModel
    from services.s3_settings_service import S3SettingsService


class HumanData(TypedDict):
    """
    Human-targeted info about rule
    """
    article: str | None
    impact: str
    report_fields: list[str]
    remediation: str
    multiregional: bool


SeverityType = dict[str, str]  # rule to severity
StandardType = dict[str, dict]  # rule to standards map
MitreType = dict[str, dict]  # rule to mitre map
ServiceSectionType = dict[str, str]  # rule to service section
ServiceType = dict[str, str]  # rule to service section
CategoryType = dict[str, str]  # rule to category
CloudRulesType = dict[str, set[str]]  # cloud to rules
Events = dict[str, dict[str, list[str]]]
HumanDataType = dict[str, HumanData]


class MappingsCollector:
    """
    This class helps to retrieve specific projections from rule's meta and
    keep them as mappings
    """

    def __init__(self):
        """
        Compressor must implement compress and decompress. Use something from
        standard library
        """
        self._severity = {}
        self._standard = {}
        self._mitre = {}
        self._service_section = {}
        self._cloud_rules = {}
        self._human_data = {}
        self._category = {}
        self._service = {}

        self._aws_standards_coverage = {}
        self._azure_standards_coverage = {}
        self._google_standards_coverage = {}

        self._aws_events = {}
        self._azure_events = {}
        self._google_events = {}

    def event_map(self, cloud: str) -> dict | None:
        if cloud == RuleDomain.AWS:
            return self._aws_events
        if cloud == RuleDomain.AZURE:
            return self._azure_events
        if cloud == RuleDomain.GCP:
            return self._google_events

    def add_meta(self, meta: 'RuleMetaModel'):
        self._severity[meta.name] = meta.severity
        self._mitre[meta.name] = meta.mitre
        self._service_section[meta.name] = meta.service_section
        self._category[meta.name] = meta.category
        self._service[meta.name] = meta.service
        self._standard[meta.name] = meta.standard
        domain = meta.get_domain()
        if domain:
            self._cloud_rules.setdefault(domain.value, []).append(meta.name)
        if meta.cloud:
            self._cloud_rules.setdefault(meta.cloud, []).append(meta.name)
        self._human_data[meta.name] = {
            'article': meta.article,
            'impact': meta.impact,
            'report_fields': meta.report_fields,
            'remediation': meta.remediation,
            'multiregional': meta.multiregional,
            'service': meta.service
        }

        _map = self.event_map(domain)
        if isinstance(_map, dict):
            for source, names in meta.events.items():
                _map.setdefault(source, {})
                for name in names:  # here already parsed, without ','
                    _map[source].setdefault(name, []).append(meta.name)

    @property
    def severity(self) -> SeverityType:
        return self._severity

    @severity.setter
    def severity(self, value: SeverityType):
        self._severity = value

    @property
    def standard(self) -> StandardType:
        return self._standard

    @standard.setter
    def standard(self, value: StandardType):
        self._standard = value

    @property
    def mitre(self) -> MitreType:
        return self._mitre

    @mitre.setter
    def mitre(self, value: MitreType):
        self._mitre = value

    @property
    def service_section(self) -> ServiceSectionType:
        return self._service_section

    @service_section.setter
    def service_section(self, value: ServiceSectionType):
        self._service_section = value

    @property
    def cloud_rules(self) -> CloudRulesType:
        return self._cloud_rules

    @cloud_rules.setter
    def cloud_rules(self, value: CloudRulesType):
        self._cloud_rules = value

    @property
    def human_data(self) -> HumanDataType:
        return self._human_data

    @human_data.setter
    def human_data(self, value: HumanDataType):
        self._human_data = value

    @property
    def service(self) -> ServiceType:
        return self._service

    @service.setter
    def service(self, value: ServiceType):
        self._service = value

    @property
    def category(self) -> CategoryType:
        return self._category

    @category.setter
    def category(self, value: CategoryType):
        self._category = value

    @property
    def aws_standards_coverage(self) -> dict:
        return self._aws_standards_coverage

    @aws_standards_coverage.setter
    def aws_standards_coverage(self, value: dict):
        self._aws_standards_coverage = value

    @property
    def azure_standards_coverage(self) -> dict:
        return self._azure_standards_coverage

    @azure_standards_coverage.setter
    def azure_standards_coverage(self, value: dict):
        self._azure_standards_coverage = value

    @property
    def google_standards_coverage(self) -> dict:
        return self._google_standards_coverage

    @google_standards_coverage.setter
    def google_standards_coverage(self, value: dict):
        self._google_standards_coverage = value

    @property
    def aws_events(self) -> Events:
        return self._aws_events

    @aws_events.setter
    def aws_events(self, value: Events):
        self._aws_events = value

    @property
    def azure_events(self) -> Events:
        return self._azure_events

    @azure_events.setter
    def azure_events(self, value: Events):
        self._azure_events = value

    @property
    def google_events(self) -> Events:
        return self._google_events

    @google_events.setter
    def google_events(self, value: Events):
        self._google_events = value

    @classmethod
    def build_from_sharding_collection_meta(cls, meta: dict
                                            ) -> 'MappingsCollector':
        """
        Sharding collection meta is a mapping of rule names to rules metadata
        that is available from the rules. That metadata has rule comment
        which we can use:
        https://github.com/epam/ecc-aws-rulepack/wiki/Rule-Index-(Comment)-Structure
        """
        instance = cls()
        for name, data in meta.items():
            comment = data.get('comment')
            if not comment:
                continue
            index = RuleIndex(comment)
            if ss := index.service_section:
                instance._service_section[name] = ss
            if category := index.category:
                instance._category[name] = category
            if source := index.source:
                instance._standard[name] = {source: ['()']}  # resembles standards format from metadata but without points
        return instance


class LazyLoadedMappingsCollector:
    """
    Read only class which allows to load mappings lazily. Currently, it
    loads them from S3. Readonly class
    """

    def __init__(self, collector: MappingsCollector,
                 s3_settings_service: 'S3SettingsService'):
        self._collector = collector
        self._s3_settings_service = s3_settings_service

    @classmethod
    def build(cls) -> 'LazyLoadedMappingsCollector':
        return cls(
            collector=MappingsCollector(),
            s3_settings_service=SP.s3_settings_service,
        )

    @cached_property
    def category(self) -> CategoryType:
        return self._s3_settings_service.rules_to_category() or {}

    @cached_property
    def service(self) -> ServiceType:
        return self._s3_settings_service.rules_to_service() or {}

    @cached_property
    def severity(self) -> SeverityType:
        return self._s3_settings_service.rules_to_severity() or {}

    @cached_property
    def service_section(self) -> ServiceSectionType:
        return self._s3_settings_service.rules_to_service_section() or {}

    @cached_property
    def standard(self) -> StandardType:
        return self._s3_settings_service.rules_to_standards() or {}

    @cached_property
    def mitre(self) -> MitreType:
        return self._s3_settings_service.rules_to_mitre() or {}

    @cached_property
    def cloud_rules(self) -> CloudRulesType:
        """
        This mapping is kind of special. It's just auxiliary. It means that
        the functions, this mapping is designed for, can be used without the
        map (obviously they will work worse, but still, they will work),
        whereas all the other mappings are required for the functional they
        provided for
        :return:
        """
        return self._s3_settings_service.cloud_to_rules() or {}

    @cached_property
    def human_data(self) -> HumanDataType:
        return self._s3_settings_service.human_data() or {}

    @cached_property
    def aws_standards_coverage(self) -> dict:
        return self._s3_settings_service.aws_standards_coverage() or {}

    @cached_property
    def azure_standards_coverage(self) -> dict:
        return self._s3_settings_service.azure_standards_coverage() or {}

    @cached_property
    def google_standards_coverage(self) -> dict:
        return self._s3_settings_service.google_standards_coverage() or {}

    @cached_property
    def aws_events(self) -> Events:
        return self._s3_settings_service.aws_events() or {}

    @cached_property
    def azure_events(self) -> Events:
        return self._s3_settings_service.azure_events() or {}

    @cached_property
    def google_events(self) -> Events:
        return self._s3_settings_service.google_events() or {}
