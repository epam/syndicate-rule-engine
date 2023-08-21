import os

from modular_sdk.models.pynamodb_extension.base_model import BaseModel
from pynamodb.attributes import UnicodeAttribute, MapAttribute, \
    ListAttribute, BooleanAttribute

from helpers.constants import ENV_VAR_REGION, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR

RM_NAME_ATTR = 'n'
RM_VERSION_ATTR = 'v'
RM_CLOUD_ATTR = 'c'
RM_SOURCE_ATTR = 's'
RM_ARTICLE_ATTR = 'a'
RM_SERVICE_SECTION_ATTR = 'ss'
RM_IMPACT_ATTR = 'i'
RM_SEVERITY_ATTR = 'se'
RM_MIN_CORE_VERSION_ATTR = 'mcv'
RM_REPORT_FIELDS_ATTR = 'r'
RM_MULTIREGIONAL_ATTR = 'm'
RM_EVENTS_ATTR = 'e'
RM_STANDARD_ATTR = 'st'
RM_MITRE_ATTR = 'mi'
RM_REMEDIATION_ATTR = 're'


class RuleMeta(BaseModel):
    class Meta:
        table_name = 'CaaSRulesMeta'
        region = os.environ.get(ENV_VAR_REGION)

    name = UnicodeAttribute(hash_key=True, attr_name=RM_NAME_ATTR)
    version = UnicodeAttribute(range_key=True, attr_name=RM_VERSION_ATTR)
    # meta
    cloud = UnicodeAttribute(attr_name=RM_CLOUD_ATTR)  # it's more a domain
    source = UnicodeAttribute(attr_name=RM_SOURCE_ATTR)
    article = UnicodeAttribute(attr_name=RM_ARTICLE_ATTR, null=True)
    service_section = UnicodeAttribute(attr_name=RM_SERVICE_SECTION_ATTR)
    impact = UnicodeAttribute(attr_name=RM_IMPACT_ATTR)
    severity = UnicodeAttribute(attr_name=RM_SEVERITY_ATTR)
    min_core_version = UnicodeAttribute(attr_name=RM_MIN_CORE_VERSION_ATTR)
    report_fields = ListAttribute(default=list, of=UnicodeAttribute,
                                  attr_name=RM_REPORT_FIELDS_ATTR)
    multiregional = BooleanAttribute(default=True,
                                     attr_name=RM_MULTIREGIONAL_ATTR)
    events = MapAttribute(default=dict, attr_name=RM_EVENTS_ATTR)
    standard = MapAttribute(default=dict, attr_name=RM_STANDARD_ATTR)
    mitre = MapAttribute(default=dict, attr_name=RM_MITRE_ATTR)
    remediation = UnicodeAttribute(attr_name=RM_REMEDIATION_ATTR)

    def is_multiregional(self) -> bool:
        """
        AWS rules can be multiregional or region-dependent whereas AZURE and
        GCP rules are always multiregional
        :return:
        """
        if self.cloud == AZURE_CLOUD_ATTR or self.cloud == GCP_CLOUD_ATTR:
            return True
        return self.multiregional
