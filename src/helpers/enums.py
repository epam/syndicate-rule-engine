from modular_sdk.commons.constants import ParentType as _ParentType

from helpers import Enum as CustomEnum
from helpers.constants import AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, \
    GCP_CLOUD_ATTR, KUBERNETES_CLOUD_ATTR

# These enums are used for pydantic models and just to keep related data
ParentType = CustomEnum.build(
    'ParentType', [
        _ParentType.CUSTODIAN.value,
        _ParentType.CUSTODIAN_LICENSES.value,
        _ParentType.SIEM_DEFECT_DOJO.value,
        _ParentType.CUSTODIAN_ACCESS.value
    ]
)


# The values of this enum represent what Custom core can scan, i.e. what
# type of rules and ruleset(s) we can have. These are not tenant clouds
class RuleDomain(CustomEnum):
    AWS = AWS_CLOUD_ATTR
    AZURE = AZURE_CLOUD_ATTR
    GCP = GCP_CLOUD_ATTR
    KUBERNETES = KUBERNETES_CLOUD_ATTR
