from modular_sdk.commons.constants import SIEM_DEFECT_DOJO_TYPE, CUSTODIAN_TYPE, \
    CUSTODIAN_LICENSES_TYPE, CUSTODIAN_ACCESS_TYPE

from helpers import Enum as CustomEnum
from helpers.constants import HC_STATUS_NOT_OK, HC_STATUS_OK, HC_STATUS_UNKNOWN

# These enums are used for pydantic models and just to keep related data
HealthCheckStatus = CustomEnum.build(
    'HealthCheckStatus', [HC_STATUS_OK, HC_STATUS_UNKNOWN, HC_STATUS_NOT_OK]
)
ParentType = CustomEnum.build(
    'ParentType',
    [CUSTODIAN_TYPE, CUSTODIAN_LICENSES_TYPE, SIEM_DEFECT_DOJO_TYPE,
     CUSTODIAN_ACCESS_TYPE]
)
