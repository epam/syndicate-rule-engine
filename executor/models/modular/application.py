import dataclasses
from typing import Optional

from modular_sdk.models.application import Application

from helpers.constants import AWS, AZURE, GOOGLE

Application = Application

ALLOWED_CLOUDS = set(map(str.lower, (AWS, AZURE, GOOGLE))) | \
                 {AWS, AZURE, GOOGLE}


# use dataclass instead of pydantic in order not to add Pydantic to docker's
# dependencies
@dataclasses.dataclass(frozen=True)
class CustodianLicensesApplicationMeta:
    """
    Application with type 'CUSTODIAN_LICENSES' meta
    """
    awsAid: Optional[str] = None
    azureAid: Optional[str] = None
    googleAid: Optional[str] = None
    awsLk: Optional[str] = None
    azureLk: Optional[str] = None
    googleLk: Optional[str] = None

    @classmethod
    def from_dict(cls, dct: dict) -> 'CustodianLicensesApplicationMeta':
        """
        Ignoring extra kwargs
        :param dct:
        :return:
        """
        return cls(**{
            k.name: dct.get(k.name) for k in dataclasses.fields(cls)
        })

    @staticmethod
    def _license_key_attr(cloud: str) -> str:
        return f'{cloud.lower()}Lk'

    @staticmethod
    def _access_application_id_attr(cloud: str) -> str:
        return f'{cloud.lower()}Aid'

    def license_key(self, cloud: str) -> Optional[str]:
        assert cloud in ALLOWED_CLOUDS
        return getattr(self, self._license_key_attr(cloud), None)

    def access_application_id(self, cloud: str) -> Optional[str]:
        assert cloud in ALLOWED_CLOUDS
        return getattr(self, self._access_application_id_attr(cloud), None)
