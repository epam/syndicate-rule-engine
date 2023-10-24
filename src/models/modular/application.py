from typing import Optional, Dict

from modular_sdk.models.application import Application
from pydantic import BaseModel

from helpers.enums import RuleDomain

Application = Application


class CustodianLicensesApplicationMeta(BaseModel):
    """
    Application with type 'CUSTODIAN_LICENSES' meta
    """

    class Config:
        extra = 'allow'

    awsAid: Optional[str]
    azureAid: Optional[str]
    googleAid: Optional[str]
    kubernetesAid: Optional[str]
    awsLk: Optional[str]
    azureLk: Optional[str]
    googleLk: Optional[str]
    kubernetesLk: Optional[str]

    @staticmethod
    def _license_key_attr(cloud: str) -> str:
        return f'{cloud.lower()}Lk'

    @staticmethod
    def _access_application_id_attr(cloud: str) -> str:
        return f'{cloud.lower()}Aid'

    def update_access_application_id(self, cloud: str, aid: str):
        setattr(self, self._access_application_id_attr(cloud), aid)

    def update_license_key(self, cloud: str, lk: Optional[str] = None):
        setattr(self, self._license_key_attr(cloud), lk)

    def license_key(self, cloud: str) -> Optional[str]:
        return getattr(self, self._license_key_attr(cloud), None)

    def access_application_id(self, cloud: str) -> Optional[str]:
        return getattr(self, self._access_application_id_attr(cloud), None)

    def cloud_to_license_key(self) -> Dict[str, Optional[str]]:
        return {
            cloud: self.license_key(cloud) for cloud in RuleDomain.iter()
        }
