from typing import Any
from abc import ABC, abstractmethod

MANAGEMENT_ATTR = 'management'
GOVERNANCE_ATTR = 'governance'
DELEGATION_ATTR = 'delegation'


class AbstractGovernanceService(ABC):

    """
    Entity Governance could be expressed to mediate access and interaction,
    adhering expressed data-model.
    - entity
    - resource `management`: Dict
     * $(management-id or mid)(s): str
      * $management-data: Any
    - party `delegation`: Dict
     * $(delegation-id or did)(s): str
      * $delegation-data: Any
    - interaction `governance`: Dict
     * $(governance-id or gid): str
      * $governance-data: Any
    """

    @property
    @abstractmethod
    def governance_type(self):
        raise NotImplementedError

    @governance_type.setter
    @abstractmethod
    def governance_type(self, attr: str):
        raise NotImplementedError

    @abstractmethod
    def get_entity(self, entity: Any):
        raise NotImplementedError

    @abstractmethod
    def get_management(self, entity: Any, mid: str):
        raise NotImplementedError

    @abstractmethod
    def i_get_management(self, entity: Any):
        raise NotImplementedError

    @abstractmethod
    def create_management(self, entity: Any, data: Any):
        raise NotImplementedError

    @abstractmethod
    def delete_management(self, entity: Any, mid: str):
        raise NotImplementedError

    @abstractmethod
    def get_management_dto(self, mid: str, data: Any):
        raise NotImplementedError
