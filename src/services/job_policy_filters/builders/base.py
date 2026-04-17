from abc import ABC, abstractmethod

from ..types import CustodianFilter


class PolicyFiltersBuilder(ABC):
    @abstractmethod
    def build(self) -> list[CustodianFilter]:
        pass


class PolicyQueryBuilder(ABC):
    @abstractmethod
    def build(self) -> list[dict[str, str]]:
        pass
