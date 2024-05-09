from abc import ABC, abstractmethod
from functools import cached_property
from typing import Callable
from helpers.constants import CustodianEndpoint

Mapping = dict[CustodianEndpoint, dict[str, Callable]]


class AbstractHandler(ABC):
    @classmethod
    @abstractmethod
    def build(cls) -> 'AbstractHandler':
        """
        Builds the instance of the class
        """

    @cached_property
    @abstractmethod
    def mapping(self) -> Mapping:
        """
        {
            CustodianEndpoint.JOBS: {HTTPMethod.GET: self.get_jobs}
        }
        """
