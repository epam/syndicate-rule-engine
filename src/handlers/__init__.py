from abc import ABC, abstractmethod
from typing import Callable
from helpers.constants import Endpoint

Mapping = dict[Endpoint, dict[str, Callable]]


class AbstractHandler(ABC):
    @classmethod
    @abstractmethod
    def build(cls) -> 'AbstractHandler':
        """
        Builds the instance of the class
        """

    @property
    @abstractmethod
    def mapping(self) -> Mapping:
        """
        {
            Endpoint.JOBS: {HTTPMethod.GET: self.get_jobs}
        }
        """
