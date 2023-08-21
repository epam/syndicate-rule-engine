from abc import ABC, abstractmethod
from functools import cached_property
from typing import Dict, Callable

Mapping = Dict[str, Dict[str, Callable]]


# gradual refactoring
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
            "path": {
                "method": self.handler
            }
        }
        """
