from abc import ABC, abstractmethod
from functools import wraps
from typing import Iterable, Generator, Union, Callable, List, Set, Optional

from helpers.constants import ALLOWED_FOR_ATTR, CUSTOMER_ATTR
from helpers.system_customer import SYSTEM_CUSTOMER
from models.rule_source import RuleSource
from models.ruleset import Ruleset
from models.rule import Rule
from services.rbac.restriction_service import RestrictionService

Entity = Union[RuleSource, Ruleset, Rule]
RulesContainer = Union[RuleSource, Ruleset]


class AbstractRuleService(ABC):
    def __init__(self, restriction_service: RestrictionService):
        self._restriction_service = restriction_service

    def filter_by_tenants(self, entities: Iterable[RulesContainer],
                          tenants: Optional[Set[str]] = None,
                          ) -> Generator[RulesContainer, None, None]:
        """
        Filters the given iterable of entities by the list of allowed
        tenants using `allowed_for` attribute
        """
        _tenants = tenants or self._restriction_service.user_tenants
        if not _tenants:
            yield from entities
            return
        for entity in entities:
            allowed_for = list(entity.allowed_for)
            if not allowed_for or allowed_for & _tenants:
                yield entity

    @staticmethod
    def system_payload(params: dict) -> dict:
        """
        Adjusts the params somehow to make the request return SYSTEM
        entities
        Make sure to use `super(Class, Class)` if you are going to use super
        in an inherited staticmethod.
        """
        return {**params, CUSTOMER_ATTR: SYSTEM_CUSTOMER}

    @classmethod
    def derive_from_system(cls, func: Callable):
        """
        A decorator to expand customer's entities with system entities.
        Make sure to use only keyword parameters
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            customer_entities = func(*args, **kwargs)
            kwargs = cls.system_payload(kwargs)
            system_entities = func(*args, **kwargs)
            return cls.expand_systems(
                system_entities, customer_entities)
        return wrapper

    @staticmethod
    @abstractmethod
    def expand_systems(system_entities: List[Entity],
                       customer_entities: List[Entity]) -> List[Entity]:
        """
        Updates two lists of objects by certain objects' attributes
        """

