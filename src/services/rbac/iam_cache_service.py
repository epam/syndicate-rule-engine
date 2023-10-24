import operator
from datetime import timedelta
from functools import cached_property
from typing import Callable, Union, Optional, Iterable, Generator, Type, \
    Dict, Tuple, TypedDict, List

import services.cache as cache
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models.policy import Policy
from models.role import Role

Entity = Union[Role, Policy]

_LOG = get_logger(__name__)


class IamService:
    class PolicyData(TypedDict):
        customer: str
        name: str
        permissions: List[str]

    class RoleData(TypedDict):
        customer: str
        name: str
        expiration: Optional[str]
        policies: List[str]
        # resource: List[str]  # not used

    def get_policy(self, customer: str, name: str) -> Optional[Policy]:
        _LOG.debug(f'Querying policy ({customer}, {name})')
        return Policy.get_nullable(hash_key=customer, range_key=name)

    def get_role(self, customer: str, name: str) -> Optional[Role]:
        _LOG.debug(f'Querying role ({customer}, {name})')
        return Role.get_nullable(hash_key=customer, range_key=name)

    def list_policies(self, customer: Optional[str] = None,
                      name: Optional[str] = None) -> Iterable[Policy]:
        if customer and name:
            item = self.get_policy(customer, name)
            return iter([item]) if item else iter([])
        if customer:
            return Policy.query(hash_key=customer)
        # now only scan is possible
        condition = None
        if name:
            condition &= Policy.name == name
        return Policy.scan(filter_condition=condition)

    def list_roles(self, customer: Optional[str] = None,
                   name: Optional[str] = None) -> Iterable[Role]:
        """
        TODO add "expired" attr with such behaviour: if None, all the roles
         are returned. If True -> only expired, if False, only still valid
        :param customer:
        :param name:
        :return:
        """
        if customer and name:
            item = self.get_role(customer, name)
            return iter([item]) if item else iter([])
        if customer:
            return Role.query(hash_key=customer)
        # now only scan is possible
        condition = None
        if name:
            condition &= Role.name == name
        return Role.scan(filter_condition=condition)

    def i_not_expired(self, roles: Iterable[Role]
                      ) -> Generator[Role, None, None]:
        for role in roles:
            if self.is_role_expired(role):
                continue
            yield role

    @staticmethod
    def is_role_expired(role: Role) -> bool:
        return utc_datetime() >= utc_datetime(_from=role.expiration)

    @staticmethod
    def i_policy_permissions(policy: Policy) -> Generator[str, None, None]:
        yield from policy.permissions or []

    def i_role_policies(self, role: Role) -> Generator[Policy, None, None]:
        for policy in role.policies:
            item = self.get_policy(role.customer, policy)
            if not item:
                continue
            yield item

    @cached_property
    def dto_getter_map(self) -> Dict[Type[Entity], Callable]:
        return {
            Role: self._get_role_dto,
            Policy: self._get_policy_dto
        }

    @cached_property
    def saver_map(self) -> Dict[Type[Entity], Callable]:
        return {
            Role: self._save_role,
            Policy: self._save_policy
        }

    @cached_property
    def deleter_map(self) -> Dict[Type[Entity], Callable]:
        return {
            Role: self._delete_role,
            Policy: self._delete_policy
        }

    def _delete_role(self, role: Role):
        role.delete()

    def _delete_policy(self, policy: Policy):
        policy.delete()

    @staticmethod
    def _get_role_dto(role: Role) -> dict:
        dto = role.get_json()
        dto.pop('resource', None)  # no use
        return dto

    @staticmethod
    def _get_policy_dto(policy: Policy) -> dict:
        return policy.get_json()

    def _save_role(self, role: Role):
        role.save()

    def _save_policy(self, policy: Policy):
        policy.save()

    def get_dto(self, entity: Entity) -> dict:
        _dto = self.dto_getter_map.get(entity.__class__, lambda x: dict())
        return _dto(entity)

    def save(self, entity: Entity):
        _save = self.saver_map.get(entity.__class__, lambda x: None)
        _save(entity)

    def delete(self, entity: Entity):
        _delete = self.deleter_map.get(entity.__class__, lambda x: None)
        _delete(entity)

    @staticmethod
    def create_policy(data: PolicyData) -> Policy:
        return Policy(**data)

    @staticmethod
    def create_role(data: RoleData) -> Role:
        data.setdefault('expiration',
                        utc_iso(utc_datetime() + timedelta(days=90)))
        return Role(**data)


class CachedIamService(IamService):
    """
    I think, we need cache mainly in order not to query Roles and Policies
    for each request ('cause we need to check whether the user is
    allowed for each request). So, cache is currently not necessary for all
    the methods. ...
    """

    def __init__(self):
        self._roles_cache = cache.factory()
        self._policies_cache = cache.factory()

    @cache.cachedmethod(operator.attrgetter('_policies_cache'))
    def get_policy(self, customer: str, name: str) -> Optional[Policy]:
        return super().get_policy(customer, name)

    @cache.cachedmethod(operator.attrgetter('_roles_cache'))
    def get_role(self, customer: str, name: str) -> Optional[Role]:
        return super().get_role(customer, name)

    @staticmethod
    def _role_cache_key(role: Role) -> Tuple[str, str]:
        """
        Must be the same as "get_role" method's args
        :param role:
        :return:
        """
        return role.customer, role.name

    @staticmethod
    def _policy_cache_key(policy: Policy) -> Tuple[str, str]:
        """
        Must be the same as "get_policy" method's args
        :param policy:
        :return:
        """
        return policy.customer, policy.name

    def _save_role(self, role: Role):
        super()._save_role(role)
        self._roles_cache[self._role_cache_key(role)] = role

    def _save_policy(self, policy: Policy):
        super()._save_policy(policy)
        self._policies_cache[self._policy_cache_key(policy)] = policy

    def _delete_role(self, role: Role):
        super()._delete_role(role)
        self._roles_cache.pop(self._role_cache_key(role), None)

    def _delete_policy(self, policy: Policy):
        super()._delete_policy(policy)
        self._policies_cache.pop(self._policy_cache_key(policy), None)

    def clean_role_cache(self, customer: Optional[str] = None,
                         name: Optional[str] = None):
        if customer and name:
            self._roles_cache.pop((customer, name), None)
            return
        # customer or name
        keys = iter(self._roles_cache.keys())
        for key in keys:
            if key[0] == customer or key[1] == name:
                self._roles_cache.pop(key, None)
        return

    def clean_policy_cache(self, customer: Optional[str] = None,
                           name: Optional[str] = None):
        if customer and name:
            self._policies_cache.pop((customer, name), None)
            return
        # customer or name
        keys = iter(self._policies_cache.keys())
        for key in keys:
            if key[0] == customer or key[1] == name:
                self._policies_cache.pop(key, None)
        return
