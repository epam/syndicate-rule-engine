from datetime import datetime
from typing import Generator

from pynamodb.pagination import ResultIterator

from helpers.constants import Permission, PolicyEffect
from helpers.time_helper import utc_iso
from models.policy import Policy
from models.role import Role
from services.base_data_service import BaseDataService


class PolicyStruct:
    __slots__ = ('customer', 'name', 'effect', 'permissions', 'tenants',
                 'description')

    def __init__(self, customer: str, name: str,
                 effect: PolicyEffect = PolicyEffect.ALLOW,
                 permissions: set[str] | None = None,
                 tenants: set[str] | None = None,
                 description: str | None = None):
        self.customer: str = customer
        self.name: str = name
        self.effect: PolicyEffect = effect
        self.permissions: set[str] = permissions or set()
        self.tenants: set[str] = tenants or set()
        self.description: str | None = description

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}'
                f'(customer={self.customer}, name={self.name})')

    @classmethod
    def from_model(cls, policy: Policy) -> 'PolicyStruct':
        ef = PolicyEffect.ALLOW
        if policy.effect:
            ef = PolicyEffect(policy.effect)
        return cls(
            customer=policy.customer,
            name=policy.name,
            effect=ef,
            permissions=set(policy.permissions),
            tenants=set(policy.tenants),
            description=policy.description
        )

    @classmethod
    def from_dct(cls, dct: dict) -> 'PolicyStruct':
        """
        Assuming that the dict is valid
        :param dct:
        :return:
        """
        ef = PolicyEffect.ALLOW
        if effect := dct.get('effect'):
            ef = PolicyEffect(effect)
        return cls(
            customer=dct['customer'],
            name=dct['name'],
            effect=ef,
            permissions=set(dct.get('permissions') or ()),
            tenants=set(dct.get('tenants') or ()),
            description=dct.get('description')
        )

    @property
    def contains_all_tenants(self) -> bool:
        return '*' in self.tenants

    def touches(self, permission: Permission) -> bool:
        """
        Tells whether the given permission is mentioned inside a policy
        :param permission:
        :return:
        """
        permissions = self.permissions
        domain, action = permission.split(':', maxsplit=1)
        return (f'{domain}:*' in permissions
                or f'*:{action}' in permissions
                or '*:*' in permissions
                or permission in permissions)

    def forbids(self, permission: Permission) -> bool:
        """
        Fast forbid. In case this method returns True this policy absolutely
        forbids the given permission and there is no need to check other
        policies, we can do 403 immediately. In case this method return False
        it does not forbid the permission, and we must check other policies
        as well.
        In case this method return False it does not mean that the permission
        is allowed. It's just not forbidden. In order to understand whether
        the permission is allowed we must be aware of all the policies
        :param permission:
        :return:
        """
        if self.effect == PolicyEffect.ALLOW:
            # allowing policy definitely cannot forbid a permission
            return False
        # assert self.effect == PolicyEffect.ALLOW
        if not self.touches(permission):
            # denying policy cannot forbid a permission if it's not in
            # permissions list
            return False
        # target permission is mentioned inside this policy
        if permission.depends_on_tenant and self.contains_all_tenants:
            return True
        if not permission.depends_on_tenant:
            return True
        return False

    def allows(self, permission: Permission) -> bool:
        """
        Tells whether this policy allows the given permission. Even if this
        policy does allow the permission some other policy can forbid the
        permission. So, we cannot use this method independently on other
        policies
        :param permission:
        :return:
        """
        if self.effect == PolicyEffect.DENY:
            # denying policy cannot allow a permission
            return False
        if not self.touches(permission):
            # allowing policy cannot allow a permission that one is not
            # mentioned inside
            return False
        if permission.depends_on_tenant and not self.tenants:
            return False
        return True


class TenantsAccessPayload:
    ALL = object()

    __slots__ = '_names', '_allowed_flag'

    def __init__(self, names: tuple[str, ...], allowed: bool):
        """
        Two states can be represented:
        - permission is allowed for some specific tenant names
        - permission is allowed for all tenant names except some specific
        If allowed is True then `names` contains only allowed tenants. If
        allowed is False then `names` contains tenants that forbidden. All
        tenant are allowed except those inside `names`
        :param names:
        :param allowed:
        """
        self._names = names
        self._allowed_flag = allowed

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}(names={self._names}, '
                f'allowed={self._allowed_flag})')

    __str__ = __repr__

    @classmethod
    def build_allowing_all(cls) -> 'TenantsAccessPayload':
        return cls(names=(), allowed=False)

    @classmethod
    def build_denying_all(cls) -> 'TenantsAccessPayload':
        # a bit confusing... `allowed` is just a flag that tells what
        # tenants inside `names`. Since names are empty there are no allowed
        return cls(names=(), allowed=True)

    def is_allowed_for(self, tenant_name: str) -> bool:
        _mention = tenant_name in self._names
        _allowed = self._allowed_flag
        return _mention and _allowed or not _mention and not _allowed

    def is_allowed_for_all_tenants(self) -> bool:
        """
        Return True only if the permission is allowed for any tenant
        :return:
        """
        return not self._names and not self._allowed_flag

    def allowed_denied(self
                       ) -> tuple[object | tuple[str, ...], tuple[str, ...]]:
        """
        >>> all_items = set()  # some items here
        >>> allowed, denied = self.allowed_denied()
        >>> if allowed is TenantsAccessPayload.ALL:
        ...     items = tuple(filter(lambda x: x not in denied, all_items))
        ... else:
        ...     items = tuple(all_items & set(allowed))
        """
        if self._allowed_flag:
            return self._names, ()
        # allowed for all
        return self.ALL, self._names


class TenantAccess:
    __slots__ = '_allow_policies', '_deny_policies'

    def __init__(self):
        self._allow_policies: list[PolicyStruct] = []
        self._deny_policies: list[PolicyStruct] = []

    def add(self, policy: PolicyStruct) -> None:
        match policy.effect:
            case PolicyEffect.ALLOW:
                self._allow_policies.append(policy)
            case PolicyEffect.DENY:
                self._deny_policies.append(policy)

    def resolve_payload(self, permission: Permission) -> TenantsAccessPayload:
        """
        Builds TenantsAccessPayload for the given permission considering all
        the policies. Then this payload can be used inside handlers
        :return:
        """
        if not permission.depends_on_tenant:
            # you should not need this payload for handlers that do not
            # depend on tenants. So I even not try to resolve
            return TenantsAccessPayload.build_denying_all()

        names, flag = set(), True  # starting state, allowed for no-one
        for policy in self._allow_policies:
            if not policy.touches(permission):
                continue
            # does touch
            if policy.contains_all_tenants:
                flag = False
                names.clear()
            else:  # todo can be optimized slightly
                if flag:
                    names.update(policy.tenants)
                else:
                    names.difference_update(policy.tenants)
        for policy in self._deny_policies:
            if not policy.touches(permission):
                continue
            # does touch
            if policy.contains_all_tenants:
                flag = True
                names.clear()
            else:  # todo can be optimized slightly
                if flag:
                    names.difference_update(policy.tenants)
                else:
                    names.update(policy.tenants)
        return TenantsAccessPayload(tuple(names), flag)


class PolicyService(BaseDataService[Policy]):
    def get_nullable(self, customer: str, name: str) -> Policy | None:
        return super().get_nullable(hash_key=customer, range_key=name)

    def iter_role_policies(self, role: Role) -> Generator[Policy, None, None]:
        customer = role.customer
        for name in set(role.policies):
            item = self.get_nullable(customer, name)
            if not item:
                continue
            yield item

    def create(self, customer: str, name: str, description: str | None = None,
               permissions: tuple[str, ...] | list[str] = (),
               tenants: tuple[str, ...] | list[str] = (),
               effect: PolicyEffect = PolicyEffect.DENY
               ) -> Policy:
        return Policy(
            customer=customer,
            name=name,
            description=description,
            permissions=list(permissions),
            tenants=list(tenants),
            effect=effect.value,
        )

    def query(self, customer: str, limit: int | None = None, 
              last_evaluated_key: dict | int | None = None
              ) -> ResultIterator[Policy]:
        return Policy.query(
            hash_key=customer,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )


class RoleService(BaseDataService[Role]):
    def get_nullable(self, customer: str, name: str) -> Role | None:
        return super().get_nullable(hash_key=customer, range_key=name)

    def create(self, customer: str, name: str, expiration: datetime | None,
               policies: tuple[str, ...] | list[str] = (), 
               description: str | None = None) -> Role:
        return Role(
            customer=customer,
            name=name,
            expiration=utc_iso(expiration) if expiration else None,
            policies=list(policies),
            description=description
        )

    def query(self, customer: str, limit: int | None = None, 
              last_evaluated_key: dict | int | None = None
              ) -> ResultIterator[Role]:
        return Role.query(
            hash_key=customer,
            limit=limit,
            last_evaluated_key=last_evaluated_key
        )
