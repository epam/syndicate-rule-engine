from functools import cached_property
from itertools import chain
from typing import Set

from helpers.log_helper import get_logger
from services.rbac.endpoint_to_permission_mapping import \
    ENDPOINT_PERMISSION_MAPPING, PERMISSIONS
from services.rbac.iam_cache_service import CachedIamService
from helpers.system_customer import SYSTEM_CUSTOMER

_LOG = get_logger(__name__)

PARAM_NAME = 'name'
PARAM_PERMISSIONS = 'permissions'
PARAM_EXPIRATION = 'expiration'
PARAM_REQUEST_PATH = 'request_path'
PARAM_TARGET_USER = 'target_user'


class AccessControlService:

    def __init__(self, iam_service: CachedIamService):
        self._iam_service = iam_service  # CachedIamService

        self._all_domains: Set[str] = set()
        self._all_actions: Set[str] = set()

    @property
    def allow_all(self) -> str:
        return '*'

    @cached_property
    def all_permissions(self) -> Set[str]:
        return {
            method.get(PERMISSIONS) for value in
            ENDPOINT_PERMISSION_MAPPING.values()
            for method in value.values() if method.get(PERMISSIONS)
        }

    @cached_property
    def permissions_to_exclude_for_admin(self) -> Set[str]:
        return {
            'user:describe_customer',
            'user:assign_customer',
            'user:update_customer',
            'user:unassign_customer',
            'user:describe_role',
            'user:assign_role',
            'user:update_role',
            'user:unassign_role',

            "user:signup",
            "customer:update_customer",
            "license:create_license_sync",
            "ruleset:get_content",
            'settings:describe_mail',
            'settings:create_mail',
            'settings:delete_mail',

            'ruleset:describe_event_driven',
            'ruleset:create_event_driven',
            'ruleset:delete_event_driven',
            'run:initiate_standard_run'
        }

    @cached_property
    def permissions_to_exclude_for_user(self) -> Set[str]:
        return self.permissions_to_exclude_for_admin | {
            # "license:create_license",  # endpoint does not exists
            "license:remove_license",

            "user:describe_tenants",
            "user:assign_tenants",
            "user:update_tenants",
            "user:unassign_tenants",

            "run:initiate_event_run",

            "report:department",
            "report:clevel"

            "application:activate",
            "application:describe",
        }

    @cached_property
    def admin_permissions(self) -> Set[str]:
        return self.all_permissions - self.permissions_to_exclude_for_admin

    @cached_property
    def user_permissions(self) -> Set[str]:
        return self.all_permissions - self.permissions_to_exclude_for_user

    def is_permission_allowed(self, customer: str, permission: str) -> bool:
        """
        Tells whether it's allowed for 'customer' to assign or access
        permissions.
        :param customer: customer who MAKES THE REQUEST, but NOT the actual
        customer, whose policy is modified.
        :param permission: permission the customer wants to assign
        :return bool
        The logic is fast-made and actually, some day may be completed to
        something decent
        """
        if customer != SYSTEM_CUSTOMER:  # wildcards not allowed here
            _scope = self.admin_permissions
            return permission in _scope
        # SYSTEM
        domain, action = permission.split(':')
        _domain_valid = domain == self.allow_all or domain in self.all_domains
        _action_valid = action == self.allow_all or action in self.all_actions
        return _domain_valid and _action_valid

    def _init_domains_actions(self):
        for perm in self.all_permissions:
            domain, action = perm.split(':')
            self._all_domains.add(domain)
            self._all_actions.add(action)

    @property
    def all_domains(self) -> Set[str]:
        if not self._all_domains:
            self._init_domains_actions()
        return self._all_domains

    @property
    def all_actions(self) -> Set[str]:
        if not self._all_actions:
            self._init_domains_actions()
        return self._all_actions

    def is_allowed_to_access(self, customer: str, role_name: str,
                             target_permission: str) -> bool:
        role = self._iam_service.get_role(customer, role_name)
        if not role:
            return False
        if self._iam_service.is_role_expired(role):
            return False
        # making efficient user's permission iterator
        it = chain.from_iterable(
            self._iam_service.i_policy_permissions(policy)
            for policy in self._iam_service.i_role_policies(role)
        )
        for permission in it:
            if self.does_permission_match(target_permission, permission):
                return True
        return False

    def does_permission_match(self, target_permission: str,
                              permission: str) -> bool:
        """
        Our permissions adhere to such a format "domain:action".
        :param target_permission: permission a user want to access.
        It's not supposed to contain '*'. Must be a solid perm.
        to access one endpoint
        :param permission: permission a user has
        :return:
        """
        if any(':' not in perm for perm in (target_permission, permission)):
            return False
        tp_domain, tp_action = map(
            str.strip, target_permission.split(':', maxsplit=2))
        p_domain, p_action = map(
            str.strip, permission.split(':', maxsplit=2))

        _domain_match = tp_domain == p_domain or p_domain == self.allow_all
        _action_match = tp_action == p_action or p_action == self.allow_all
        return _domain_match and _action_match
